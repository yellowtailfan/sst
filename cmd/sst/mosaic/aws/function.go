package aws

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/sst/sst/v3/cmd/sst/mosaic/aws/bridge"
	"github.com/sst/sst/v3/cmd/sst/mosaic/watcher"
	"github.com/sst/sst/v3/pkg/bus"
	"github.com/sst/sst/v3/pkg/project"
	"github.com/sst/sst/v3/pkg/runtime"
	"github.com/sst/sst/v3/pkg/server"
)

type FunctionInvokedEvent struct {
	FunctionID string
	WorkerID   string
	RequestID  string
	Input      []byte
}

type FunctionResponseEvent struct {
	FunctionID string
	WorkerID   string
	RequestID  string
	Output     []byte
}

type FunctionErrorEvent struct {
	FunctionID   string
	WorkerID     string
	RequestID    string
	ErrorType    string   `json:"errorType"`
	ErrorMessage string   `json:"errorMessage"`
	Trace        []string `json:"trace"`
}

type FunctionBuildEvent struct {
	FunctionID string
	Errors     []string
}

type FunctionLogEvent struct {
	FunctionID string
	WorkerID   string
	RequestID  string
	Line       string
}

type input struct {
	config  aws.Config
	project *project.Project
	server  *server.Server
	client  *bridge.Client
	msg     chan bridge.Message
	prefix  string
}

func function(ctx context.Context, input input) {
	log := slog.Default().With("service", "aws.function")
	server := fmt.Sprintf("localhost:%d/lambda/", input.server.Port)
	type WorkerInfo struct {
		FunctionID       string
		WorkerID         string
		Worker           runtime.Worker
		CurrentRequestID string
		Env              []string
		StartedAt        time.Time
	}
	workerShutdownChan := make(chan *WorkerInfo, 1000)

	// A Node worker exits after 60s idle, and a request handed to it in that instant is lost with it:
	// nothing answers and the bridge times out. So each request is held until the worker answers, and
	// a worker that exits holding one is restarted with it. Once per request, so a handler that kills
	// its own worker cannot loop.
	type heldRequest struct {
		raw      []byte
		attempts int
	}
	// Replies kept a while after sending: AppSync can accept a publish and never deliver it, even to a
	// subscription that has delivered before, so a bridge still waiting asks for its reply again.
	type sentReply struct {
		message  bridge.MessageType
		workerID string
		body     []byte
		at       time.Time
	}
	var sentLock sync.Mutex
	sent := map[string]*sentReply{}
	remember := func(requestID string, reply *sentReply) {
		sentLock.Lock()
		defer sentLock.Unlock()
		for id, r := range sent {
			if time.Since(r.at) > 2*time.Minute {
				delete(sent, id)
			}
		}
		sent[requestID] = reply
	}

	var inflightLock sync.Mutex
	inflight := map[string]*heldRequest{}
	release := func(workerID string) {
		inflightLock.Lock()
		delete(inflight, workerID)
		inflightLock.Unlock()
	}
	nextChan := map[string]chan io.Reader{}
	workers := map[string]*WorkerInfo{}
	evts := bus.Subscribe(&watcher.FileChangedEvent{}, &project.CompleteEvent{}, &runtime.BuildInput{}, &FunctionInvokedEvent{})
	go fileLogger(input.project)

	input.server.Mux.HandleFunc(`/lambda/{workerID}/2018-06-01/runtime/invocation/next`, func(w http.ResponseWriter, r *http.Request) {
		log.Info("got next request", "workerID", r.PathValue("workerID"))
		workerID := r.PathValue("workerID")
		ch := nextChan[workerID]
		select {
		case <-r.Context().Done():
			log.Info("worker disconnected", "workerID", workerID)
			return
		case reader := <-ch:
			log.Info("worker got next request", "workerID", workerID)
			raw, _ := io.ReadAll(reader)
			inflightLock.Lock()
			held, ok := inflight[workerID]
			if !ok || !bytes.Equal(held.raw, raw) {
				held = &heldRequest{raw: raw}
				inflight[workerID] = held
			}
			held.attempts++
			inflightLock.Unlock()
			resp, _ := http.ReadResponse(bufio.NewReader(bytes.NewReader(raw)), r)
			requestID := resp.Header.Get("lambda-runtime-aws-request-id")
			for key, values := range resp.Header {
				for _, value := range values {
					w.Header().Add(key, value)
				}
			}
			w.WriteHeader(resp.StatusCode)

			var buf bytes.Buffer
			tee := io.TeeReader(resp.Body, &buf)
			io.Copy(w, tee)
			workerInfo, ok := workers[workerID]
			if ok {
				bus.Publish(&FunctionInvokedEvent{
					FunctionID: workerInfo.FunctionID,
					WorkerID:   workerID,
					RequestID:  requestID,
					Input:      buf.Bytes(),
				})
			}
		}
	})

	input.server.Mux.HandleFunc(`/lambda/{workerID}/2018-06-01/runtime/init/error`, func(w http.ResponseWriter, r *http.Request) {
		workerID := r.PathValue("workerID")
		log.Info("got init error", "workerID", workerID, "requestID", r.PathValue("requestID"))
		writer := input.client.NewWriter(bridge.MessageInitError, input.prefix+"/"+workerID+"/in")
		var buf bytes.Buffer
		tee := io.TeeReader(r.Body, &buf)
		io.Copy(writer, tee)
		if err := writer.Close(); err != nil {
			log.Error("failed to send to the bridge", "workerID", workerID, "err", err)
		}
		w.WriteHeader(200)
		info, ok := workers[workerID]
		if ok {
			fee := &FunctionErrorEvent{
				FunctionID: info.FunctionID,
				WorkerID:   info.WorkerID,
			}
			json.Unmarshal(buf.Bytes(), &fee)
			bus.Publish(fee)
		}
	})

	input.server.Mux.HandleFunc(`/lambda/{workerID}/2018-06-01/runtime/invocation/{requestID}/response`, func(w http.ResponseWriter, r *http.Request) {
		workerID := r.PathValue("workerID")
		requestID := r.PathValue("requestID")
		log.Info("got response", "workerID", workerID, "requestID", r.PathValue("requestID"))
		release(workerID)
		writer := input.client.NewWriter(bridge.MessageResponse, input.prefix+"/"+workerID+"/in")
		writer.SetID(requestID)
		var buf bytes.Buffer
		tee := io.TeeReader(r.Body, &buf)
		io.Copy(writer, tee)
		if err := writer.Close(); err != nil {
			log.Error("failed to send to the bridge", "workerID", workerID, "err", err)
		}
		remember(requestID, &sentReply{bridge.MessageResponse, workerID, buf.Bytes(), time.Now()})
		w.WriteHeader(202)
		info, ok := workers[workerID]
		if ok {
			bus.Publish(&FunctionResponseEvent{
				FunctionID: info.FunctionID,
				WorkerID:   workerID,
				RequestID:  requestID,
				Output:     buf.Bytes(),
			})
		}
	})

	input.server.Mux.HandleFunc(`/lambda/{workerID}/2018-06-01/runtime/invocation/{requestID}/error`, func(w http.ResponseWriter, r *http.Request) {
		workerID := r.PathValue("workerID")
		requestID := r.PathValue("requestID")
		log.Info("got error", "workerID", workerID, "requestID", r.PathValue("requestID"))
		release(workerID)
		writer := input.client.NewWriter(bridge.MessageError, input.prefix+"/"+workerID+"/in")
		writer.SetID(requestID)
		var buf bytes.Buffer
		tee := io.TeeReader(r.Body, &buf)
		io.Copy(writer, tee)
		if err := writer.Close(); err != nil {
			log.Error("failed to send to the bridge", "workerID", workerID, "err", err)
		}
		remember(requestID, &sentReply{bridge.MessageError, workerID, buf.Bytes(), time.Now()})
		w.WriteHeader(202)
		info, ok := workers[workerID]
		if ok {
			fee := &FunctionErrorEvent{
				FunctionID: info.FunctionID,
				WorkerID:   info.WorkerID,
				RequestID:  requestID,
			}
			json.Unmarshal(buf.Bytes(), &fee)
			bus.Publish(fee)
		}
	})

	workerEnv := map[string][]string{}
	builds := map[string]*runtime.BuildOutput{}
	targets := map[string]*runtime.BuildInput{}

	getBuildOutput := func(functionID string) *runtime.BuildOutput {
		build := builds[functionID]
		if build != nil {
			return build
		}
		target, _ := targets[functionID]
		build, err := input.project.Runtime.Build(ctx, target)
		if err == nil {
			bus.Publish(&FunctionBuildEvent{
				FunctionID: functionID,
				Errors:     build.Errors,
			})
		} else {
			bus.Publish(&FunctionBuildEvent{
				FunctionID: functionID,
				Errors:     []string{err.Error()},
			})
		}
		if err != nil || len(build.Errors) > 0 {
			delete(builds, functionID)
			return nil
		}
		builds[functionID] = build
		return build
	}

	run := func(functionID string, workerID string) bool {
		build := getBuildOutput(functionID)
		if build == nil {
			return false
		}
		target, ok := targets[functionID]
		if !ok {
			return false
		}
		worker, err := input.project.Runtime.Run(ctx, &runtime.RunInput{
			CfgPath:    input.project.PathConfig(),
			Runtime:    target.Runtime,
			Server:     server + workerID,
			WorkerID:   workerID,
			FunctionID: functionID,
			Build:      build,
			Env:        workerEnv[workerID],
		})
		if err != nil {
			log.Error("failed to run worker", "error", err)
			return false
		}
		info := &WorkerInfo{
			FunctionID: functionID,
			Worker:     worker,
			WorkerID:   workerID,
			StartedAt:  time.Now(),
		}
		go func() {
			logs := worker.Logs()
			scanner := bufio.NewScanner(logs)
			for scanner.Scan() {
				line := scanner.Text()
				bus.Publish(&FunctionLogEvent{
					FunctionID: functionID,
					WorkerID:   workerID,
					RequestID:  info.CurrentRequestID,
					Line:       line,
				})
			}
			workerShutdownChan <- info
		}()
		workers[workerID] = info

		return true
	}

	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-input.msg:
			switch msg.Type {
			case bridge.MessageResend:
				var body bridge.ResendBody
				json.NewDecoder(msg.Body).Decode(&body)
				sentLock.Lock()
				reply, ok := sent[body.RequestID]
				sentLock.Unlock()
				// Not sent yet means the function is still running; the bridge asks again.
				if !ok {
					continue
				}
				writer := input.client.NewWriter(reply.message, input.prefix+"/"+reply.workerID+"/in")
				writer.SetID(body.RequestID)
				writer.Write(reply.body)
				if err := writer.Close(); err != nil {
					log.Error("failed to resend to the bridge", "workerID", reply.workerID, "err", err)
					continue
				}
				log.Warn("resent a reply the bridge did not receive", "workerID", reply.workerID, "requestID", body.RequestID)
				continue
			case bridge.MessageInit:
				ch, ok := nextChan[msg.Source]
				if !ok {
					ch = make(chan io.Reader, 100)
					nextChan[msg.Source] = ch
				}
				init := bridge.InitBody{}
				json.NewDecoder(msg.Body).Decode(&init)
				if _, ok := targets[init.FunctionID]; !ok {
					log.Error("function not found", "functionID", init.FunctionID)
					continue
				}
				workerID := msg.Source
				if _, ok := workers[workerID]; ok {
					log.Error("got reboot but worker already exists", "workerID", workerID, "functionID", init.FunctionID)
					continue
				}
				log.Info("worker init", "workerID", msg.Source, "functionID", init.FunctionID)
				workerEnv[workerID] = init.Environment
				if ok := run(init.FunctionID, workerID); !ok {
					result, err := http.Post("http://"+server+workerID+"/runtime/init/error", "application/json", strings.NewReader(`{"errorMessage":"Function failed to build"}`))
					if err != nil {
						continue
					}
					defer result.Body.Close()
					body, err := io.ReadAll(result.Body)
					if err != nil {
						continue
					}
					log.Info("error", "body", string(body), "status", result.StatusCode)

					if result.StatusCode != 202 {
						result, err := http.Get("http://" + server + workerID + "/runtime/invocation/next")
						if err != nil {
							continue
						}
						requestID := result.Header.Get("lambda-runtime-aws-request-id")
						result, err = http.Post("http://"+server+workerID+"/runtime/invocation/"+requestID+"/error", "application/json", strings.NewReader(`{"errorMessage":"Function failed to build"}`))
						if err != nil {
							continue
						}
						defer result.Body.Close()
						body, err := io.ReadAll(result.Body)
						if err != nil {
							continue
						}
						log.Info("error", "body", string(body), "status", result.StatusCode)
					}
				}
			case bridge.MessageNext:
				writer := input.client.NewWriter(bridge.MessagePing, input.prefix+"/"+msg.Source+"/in")
				json.NewEncoder(writer).Encode(bridge.PingBody{})
				if err := writer.Close(); err != nil {
					log.Error("failed to send ping", "workerID", msg.Source, "err", err)
				}
				ch, ok := nextChan[msg.Source]
				if !ok {
					ch = make(chan io.Reader, 100)
					nextChan[msg.Source] = ch
				}
				_, ok = workers[msg.Source]
				if !ok {
					log.Info("asking for reboot", "workerID", msg.Source)
					writer := input.client.NewWriter(bridge.MessageReboot, input.prefix+"/"+msg.Source+"/in")
					json.NewEncoder(writer).Encode(bridge.RebootBody{})
					writer.Close()
				}
				ch <- msg.Body
				continue
			}

		case info := <-workerShutdownChan:
			log.Info("worker died", "workerID", info.WorkerID)
			existing, ok := workers[info.WorkerID]
			if !ok {
				continue
			}
			// only delete if a new worker hasn't already been started
			if existing == info {
				inflightLock.Lock()
				held, holding := inflight[info.WorkerID]
				delete(inflight, info.WorkerID)
				inflightLock.Unlock()
				ch := nextChan[info.WorkerID]
				// A request handed over and unanswered, or one queued behind a worker that had been
				// running a while (not one that died on startup).
				retry := holding && held.attempts < 2
				queued := ch != nil && len(ch) > 0 && time.Since(info.StartedAt) > 5*time.Second
				if retry || queued {
					log.Warn("worker exited holding a request; restarting it", "workerID", info.WorkerID)
					delete(workers, info.WorkerID)
					if run(info.FunctionID, info.WorkerID) {
						if retry {
							inflightLock.Lock()
							inflight[info.WorkerID] = held
							inflightLock.Unlock()
							ch <- bytes.NewReader(held.raw)
						}
						break
					}
				}
				log.Info("deleting worker", "workerID", info.WorkerID)
				delete(workers, info.WorkerID)
				delete(nextChan, info.WorkerID)
			}
			break
		case unknown := <-evts:
			switch evt := unknown.(type) {
			case *FunctionInvokedEvent:
				info, ok := workers[evt.WorkerID]
				if !ok {
					continue
				}
				info.CurrentRequestID = evt.RequestID
			case *project.CompleteEvent:
				if evt.Old {
					continue
				}
				for _, info := range workers {
					info.Worker.Stop()
				}
				builds = map[string]*runtime.BuildOutput{}
				for workerID, info := range workers {
					run(info.FunctionID, workerID)
				}
			case *runtime.BuildInput:
				targets[evt.FunctionID] = evt
			case *watcher.FileChangedEvent:
				log.Info("checking if code needs to be rebuilt", "file", evt.Path)
				toBuild := map[string]bool{}

				for functionID := range builds {
					target, ok := targets[functionID]
					if !ok {
						continue
					}
					if input.project.Runtime.ShouldRebuild(target.Runtime, target.FunctionID, evt.Path) {
						for _, worker := range workers {
							if worker.FunctionID == functionID {
								log.Info("stopping", "workerID", worker.WorkerID, "functionID", worker.FunctionID)
								worker.Worker.Stop()
							}
						}
						delete(builds, functionID)
						toBuild[functionID] = true
					}
				}

				for functionID := range toBuild {
					output := getBuildOutput(functionID)
					if output == nil {
						delete(toBuild, functionID)
					}
				}

				for workerID, info := range workers {
					if toBuild[info.FunctionID] {
						run(info.FunctionID, workerID)
					}
				}
				break
			}
		}
	}
}

func fileLogger(p *project.Project) {
	evts := bus.Subscribe(&FunctionLogEvent{}, &FunctionInvokedEvent{}, &FunctionResponseEvent{}, &FunctionErrorEvent{}, &FunctionBuildEvent{})
	logs := map[string]*os.File{}

	getLog := func(functionID string, requestID string) *os.File {
		log, ok := logs[requestID]
		if !ok {
			path := p.PathLog(fmt.Sprintf("lambda/%s/%d-%s", functionID, time.Now().Unix(), requestID))
			os.MkdirAll(filepath.Dir(path), 0755)
			log, _ = os.Create(path)
			logs[requestID] = log
		}
		return log
	}

	for range evts {
		for evt := range evts {
			switch evt := evt.(type) {
			case *FunctionInvokedEvent:
				log := getLog(evt.FunctionID, evt.RequestID)
				log.WriteString("invocation " + evt.RequestID + "\n")
				log.WriteString(string(evt.Input))
				log.WriteString("\n")
			case *FunctionLogEvent:
				getLog(evt.FunctionID, evt.RequestID).WriteString(evt.Line + "\n")
			case *FunctionResponseEvent:
				log := getLog(evt.FunctionID, evt.RequestID)
				log.WriteString("response " + evt.RequestID + "\n")
				log.WriteString(string(evt.Output))
				log.WriteString("\n")
				delete(logs, evt.RequestID)
			case *FunctionErrorEvent:
				getLog(evt.FunctionID, evt.RequestID).WriteString(evt.ErrorType + ": " + evt.ErrorMessage + "\n")
				delete(logs, evt.RequestID)
			}
		}
	}
}
