package worker

import (
	"context"
	"io"
	"io/fs"
	"net/url"
	"os"
	"os/signal"
	"path"
	"strings"
	"syscall"
	"time"

	"github.com/Fishwaldo/go-logadapter"
	"github.com/Fishwaldo/restic-nats-server/internal"
	"github.com/Fishwaldo/restic-nats-server/internal/cache"
	"github.com/Fishwaldo/restic-nats-server/internal/natsserver"

	"github.com/Fishwaldo/restic-nats-server/internal/backend/localfs"
	"github.com/Fishwaldo/restic-nats-server/protocol"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

type WorkerData struct {
	ID     int
	Be     *internal.Backend
	cancel context.CancelFunc
	Log    logadapter.Logger
}

type workerCfgT struct {
	NumWorkers int
	ConnectURL string
	NKey string
	Credfile string
	Backends []string
}

var WorkerCfg workerCfgT


func init() {
	internal.ConfigRegister("worker", parseConfig, validateConfig)
	viper.SetDefault("worker.number", 10)
	viper.SetDefault("worker.connecturl", "nats://localhost:4222")
	viper.SetDefault("worker.handles", "*")
}

func parseConfig(cfg *viper.Viper) (error) {
	WorkerCfg.NumWorkers = cfg.GetInt("number")
	WorkerCfg.ConnectURL = cfg.GetString("connecturl")
	WorkerCfg.NKey = cfg.GetString("nkey")
	WorkerCfg.Credfile = cfg.GetString("credfile")
	WorkerCfg.Backends = cfg.GetStringSlice("handles")
	return nil
}
func validateConfig() (warnings []error, errors error) {
	internal.Log.Info("%+v\n", WorkerCfg)
	return nil, nil
}

func StartWorker() {
	var url *url.URL
	if viper.GetBool("start-nats-server") {
		url, _ = natsserver.GetInternalWorkerURL()
	} else {
		url, _ = url.Parse(WorkerCfg.ConnectURL)
	}

	cfg := internal.Config{
		Server:     url,
		//Credential: "/home/fish/.nkeys/creds/Operator/Backup/restic.creds",
	}
	var repo string
	if cfg.Server.Path[0] == '/' {
		repo = cfg.Server.Path[1:]
	}
	if repo[len(repo)-1] == '/' {
		repo = repo[0 : len(repo)-1]
	}
	// replace any further slashes with . to specify a nested queue
	repo = strings.Replace(repo, "/", ".", -1)

	cfg.Repo = repo

	be := &internal.Backend{
		Cfg: cfg,
	}
	if err := connectNats(be); err != nil {
		internal.Log.Fatal("Error Connecting to Nats: %s", err)
		os.Exit(0)
	}

	for i :=0; i < WorkerCfg.NumWorkers; i++ {
		wd := WorkerData{ID: i, Be: be, Log: internal.Log.New("worker").With("ID", i)}
		be.T.Go(wd.Run);
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT)

	s := <-signalChan
	be.T.Kill(nil)
	if err := be.T.Wait(); err != nil {
		internal.Log.Warn("Workers Reported Error: %s", err)
	}
	
	internal.Log.Warn("Got Shutdown Signal %s", s)
	cache.Shutdown()
	natsserver.Shutdown()
	os.Exit(0)
}




func (wd *WorkerData) Run() error {
	for {
		var ctx context.Context
		ctx, wd.cancel = context.WithCancel(context.Background())
		netctx, _ := context.WithTimeout(ctx, 1*time.Second)
		select {
		case <-wd.Be.T.Dying():
			wd.Log.Warn("Killing Worker")
			wd.cancel()
			return nil
		default:
		}
		//wd.Be.Mx.Lock()
		msg, err := wd.Be.BuCommands.NextMsgWithContext(netctx)
		//wd.Be.Mx.Unlock()
		if err != nil {
			if !errors.Is(err, context.DeadlineExceeded) {
				wd.Log.Error("NextMsg Failed: Error: %s %t", err, err)
				return err
			}
			continue
		}
		if msg != nil {
			jobctx, _ := context.WithTimeout(ctx, 120*time.Second)
			wd.Log.Info("Message: %s %s", msg.Subject, msg.Sub.Queue)
			start := time.Now()

			log := func(msg string, args ...interface{}) {
				wd.Log.Info(msg, args...)
			}

			msg, err := protocol.ChunkReadRequestMsgWithContext(jobctx, wd.Be.Conn, msg, log)
			if err != nil {
				wd.Log.Warn("ChunkedRead Failed: %s", err)
				continue
			}

			operation := msg.Header.Get("X-RNS-OP")
			switch operation {
			case "open":
				var oo protocol.OpenOp
				or := protocol.OpenResult{Ok: false}

				if err := wd.Be.Enc.Decode(msg.Subject, msg.Data, &oo); err != nil {
					wd.Log.Warn("Decode Failed: %s", err)
				}

				or.Ok, err = wd.OpenRepo(oo)
				if err != nil {
					or.Ok = false
					wd.Log.Warn("OpenOp: Error: %s", err)
				}

				replymsg := protocol.NewRNSMsg(msg.Reply)

				replymsg.Data, err = wd.Be.Enc.Encode(msg.Reply, or)
				if err != nil {
					wd.Log.Warn("Encode Failed: %s", wd.ID, err)
				}
				if err = protocol.ChunkSendReplyMsgWithContext(ctx, wd.Be.Conn, msg, replymsg, log); err != nil {
					wd.Log.Warn("ChunkReplyRequestMsgWithContext Failed: %s", err)
				}
			case "stat":
				var so protocol.StatOp
				if err := wd.Be.Enc.Decode(msg.Subject, msg.Data, &so); err != nil {
					return errors.Wrap(err, "Decode Failed")
				}

				fi, err := wd.Stat(so)
				var sr protocol.StatResult
				if err != nil {
					wd.Log.Warn("Stat: Error: %s\n", err)
					sr.Ok = false
				} else {
					sr.Ok = true
					sr.Size = fi.Size()
					sr.Name = fi.Name()
				}
				replymsg := protocol.NewRNSMsg(msg.Reply)
				replymsg.Data, err = wd.Be.Enc.Encode(msg.Reply, sr)
				if err != nil {
					return errors.Wrap(err, "Encode Failed")
				}
				if err = protocol.ChunkSendReplyMsgWithContext(ctx, wd.Be.Conn, msg, replymsg, log); err != nil {
					wd.Log.Warn("ChunkReplyRequestMsgWithContext Failed: %s", err)
				}
			case "mkdir":

				var mo protocol.MkdirOp
				if err := wd.Be.Enc.Decode(msg.Subject, msg.Data, &mo); err != nil {
					return errors.Wrap(err, "Decode Failed")
				}

				var mr protocol.MkdirResult
				err := wd.Mkdir(mo)
				if err != nil {
					wd.Log.Warn("Mkdir: Error: %s", err)
					mr.Ok = false
				} else {
					mr.Ok = true
				}
				replymsg := protocol.NewRNSMsg(msg.Reply)
				replymsg.Data, err = wd.Be.Enc.Encode(msg.Reply, mr)
				if err != nil {
					return errors.Wrap(err, "Encode Failed")
				}
				if err = protocol.ChunkSendReplyMsgWithContext(ctx, wd.Be.Conn, msg, replymsg, log); err != nil {
					wd.Log.Warn("ChunkReplyRequestMsgWithContext Failed: %s", err)
				}
			case "save":
				var so protocol.SaveOp
				if err := wd.Be.Enc.Decode(msg.Subject, msg.Data, &so); err != nil {
					return errors.Wrap(err, "Decode Failed")
				}

				var sr protocol.SaveResult
				err := wd.Save(so)
				if err != nil {
					wd.Log.Warn("Save: Error: %s", err)
					sr.Ok = false
				} else {
					sr.Ok = true
				}
				wd.Log.Warn("Save Result: %+v", sr)
				replymsg := protocol.NewRNSMsg(msg.Reply)
				replymsg.Data, err = wd.Be.Enc.Encode(msg.Reply, sr)
				wd.Log.Warn("Reply is %d", len(replymsg.Data))
				if err != nil {
					return errors.Wrap(err, "Encode Failed")
				}
				if err = protocol.ChunkSendReplyMsgWithContext(ctx, wd.Be.Conn, msg, replymsg, log); err != nil {
					wd.Log.Warn("ChunkSendReplyMsgWithContext Failed: %s", err)
				}
			case "list":
				var lo protocol.ListOp
				if err := wd.Be.Enc.Decode(msg.Subject, msg.Data, &lo); err != nil {
					return errors.Wrap(err, "Decode Failed")
				}

				var lr protocol.ListResult
				lr, err = wd.List(lo)
				if err != nil {
					wd.Log.Warn("List: Error: %s", err)
					lr.Ok = false
				} else {
					lr.Ok = true
				}
				replymsg := protocol.NewRNSMsg(msg.Reply)
				replymsg.Data, err = wd.Be.Enc.Encode(msg.Reply, lr)
				if err != nil {
					return errors.Wrap(err, "Encode Failed")
				}
				if err = protocol.ChunkSendReplyMsgWithContext(ctx, wd.Be.Conn, msg, replymsg, log); err != nil {
					wd.Log.Warn("ChunkReplyRequestMsgWithContext Failed: %s", err)
				}
			case "load":
				var lo protocol.LoadOp
				if err := wd.Be.Enc.Decode(msg.Subject, msg.Data, &lo); err != nil {
					return errors.Wrap(err, "Decode Failed")
				}

				var lr protocol.LoadResult
				lr, err = wd.Load(lo)
				if err != nil {
					wd.Log.Warn("List: Error: %s", err)
					lr.Ok = false
				} else {
					lr.Ok = true
				}
				replymsg := protocol.NewRNSMsg(msg.Reply)
				replymsg.Data, err = wd.Be.Enc.Encode(msg.Reply, lr)
				if err != nil {
					return errors.Wrap(err, "Encode Failed")
				}
				if err = protocol.ChunkSendReplyMsgWithContext(ctx, wd.Be.Conn, msg, replymsg, log); err != nil {
					wd.Log.Warn("ChunkReplyRequestMsgWithContext Failed: %s", err)
				}
			case "remove":
				var ro protocol.RemoveOp
				if err := wd.Be.Enc.Decode(msg.Subject, msg.Data, &ro); err != nil {
					return errors.Wrap(err, "Decode Failed")
				}

				var rr protocol.RemoveResult
				rr, err = wd.Remove(ro)
				if err != nil {
					wd.Log.Warn("List: Error: %s", err)
					rr.Ok = false
				} else {
					rr.Ok = true
				}
				replymsg := protocol.NewRNSMsg(msg.Reply)
				replymsg.Data, err = wd.Be.Enc.Encode(msg.Reply, rr)
				if err != nil {
					return errors.Wrap(err, "Encode Failed")
				}
				if err = protocol.ChunkSendReplyMsgWithContext(ctx, wd.Be.Conn, msg, replymsg, log); err != nil {
					wd.Log.Warn("ChunkReplyRequestMsgWithContext Failed: %s", err)
				}
			}
			wd.Log.Info("Command %s Took %s", operation, time.Since(start))
		}
	}
	return nil
}

func (wd *WorkerData) OpenRepo(oo protocol.OpenOp) (bool, error) {
	fs, err := localfs.FSStat(oo.Bucket)
	if err != nil {
		return false, errors.New("Failed to Open Repository")
	}
	return fs.IsDir(), nil
}

func (wd *WorkerData) Stat(so protocol.StatOp) (fs.FileInfo, error) {
	fs, err := localfs.FSStat(path.Join(so.Bucket, so.Filename))
	if err != nil {
		return nil, errors.Wrap(err, "Stat")
	}
	return fs, nil
}
func (wd *WorkerData) Mkdir(mo protocol.MkdirOp) error {
	path := path.Join(mo.Bucket, mo.Dir)
	if err := localfs.FSMkDir(path); err != nil {
		return err
	}
	return nil
}

func (wd *WorkerData) Save(so protocol.SaveOp) error {
	path := path.Join(so.Bucket, so.Dir, so.Name)
	len, err := localfs.FSSave(path, &so.Data, so.Offset)
	if err != nil {
		return err
	}
	if len != so.PacketSize {
		return errors.New("Packetsize != Writtensize")
	}
	return nil
}

func (wd *WorkerData) List(lo protocol.ListOp) (protocol.ListResult, error) {
	var result protocol.ListResult
	fi, err := localfs.FSListFiles(path.Join(lo.Bucket, lo.BaseDir), lo.SubDir)
	if err != nil {
		return protocol.ListResult{Ok: false}, errors.Wrap(err, "ListFiles")
	}
	result.Ok = true
	result.FI = fi
	return result, nil
}

func (wd *WorkerData) Load(lo protocol.LoadOp) (protocol.LoadResult, error) {
	var result protocol.LoadResult
	rd, err := localfs.FSLoadFile(path.Join(lo.Bucket, lo.Dir, lo.Name))
	if err != nil {
		return protocol.LoadResult{Ok: false}, errors.Wrap(err, "LoadFile")
	}
	defer rd.Close()
	if lo.Offset > 0 {
		_, err = rd.Seek(lo.Offset, 0)
		if err != nil {
			return protocol.LoadResult{Ok: false}, errors.Wrap(err, "Seek")
		}
	}
	if lo.Length > 0 {
		result.Data = make([]byte, lo.Length)
		len, err := rd.Read(result.Data)
		if err != nil {
			return protocol.LoadResult{Ok: false}, errors.Wrap(err, "Read")
		}
		if len != lo.Length {
			return protocol.LoadResult{Ok: false}, errors.Errorf("Requested Length %d != Actual Length %d", lo.Length, len)
		}
	} else {
		result.Data, err = io.ReadAll(rd)
		if err != nil {
			return protocol.LoadResult{Ok: false}, errors.Wrap(err, "ReadAll")
		}
	}
	//fmt.Printf("%+v\n", result)
	return result, nil
}

func (wd *WorkerData) Remove(ro protocol.RemoveOp) (protocol.RemoveResult, error) {
	var result protocol.RemoveResult
	if err := localfs.FSRemove(path.Join(ro.Bucket, ro.Dir, ro.Name)); err != nil {
		return protocol.RemoveResult{Ok: false}, errors.Wrap(err, "FSRemove")
	}
	result.Ok = true
	return result, nil
}
