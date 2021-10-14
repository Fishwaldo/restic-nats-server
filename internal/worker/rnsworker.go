package worker

import (
	"context"
	"io"
	"net/url"
	"os"
	"os/signal"
	"path"
	"syscall"
	"time"

	"github.com/Fishwaldo/restic-nats-server/internal"
	"github.com/Fishwaldo/restic-nats-server/internal/backend/localfs"
	"github.com/Fishwaldo/restic-nats-server/internal/cache"
	"github.com/Fishwaldo/restic-nats-server/internal/client"
	"github.com/Fishwaldo/restic-nats-server/internal/natsserver"
	"github.com/nats-io/nats.go"

	"github.com/Fishwaldo/go-logadapter"
	rns "github.com/Fishwaldo/restic-nats"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

type Worker struct {
	ID     int
	cancel context.CancelFunc
	Log    logadapter.Logger
	Conn   *rns.ResticNatsClient
}

func init() {
	internal.ConfigRegister("worker", parseConfig, validateConfig)
	viper.SetDefault("worker.number", 10)
	viper.SetDefault("worker.connecturl", "nats://localhost:4222")
	viper.SetDefault("worker.handles", "*")
}

func parseConfig(cfg *viper.Viper) error {
	var err error
	internal.GlobalState.WorkerConfig.NumWorkers = cfg.GetInt("number")
	internal.GlobalState.NatsConfig.NatsURL, err = url.Parse(cfg.GetString("connecturl"))
	if err != nil {
		return err
	}
	internal.GlobalState.NatsConfig.NatsNKey = cfg.GetString("nkey")
	internal.GlobalState.NatsConfig.NatsCredfile = cfg.GetString("credfile")
	//WorkerCfg.Backends = cfg.GetStringSlice("handles")
	return nil
}
func validateConfig() (warnings []error, err error) {
	if viper.GetBool("start-nats-server") &&
		internal.GlobalState.NatsConfig.NatsURL.String() != "" {
		warnings = append(warnings, errors.New("Using Internal Nats Server. Ignoring Nats Credentials/URL"))
		url, _ := natsserver.GetInternalWorkerURL()
		internal.GlobalState.NatsConfig.NatsURL = url
	} else if viper.GetBool("start-nats-server") {
		url, _ := natsserver.GetInternalWorkerURL()
		internal.GlobalState.NatsConfig.NatsURL = url
	} else {
		if internal.GlobalState.NatsConfig.NatsURL.User.Username() != "" && internal.GlobalState.NatsConfig.NatsNKey != "" {
			return nil, errors.New("Cannot Set a Username and Nkey at the same time")
		}
		if internal.GlobalState.NatsConfig.NatsURL.User.Username() != "" && internal.GlobalState.NatsConfig.NatsCredfile != "" {
			return nil, errors.New("Cannot Set a Username and Credential file at the same time")
		}
		/* stat the Creds File if it exists */
		if internal.GlobalState.NatsConfig.NatsCredfile != "" {
			f, err := os.Open(internal.GlobalState.NatsConfig.NatsCredfile)
			if err != nil {
				return nil, errors.Wrap(err, "Cannot find Credential File")
			}
			f.Close()
		}

	}
	return warnings, nil
}

func StartWorker() {
	var options []rns.RNSOptions

	if internal.GlobalState.NatsConfig.NatsCredfile != "" {
		options = append(options, rns.WithCredentials(internal.GlobalState.NatsConfig.NatsCredfile))
	} else if internal.GlobalState.NatsConfig.NatsNKey != "" {
		//XXX TODO
		internal.Log.Fatal("NKey Authentication TODO")
	}
	options = append(options, rns.WithLogger(internal.Log.New("RNSClient")))
	host, _ := os.Hostname()
	options = append(options, rns.WithName(host))
	options = append(options, rns.WithServer())

	internal.Log.Debug("Connecting to %s", internal.GlobalState.NatsConfig.NatsURL)

	conn, err := rns.New(*internal.GlobalState.NatsConfig.NatsURL, options...)
	if err != nil {
		internal.Log.Fatal("Cannot Create a new RNS Connection: %s", err)
	}
	internal.GlobalState.Conn = conn

	internal.Log.Debug("Connected to Nats Server %s (%s)", conn.Conn.ConnectedServerName(), conn.Conn.ConnectedClusterName())

	/* setup our Subscription for Client Commands */
	internal.GlobalState.ClientCommand = make(chan *nats.Msg, 5)
	sub, err := internal.GlobalState.Conn.Conn.ChanQueueSubscribe("repo.Hosts.commands.*", "workerqueue", internal.GlobalState.ClientCommand)
	if err != nil {
		internal.Log.Fatal("Cant Setup Client Command Subscription: %s", err)
		return
	}
	internal.GlobalState.ClientCommandSubscription = sub

	for i := 0; i < internal.GlobalState.WorkerConfig.NumWorkers; i++ {
		wd := Worker{ID: i,
			Log:  internal.Log.New("worker").With("ID", i),
			Conn: internal.GlobalState.Conn}
		internal.GlobalState.T.Go(wd.Run)
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT)

	s := <-signalChan
	internal.GlobalState.T.Kill(nil)
	if err := internal.GlobalState.T.Wait(); err != nil {
		internal.Log.Warn("Workers Reported Error: %s", err)
	}

	internal.Log.Warn("Got Shutdown Signal %s", s)
	cache.Shutdown()
	natsserver.Shutdown()
	os.Exit(0)
}

func (wd *Worker) Run() error {
	wd.Log.Trace("Worker Started")
	rnsServer, err := rns.NewRNSServer(wd, wd.Conn, wd.Log.New("RNSServer"))
	if err != nil {
		wd.Log.Warn("NewRNSServer Failed: %s", err)
		return err
	}

	for {
		var ctx context.Context
		ctx, wd.cancel = context.WithCancel(context.Background())
		var msg *nats.Msg
		select {
		case <-internal.GlobalState.T.Dying():
			wd.Log.Warn("Killing Worker")
			wd.cancel()
			return nil
		case msg = <-internal.GlobalState.ClientCommand:
		}
		jobctx, _ := context.WithTimeout(ctx, 120*time.Second)
		start := time.Now()

		if err := rnsServer.ProcessServerMsg(jobctx, msg); err != nil {
			wd.Log.Warn("Process Client Message Failed: %s", err)
			continue
		}
		wd.Log.Info("Command Took %s", time.Since(start))
	}
	return nil
}

func (wd *Worker) LookupClient(clientid string) (rns.Client, error) {
	return client.Find(clientid)
}

func (wd *Worker) Open(ctx context.Context, oo rns.OpenRepoOp) (rns.OpenRepoResult, rns.Client, error) {
	_, err := localfs.FSStat(oo.Bucket)
	or := rns.OpenRepoResult{}
	if err != nil {
		or.Err = errors.New("Repository Not Found")
		return or, rns.Client{}, errors.New("Failed to Open Repository")
	}

	/* create a new Client */
	rnsclient, err := client.Create(oo)
	if err != nil {
		return or, rns.Client{}, errors.Wrap(err, "ClientCreate")
	}
	or.Ok = true
	or.ClientID = rnsclient.ClientID

	return or, rnsclient, nil
}

func (wd *Worker) Stat(ctx context.Context, rnsclient rns.Client, so rns.StatOp) (rns.StatResult, error) {
	fs, err := localfs.FSStat(path.Join(rnsclient.Bucket, so.Filename))
	if err != nil {
		return rns.StatResult{Ok: false}, errors.Wrap(err, "Stat")
	}
	sr := rns.StatResult{
		Ok:   true,
		Name: fs.Name(),
		Size: fs.Size(),
	}
	return sr, nil
}
func (wd *Worker) Mkdir(ctx context.Context, rnsclient rns.Client, mo rns.MkdirOp) (rns.MkdirResult, error) {
	path := path.Join(rnsclient.Bucket, mo.Dir)
	if err := localfs.FSMkDir(path); err != nil {
		return rns.MkdirResult{Ok: false}, errors.Wrap(err, "Mkdir")
	}
	return rns.MkdirResult{Ok: true}, nil
}

func (wd *Worker) Save(ctx context.Context, rnsclient rns.Client, so rns.SaveOp) (rns.SaveResult, error) {
	path := path.Join(rnsclient.Bucket, so.Dir, so.Name)
	len, err := localfs.FSSave(path, &so.Data)
	if err != nil {
		return rns.SaveResult{Ok: false}, errors.Wrap(err, "Save")
	}
	if len != int(so.Filesize) {
		return rns.SaveResult{Ok: false}, errors.New("Packetsize != Writtensize")
	}
	return rns.SaveResult{Ok: true}, nil
}

func (wd *Worker) List(ctx context.Context, rnsclient rns.Client, lo rns.ListOp) (rns.ListResult, error) {
	var result rns.ListResult
	fi, err := localfs.FSListFiles(path.Join(rnsclient.Bucket, lo.BaseDir), lo.Recurse)
	if err != nil {
		return rns.ListResult{Ok: false}, errors.Wrap(err, "List")
	}
	result.Ok = true
	result.FI = fi
	return result, nil
}

func (wd *Worker) Load(ctx context.Context, rnsclient rns.Client, lo rns.LoadOp) (rns.LoadResult, error) {
	var result rns.LoadResult
	rd, err := localfs.FSLoadFile(path.Join(rnsclient.Bucket, lo.Dir, lo.Name))
	if err != nil {
		return rns.LoadResult{Ok: false}, errors.Wrap(err, "Load")
	}
	defer rd.Close()
	if lo.Offset > 0 {
		_, err = rd.Seek(lo.Offset, 0)
		if err != nil {
			return rns.LoadResult{Ok: false}, errors.Wrap(err, "Seek")
		}
	}
	if lo.Length > 0 {
		result.Data = make([]byte, lo.Length)
		len, err := rd.Read(result.Data)
		if err != nil {
			return rns.LoadResult{Ok: false}, errors.Wrap(err, "Read")
		}
		if len != lo.Length {
			return rns.LoadResult{Ok: false}, errors.Errorf("Requested Length %d != Actual Length %d", lo.Length, len)
		}
	} else {
		result.Data, err = io.ReadAll(rd)
		if err != nil {
			return rns.LoadResult{Ok: false}, errors.Wrap(err, "ReadAll")
		}
	}
	result.Ok = true
	return result, nil
}

func (wd *Worker) Remove(ctx context.Context, rnsclient rns.Client, ro rns.RemoveOp) (rns.RemoveResult, error) {
	var result rns.RemoveResult
	if err := localfs.FSRemove(path.Join(rnsclient.Bucket, ro.Dir, ro.Name)); err != nil {
		return rns.RemoveResult{Ok: false}, errors.Wrap(err, "Remove")
	}
	result.Ok = true
	return result, nil
}

func (wd *Worker) Close(ctx context.Context, rnsclient rns.Client, co rns.CloseOp) (rns.CloseResult, error) {
	if err := client.Remove(rnsclient.ClientID); err != nil {
		wd.Log.Warn("Can't Find Client %s", rnsclient.ClientID)
	}
	/* always return success */
	return rns.CloseResult{Ok: true}, nil
}
