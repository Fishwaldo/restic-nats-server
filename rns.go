package main

import (
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/Fishwaldo/restic-nats-server/cache"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"gopkg.in/tomb.v2"
)

type Config struct {
	Server      *url.URL
	Credential  string `option:"credentialfile" help:"Path to the NatsIO Credential File"`
	Connections uint   `option:"connections" help:"set a limit for the number of concurrent connections (default: 5)"`
	Repo        string
}

type Backend struct {
	cfg      Config
	conn     *nats.Conn
	// js       nats.JetStreamContext
	// buStream *nats.StreamInfo
	buCommands *nats.Subscription
	mx sync.Mutex
	t tomb.Tomb
	enc nats.Encoder
}




func createStreamConfig(name string) *nats.StreamConfig {
	var subjects []string
	subjects = append(subjects, "repo.commands.>")
	sc := nats.StreamConfig{
		Storage:     nats.FileStorage,
//		Description: fmt.Sprintf("Stream for %s", name),
		Retention:   nats.WorkQueuePolicy,
		Name:        name,
		Subjects:    subjects,
	}
	return &sc
}

//  func createConsumerBUConfig() *nats.ConsumerConfig {
//  	cc := nats.ConsumerConfig {
//  		Description: fmt.Sprintf("Consumer for me"),
//  		//DeliverGroup: "workers",
//  		DeliverPolicy: nats.DeliverAllPolicy,
//  		AckPolicy: nats.AckExplicitPolicy,
//  		FilterSubject: "repo.commands.open",
// 		//Durable: "Workers",
//  	}
//  	return &cc
//  }

func connectNats(be *Backend) error {
	be.mx.Lock()
	defer be.mx.Unlock()
	server := be.cfg.Server.Hostname()
	port := be.cfg.Server.Port()
	if port == "" {
		port = "4222"
	}
	url := fmt.Sprintf("nats://%s:%s", server, port)

	/* Check Credential File Exists */
	_, err := os.Stat(be.cfg.Credential)
	if err != nil {
		return errors.Wrap(err, "credential file missing")
	}

	var options []nats.Option
	options = append(options, nats.UserCredentials(be.cfg.Credential))
	options = append(options, nats.ClosedHandler(natsClosedCB))
	options = append(options, nats.DisconnectHandler(natsDisconnectedCB))

	be.conn, err = nats.Connect(url, options...)
	if err != nil {
		return errors.Wrap(err, "nats connection failed")
	}
	if size := be.conn.MaxPayload(); size < 8388608 {
		return errors.New("NATS Server Max Payload Size is below 8Mb")
	}

	fmt.Printf("Connected to %s (%s)\n", be.conn.ConnectedClusterName(), be.conn.ConnectedServerName())

	fmt.Printf("Nats Message Size: %d\n", be.conn.MaxPayload())

	be.enc = nats.EncoderForType("gob")

	// be.js, err = be.conn.JetStream()
	// if err != nil {
	// 	return errors.Wrap(err, "can't get nats jetstream context")
	// }
	// fmt.Printf("Stream Name: %s\n", be.cfg.Repo)
	// be.buStream, err = be.js.StreamInfo(be.cfg.Repo)
	// if err != nil {
	// 	if err == nats.ErrStreamNotFound {
	// 		/* create the Stream */
	// 		sc := createStreamConfig(be.cfg.Repo)
	// 		fmt.Printf("Creating Stream %s\n", sc.Name)
	// 		be.buStream, err = be.js.AddStream(sc)
	// 		if err != nil {
	// 			return errors.Wrapf(err, "Can't Create Stream %#v", sc)
	// 		}
	// 	} else {
	// 		return errors.Wrap(err, "Error getting StreamInfo")
	// 	}
	// }
	// var subOps []nats.SubOpt
	// //subOps = append(subOps, nats.Durable("commands"))
	// subOps = append(subOps, nats.AckExplicit())
	// subOps = append(subOps, nats.BindStream(be.buStream.Config.Name))
	// subOps = append(subOps, nats.MaxAckPending(5))
	be.buCommands, err = be.conn.QueueSubscribeSync("repo.commands.*", "opencommandworkers")
	if err != nil {
		return errors.Wrap(err, "Can't Add Consumer")
	}
//	fmt.Printf("Stream %#v\n", be.buStream)
	return nil
}

func natsClosedCB(conn *nats.Conn) {
	fmt.Printf("Connection Closed: %s", conn.LastError())
}

func natsDisconnectedCB(conn *nats.Conn) {
	fmt.Printf("Connection Disconnected: %s", conn.LastError())
}

func main() {

	url, _ := url.Parse("nats://gw-docker-1.dmz.dynam.ac:4222/backup")

	cfg := Config{
		Server:     url,
		Credential: "/home/fish/.nkeys/creds/Operator/Backup/restic.creds",
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

	be := &Backend{
		cfg: cfg,
	}
	if err := connectNats(be); err != nil {
		fmt.Printf("Error Connecting to Nats: %s\n", err)
		os.Exit(0)
	}

	for i :=0; i < 50; i++ {
		wd := WorkerData{id: i, be: be}
		be.t.Go(wd.Run);
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT)

	s := <-signalChan
	be.t.Kill(errors.New("Shutting Down"))
	if err := be.t.Wait(); err != nil {
		fmt.Printf("Workers Reported Error: %s", err)
	}
	
	fmt.Printf("Got Shutdown Signal %s", s)
	cache.ShutdownCache()
	os.Exit(0)

}
