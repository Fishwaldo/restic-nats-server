// +build !nocacheserver

package cache

import (
	"bytes"
	"context"
	stdlog "log"
	"time"

	"github.com/Fishwaldo/go-logadapter"

	"github.com/Fishwaldo/restic-nats-server/internal"
	"github.com/buraksezer/olric"
	"github.com/buraksezer/olric/config"
	"github.com/spf13/viper"
)

var CacheDM *olric.DMap
var db *olric.Olric

var log logadapter.Logger

func init() {
	log = internal.Log.New("olric")
	viper.SetDefault("start-cache-server", true)
}


func Start() {
	if !viper.GetBool("start-cache-server") {
		log.Info("Not Starting Cache Server")
		return
	}

	// Deployment scenario: embedded-member
	// This creates a single-node Olric cluster. It's good enough for experimenting.

	// config.New returns a new config.Config with sane defaults. Available values for env:
	// local, lan, wan
	c := config.New("lan")
	c.Logger = stdlog.New(&olrisLogger{log: log}, "" /* prefix */, 0 /* flags */)

	// Callback function. It's called when this node is ready to accept connections.
	ctx, cancel := context.WithCancel(context.Background())
	c.Started = func() {
		defer cancel()
		log.Info("Olric is ready to accept connections")
	}
	var err error
	db, err = olric.New(c)
	if err != nil {
		log.Fatal("Failed to create Olric instance: %v", err)
	}

	go func() {
		// Call Start at background. It's a blocker call.
		err = db.Start()
		if err != nil {
			log.Fatal("olric.Start returned an error: %v", err)
		}
	}()
	<-ctx.Done()

	CacheDM, err = db.NewDMap("bucket-of-arbitrary-items")
	if err != nil {
		log.Fatal("olric.NewDMap returned an error: %v", err)
	}
}

func Shutdown() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	err := db.Shutdown(ctx)
	if err != nil {
		log.Fatal("Failed to shutdown Olric: %v", err)
	}
	cancel()
}

type olrisLogger struct {
	log logadapter.Logger
}

func (ol olrisLogger) Write(message  []byte) (int, error) {
	message = bytes.TrimSpace(message)
	size := len(message)
	if size < 2 {
	  ol.log.Info(string(message))
	} else {
	  switch message[1] {
	  case 'I':
		ol.log.Info(string(message[7:]))
	  case 'W':
		ol.log.Warn(string(message[7:]))
	  case 'D':
		ol.log.Debug(string(message[8:]))
	  default:
		ol.log.Info(string(message))
	  }
	}
	return size, nil
}

