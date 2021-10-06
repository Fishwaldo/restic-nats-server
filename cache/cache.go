package cache

import (
	"context"
	"log"
	"time"

	"github.com/buraksezer/olric"
	"github.com/buraksezer/olric/config"
)

var CacheDM *olric.DMap
var db *olric.Olric

func init() {
	// Deployment scenario: embedded-member
	// This creates a single-node Olric cluster. It's good enough for experimenting.

	// config.New returns a new config.Config with sane defaults. Available values for env:
	// local, lan, wan
	c := config.New("lan")

	// Callback function. It's called when this node is ready to accept connections.
	ctx, cancel := context.WithCancel(context.Background())
	c.Started = func() {
		defer cancel()
		log.Println("[INFO] Olric is ready to accept connections")
	}
	var err error
	db, err = olric.New(c)
	if err != nil {
		log.Fatalf("Failed to create Olric instance: %v", err)
	}

	go func() {
		// Call Start at background. It's a blocker call.
		err = db.Start()
		if err != nil {
			log.Fatalf("olric.Start returned an error: %v", err)
		}
	}()
	<-ctx.Done()

	CacheDM, err = db.NewDMap("bucket-of-arbitrary-items")
	if err != nil {
		log.Fatalf("olric.NewDMap returned an error: %v", err)
	}
}

func ShutdownCache() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	err := db.Shutdown(ctx)
	if err != nil {
		log.Printf("Failed to shutdown Olric: %v", err)
	}
	cancel()
}