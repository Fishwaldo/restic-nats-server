// +build nonatsserver

package natsserver

import (
	"github.com/Fishwaldo/go-logadapter"

	"github.com/Fishwaldo/restic-nats-server/internal"
)

var log logadapter.Logger

func Start() {
	log = internal.Log.New("natsserver")
	log.Info("No Nats Server")
}

func Shutdown() {
	
}