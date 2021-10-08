// +build nocacheserver

package cache

import (
	"github.com/Fishwaldo/go-logadapter"

	"github.com/Fishwaldo/restic-nats-server/internal"
)

var log logadapter.Logger

func Start() {
	log = internal.Log.New("olric")
	log.Info("No Embedded Cache Server")
}

var CacheDM *interface{}


func Shutdown() {

}