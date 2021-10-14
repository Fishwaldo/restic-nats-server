package internal

import (
	"net/url"
	"sync"

	rns "github.com/Fishwaldo/restic-nats"
	"github.com/nats-io/nats.go"
	"gopkg.in/tomb.v2"
)


type WorkerConfigT struct {
	NumWorkers  int
	Connections uint
}

type NatsConfigT struct {
	MaxNatsConnections int
	NatsURL            *url.URL
	NatsNKey           string
	NatsCredfile       string
}

type GlobalStateT struct {
	WorkerConfig              WorkerConfigT
	NatsConfig               NatsConfigT
	Conn                      *rns.ResticNatsClient
	ClientCommand 				chan *nats.Msg
	ClientCommandSubscription *nats.Subscription
	Mx                        sync.Mutex
	T                         tomb.Tomb
}

var GlobalState GlobalStateT
