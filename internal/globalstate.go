package internal

import (
	"net/url"
	"sync"

	"github.com/nats-io/nats.go"
	"gopkg.in/tomb.v2"
)

type Config struct {
	Server      *url.URL
	Credential  string
	Connections uint
	Repo        string
}

type Backend struct {
	Cfg        Config
	Conn       *nats.Conn
	BuCommands *nats.Subscription
	Mx         sync.Mutex
	T          tomb.Tomb
	Enc        nats.Encoder
}
