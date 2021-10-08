package rns

import (
	"github.com/Fishwaldo/restic-nats-server/internal"
	"github.com/Fishwaldo/restic-nats-server/internal/cache"
	"github.com/Fishwaldo/restic-nats-server/internal/natsserver"
)

func StartServies() {
	internal.StartLogger()
	natsserver.Start()
	cache.Start()
}
