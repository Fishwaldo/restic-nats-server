package client

import (
	"sync"

	"github.com/Fishwaldo/restic-nats"
	"github.com/Fishwaldo/restic-nats-server/internal"
	"github.com/pkg/errors"
)

var clientList sync.Map

func Create(or rns.OpenRepoOp) (rns.Client, error) {
	client := rns.Client{ClientID: internal.RandString(16), Bucket: or.Bucket}
	clientList.Store(client.ClientID, client)
	return client, nil
}

func Find(clientid string) (rns.Client, error) {
	client, found := clientList.Load(clientid)
	if !found {
		return rns.Client{}, errors.New("Client Not Found")
	}
	return client.(rns.Client), nil
}

func Remove(clientid string) (error) {
	_, found := clientList.Load(clientid)
	if found {
		clientList.Delete(clientid)
		return nil
	} else {
		return errors.New("Client Not Found")
	}
}