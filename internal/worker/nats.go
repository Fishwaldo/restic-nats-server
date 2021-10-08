package worker

import (
	"fmt"
	"os"

	"github.com/Fishwaldo/restic-nats-server/internal"

	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
)

func connectNats(be *internal.Backend) error {
	be.Mx.Lock()
	defer be.Mx.Unlock()
	server := be.Cfg.Server.Hostname()
	port := be.Cfg.Server.Port()
	if port == "" {
		port = "4222"
	}
	url := fmt.Sprintf("nats://%s:%s", server, port)

	var options []nats.Option

	if len(be.Cfg.Credential) > 0 {
		/* Check Credential File Exists */
		_, err := os.Stat(be.Cfg.Credential)
		if err != nil {
			return errors.Wrap(err, "credential file missing")
		}
		options = append(options, nats.UserCredentials(be.Cfg.Credential))
	} else if len(be.Cfg.Server.User.Username()) > 0 {
		pass, _ := be.Cfg.Server.User.Password()
		options = append(options, nats.UserInfo(be.Cfg.Server.User.Username(), pass))
	}



	options = append(options, nats.ClosedHandler(natsClosedCB))
	options = append(options, nats.DisconnectHandler(natsDisconnectedCB))
	var err error
	be.Conn, err = nats.Connect(url, options...)
	if err != nil {
		return errors.Wrap(err, "nats connection failed")
	}
	if size := be.Conn.MaxPayload(); size < 8388608 {
		return errors.New("NATS Server Max Payload Size is below 8Mb")
	}

	internal.Log.Info("Connected to %s (%s)", be.Conn.ConnectedClusterName(), be.Conn.ConnectedServerName())

	internal.Log.Info("Nats Message Size: %d", be.Conn.MaxPayload())

	be.Enc = nats.EncoderForType("gob")

	be.BuCommands, err = be.Conn.QueueSubscribeSync("repo.Hosts.commands.*", "opencommandworkers")
	if err != nil {
		return errors.Wrap(err, "Can't Add Consumer")
	}

	return nil
}

func natsClosedCB(conn *nats.Conn) {
	internal.Log.Info("Connection Closed: %s", conn.LastError())
}

func natsDisconnectedCB(conn *nats.Conn) {
	internal.Log.Info("Connection Disconnected: %s", conn.LastError())
}
