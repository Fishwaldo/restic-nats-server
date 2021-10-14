// +build !nonatsserver

package natsserver

import (
	"fmt"
	"net/url"
	"os"

	"github.com/Fishwaldo/go-logadapter"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/pkg/errors"
	"github.com/spf13/viper"

	"github.com/Fishwaldo/restic-nats-server/internal"
)

var log logadapter.Logger

type userInfo struct {
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
}

type natsConfigT struct {
	Hosts   []userInfo
	Workers []userInfo
}

var natsConfig natsConfigT

var internalWorkerCred userInfo

func init() {
	log = internal.Log.New("natsserver")
	viper.SetDefault("start-nats-server", true)
	internal.ConfigRegister("nats", parseConfig, validateConfig)
}

func parseConfig(cfg *viper.Viper) error {
	/* load Worker Accounts */
	err := cfg.UnmarshalKey("workers", &natsConfig.Workers)
	if err != nil {
		return errors.Wrap(err, "nats parseConfig")
	}
	log.Info("Worker Accounts %+v\n", natsConfig.Workers)
	err = cfg.UnmarshalKey("hosts", &natsConfig.Hosts)
	if err != nil {
		return errors.Wrap(err, "nats parseConfig")
	}
	log.Info("Host Accounts %+v\n", natsConfig.Hosts)
	return nil
}

func validateConfig() (warnings []error, err error) {
	var warn []error
	/* create a Internal Worker User */
	internalWorkerCred.Username = internal.RandString(8)
	internalWorkerCred.Password = internal.RandString(8)
	natsConfig.Workers = append(natsConfig.Workers, internalWorkerCred)

	if len(natsConfig.Hosts) == 0 {
		warn = append(warn, errors.New("No Host Authentication Configured. Allowing Anonymous Connections"))
		tmp := userInfo{Username: "anon"}
		natsConfig.Hosts = append(natsConfig.Hosts, tmp)
	}

	return warn, nil
}

func GetInternalWorkerURL() (path *url.URL, err error) {
	if internalWorkerCred.Username == "" || internalWorkerCred.Password == "" {
		return nil, errors.New("Internal User Credentials are empty?")
	}
	return url.Parse(fmt.Sprintf("nats://%s:%s@localhost:%d/", internalWorkerCred.Username, internalWorkerCred.Password, 4222))
}

func Start() {

	if !viper.GetBool("start-nats-server") {
		log.Info("Not Starting Embedded Nats Server")
		return
	}

	natslog := natsLogger{log: log}
	log.Info("Starting Embedded Nats Server")
	host, err := os.Hostname()
	if err != nil {
		host = "Undefined"
	}
	cluster := server.ClusterOpts{
		Name: "RNS-Worker-Cluster",
	}

	var hostaccounts []*server.Account
	var workeraccounts []*server.Account
	var users []*server.User

	hostacc := server.NewAccount("Hosts")
	hostaccounts = append(hostaccounts, hostacc)
	for _, worker := range natsConfig.Hosts {
		users = append(users, &server.User{
			Username: worker.Username,
			Password: worker.Password,
			Account:  hostacc,
		})
	}

	workeracc := server.NewAccount("Worker")
	workeraccounts = append(workeraccounts, workeracc)
	for _, worker := range natsConfig.Workers {
		users = append(users, &server.User{
			Username: worker.Username,
			Password: worker.Password,
			Account:  workeracc,
		})
	}

	for _, worker := range workeraccounts {
		for _, host := range hostaccounts {
			stream := fmt.Sprintf("repo.%s.>", host.Name)
			var exportedhosts []*server.Account
			exportedhosts = append(exportedhosts, host)
			if err := worker.AddServiceExportWithResponse(stream, server.Singleton, exportedhosts); err != nil {
				log.Warn("Can't Export Repo Stream For Host Account %s: %s", host.Name, err)
			}
			if err := host.AddServiceImport(worker, "repo.>", stream); err != nil {
				log.Warn("Can't Import Repo Stream %s from Worker Account %s: %s", stream, worker.Name, err)
			}
			log.Info("Exported Stream %s to %s", stream, host.Name)
			stream = fmt.Sprintf("chunk.%s.recieve.>", host.Name)
			if err := worker.AddServiceExportWithResponse(stream, server.Singleton, exportedhosts); err != nil {
				log.Warn("Worker: Can't Export Chunk Stream For Host Account %s: %s", host.Name, err)
			}
			if err := host.AddServiceImport(worker, "chunk.send.>", stream); err != nil {
				log.Warn("Host: Can't Import Chunk Stream %s from Worker Account %s: %s", stream, worker.Name, err)
			}
			stream = fmt.Sprintf("chunk.%s.send.>", host.Name)
			if err := host.AddServiceExportWithResponse("chunk.recieve.>", server.Singleton, workeraccounts); err != nil {
				log.Warn("Host: Can't Export Chunk Stream for Worker Account %s: %s", worker.Name, err)
			}
			if err := worker.AddServiceImport(host, stream, "chunk.recieve.>"); err != nil {
				log.Warn("Worker: Can't Import Chunk Stream %s from Host Account %s: %s", stream, host.Name, err)
			}
			log.Info("Exported Stream %s to %s", stream, host.Name)
		}
	}
	// for _, host := range hostaccounts {
	// 	stream := fmt.Sprintf("repo.%s.>", host.Name)
	// }

	opts := &server.Options{
		Debug:      false,
		NoSigs:     true,
		MaxPayload: 8388608,
		ServerName: host,
		Cluster:    cluster,
		Accounts:   append(hostaccounts, workeraccounts...),
		Users:      users,
		HTTPPort:   8081,
	}
	s, err := server.NewServer(opts)
	s.SetLoggerV2(natslog, false, false, false)
	if err != nil {
		log.Error("%w", err)
	}

	// Start things up. Block here until done.
	if err := server.Run(s); err != nil {
		log.Error("%w", err)
	}
}

func Shutdown() {
	log.Info("Shuting Down Nats Server")
}

type natsLogger struct {
	log logadapter.Logger
}

// Log a notice statement
func (l natsLogger) Noticef(format string, v ...interface{}) {
	l.log.Info(format, v...)
}

// Log a warning statement
func (l natsLogger) Warnf(format string, v ...interface{}) {
	l.log.Warn(format, v...)
}

// Log a fatal error
func (l natsLogger) Fatalf(format string, v ...interface{}) {
	l.log.Fatal(format, v...)
}

// Log an error
func (l natsLogger) Errorf(format string, v ...interface{}) {
	l.log.Error(format, v...)
}

// Log a debug statement
func (l natsLogger) Debugf(format string, v ...interface{}) {
	l.log.Debug(format, v...)
}

// Log a trace statement
func (l natsLogger) Tracef(format string, v ...interface{}) {
	l.log.Trace(format, v)
}
