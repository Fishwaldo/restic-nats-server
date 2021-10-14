package internal

import (
	"github.com/Fishwaldo/go-logadapter"
	"github.com/Fishwaldo/go-logadapter/loggers/logrus"
	"github.com/spf13/viper"
)

var Log logadapter.Logger

func init() {
	Log = logrus.LogrusDefaultLogger()
	viper.SetDefault("loglevel", logadapter.LOG_DEBUG)
}

func StartLogger() {
	Log.SetLevel(logadapter.Log_Level(viper.GetInt("loglevel")))
	Log.Info("Starting Logging")
}