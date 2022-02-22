package config

import (
	"github.com/sirupsen/logrus"
	mqtx "github.com/zoh/ibmmqtx"
	"os"
	"time"
)

func GetMqConfig(level logrus.Level) *mqtx.Config {
	return &mqtx.Config{
		Env: mqtx.Env{
			User:               os.Getenv("MQ_USER"),
			Password:           os.Getenv("MQ_PASS"),
			QManager:           os.Getenv("MQ_MANAGER"),
			Host:               os.Getenv("MQ_HOST"),
			Port:               os.Getenv("MQ_PORT"),
			Channel:            os.Getenv("MQ_CHANNEL"),
			TLS:                false,
			LogLevel:           level,
			WaitGetMsgInterval: 100,

			GetQueueName:    os.Getenv("MQ_GET_Q"),
			PutQueueName:    os.Getenv("MQ_PUT_Q"),
			BrowseQueueName: os.Getenv("MQ_BROWSE_Q"),
		},
		AutoReconnect:  false,
		ReconnectDelay: time.Second * 2,
	}
}
