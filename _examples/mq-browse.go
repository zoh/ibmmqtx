package main

import (
	"context"
	"github.com/sirupsen/logrus"
	ibmmqtx "github.com/zoh/ibmmqtx"
	"github.com/zoh/ibmmqtx/_examples/config"
	"log"
)

func main() {
	cfg := config.GetMqConfig(logrus.TraceLevel)
	cfg.AutoReconnect = true

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mq, err := ibmmqtx.Dial(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}

	messages, err := mq.BrowseMessages(ctx)
	if err != nil {
		panic(err)
	}

	var count = 0
	for msg := range messages {
		msg.PrintMsg()
		count++
	}

	log.Printf("All messages: %d \n", count)
}
