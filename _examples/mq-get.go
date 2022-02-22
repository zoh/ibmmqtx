package main

import (
	"context"
	"encoding/hex"
	"github.com/sirupsen/logrus"
	ibmmqtx "github.com/zoh/ibmmqtx"
	"github.com/zoh/ibmmqtx/_examples/config"
	"log"
	"os"
	"time"
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

	correlIdArg := os.Args[1]

	correlId, err := hex.DecodeString(correlIdArg)
	if err != nil {
		panic(err)
	}

	ctx, cancel = context.WithTimeout(ctx, time.Second*10)

	msg, ok, err := mq.GetMessageByCorrelID(ctx, correlId)
	if err != nil {
		log.Fatal(err)
	}

	if !ok {
		log.Fatal("not found messages.")
	}
	_ = msg.Commit()

	msg.PrintMsg()

	log.Println("end.")
}
