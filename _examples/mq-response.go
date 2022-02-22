package main

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	ibmmqtx "github.com/zoh/ibmmqtx"
	"github.com/zoh/ibmmqtx/_examples/config"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	cfg := config.GetMqConfig(logrus.TraceLevel)
	cfg.AutoReconnect = true

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM, os.Interrupt)
		<-sig
		cancel()
	}()
	fmt.Println("Ctrl+C for exit...")

	mq, err := ibmmqtx.Dial(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}

	go waitingRequest(ctx, mq)

	<-ctx.Done()
	log.Println("end.")
}

func waitingRequest(ctx_ context.Context, mq *ibmmqtx.MQPro) {
	for {
		// // Получаем сообщение get
		msg, ok, err := mq.GetMessage(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		if ok == false {
			log.Fatal("Сообщение так и не пришло")
		}
		msg.PrintMsg()
		if err := msg.Commit(); err != nil {
			log.Fatal(err)
		}

		msgResp := &ibmmqtx.Msg{
			CorrelId: msg.MsgId,
			MsgId:    msg.MsgId,
			Payload:  append([]byte("Payload Response message: "), msg.Payload...),
			ReplyToQ: msg.ReplyToQ,
		}

		msg.Payload = append([]byte("Payload Response message: "), msg.Payload...)
		tx, err := mq.ReplyToMsg(msgResp)
		if err != nil {
			log.Fatal(err)
		}

		_ = tx.Commit()

		select {
		case <-time.After(1):
		case <-ctx_.Done():
			return
		}
	}
}
