package main

import (
	"context"
	"encoding/hex"
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

	correlId, _ := hex.DecodeString("414d5120514d3120202020202020202005b3b06029480440")

	for i := 0; i < 10; i++ {
		_, tx, err := mq.PutMessage(&ibmmqtx.Msg{
			Payload:  []byte("Test "),
			CorrelId: correlId,
		})
		if err != nil {
			log.Fatal(err)
		}
		_ = tx.Commit()
	}

	log.Println("end.")
}
