package main

import (
	"context"
	"encoding/hex"
	"os"
	"os/signal"
	"syscall"
)

func init() {
	correlId, _ = hex.DecodeString("414d5120514d3120202020202020202005b3b06029480999")
}

func main() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-signals
		cancel()
	}()

	// send req
	done := workReq(ctx)

	<-done
}
