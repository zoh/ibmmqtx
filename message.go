package ibmmqtx

import (
	"fmt"
	"time"
)

type MsgID []byte

// Msg отправляемое / получаемое сообщение
type Msg struct {
	MsgId    MsgID
	CorrelId []byte
	Payload  []byte
	Props    MsgProps

	// Time when message was putting
	Time time.Time

	// ReplyToQ - для отправки ответа в очередь
	ReplyToQ string
}

type MsgProps map[string]interface{}

func (msg *Msg) PrintMsg() {
	if msg == nil {
		return
	}
	fmt.Printf(">>>>> msg.Payload  = %s\n", string(msg.Payload))
	fmt.Printf(">>>>> msg.Props    = %+v\n", msg.Props)
	fmt.Printf(">>>>> msg.CorrelId = %x\n", msg.CorrelId)
	fmt.Printf(">>>>> msg.MsgId    = %x\n", msg.MsgId)

	if msg.ReplyToQ != "" {
		fmt.Printf(">>>>> msg.ReplyToQ = %s\n", msg.ReplyToQ)
	}

	if msg.Time.UnixNano() > 0 {
		fmt.Printf(">>>>> msg.Time = %v\n", msg.Time)
	}
}

type MsgTx struct {
	*Msg
	Tx
}
