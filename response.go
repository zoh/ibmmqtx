package ibmmqtx

import (
	"bytes"
	"encoding/hex"
	"errors"
	"github.com/ibm-messaging/mq-golang/v5/ibmmq"
	"os"
	"strings"
)

// ReplyToMsg отправляем ответ на данное сообщение
func (p *MQPro) ReplyToMsg(msg *Msg) (Tx, error) {
	logger := p.log.WithField("method", "ReplyToMsg")

	if msg.ReplyToQ == "" {
		return nil, errors.New("empty ReplyToQ into message")
	}

	qObject, err := OpenDynamicQueue(&p.cfg.Env, p.conn.mqQueryManager, msg.ReplyToQ)
	if err != nil {
		logger.Fatalln("Unable to Open Queue")
		os.Exit(1)
	}
	defer qObject.Close(0)

	putmqmd := ibmmq.NewMQMD()
	pmo := ibmmq.NewMQPMO()
	cmho := ibmmq.NewMQCMHO()

	//putmqmd.MsgType = ibmmq.MQMT_REPLY

	putMsgHandle, err := p.conn.mqQueryManager.CrtMH(cmho)
	if err != nil {
		logger.Errorf("Ошибка создания объекта свойств сообщения: %s", err)
		if IsConnBroken(err) {
			err = ErrConnBroken
		} else {
			err = ErrPutMsg
		}
		return nil, err
	}

	err = setProps(&putMsgHandle, msg.Props, logger)
	if err != nil {
		return nil, ErrPutMsg
	}
	pmo.OriginalMsgHandle = putMsgHandle

	if msg.CorrelId != nil {
		putmqmd.CorrelId = msg.CorrelId
	}

	emptyByteArray := make([]byte, 24)
	if bytes.Equal(msg.CorrelId, emptyByteArray) || bytes.Contains(msg.CorrelId, emptyByteArray) {
		logger.Traceln("CorrelId is empty")
		putmqmd.CorrelId = msg.MsgId
	} else {
		logger.Traceln("Correl ID found on request")
		putmqmd.CorrelId = msg.CorrelId
	}
	putmqmd.MsgId = msg.MsgId

	// Tell MQ what the message body format is.
	// In this case, a text string
	putmqmd.Format = ibmmq.MQFMT_STRING
	pmo.Options = ibmmq.MQPMO_SYNCPOINT

	logger.Traceln("Looking for match on Correl ID CorrelID:" + hex.EncodeToString(putmqmd.CorrelId))

	var d []byte
	if msg.Payload == nil {
		d = make([]byte, 0)
	} else {
		d = msg.Payload
	}

	err = qObject.Put(putmqmd, pmo, d)
	if err != nil {
		logger.Warnf("Error ReplyToMsg: %v", err)
		p.conn.isWarn(p.conn.mqQueryManager.Back())
		return nil, err
	} else {
		// thought out commit into "Tx"
		logger.Traceln("Put message to", strings.TrimSpace(qObject.Name))
		logger.Traceln("MsgId:" + hex.EncodeToString(putmqmd.MsgId))
	}
	return createTx(logger.WithField("msgId", hex.EncodeToString(putmqmd.MsgId)), p.conn.mqQueryManager), nil
}
