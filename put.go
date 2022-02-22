package ibmmqtx

import (
	"encoding/hex"
	"github.com/ibm-messaging/mq-golang/v5/ibmmq"
	"time"
)

func (p *MQPro) PutMessage(msg *Msg) (MsgID, Tx, error) {
	l := p.log.WithField("method", "PutMessage")
RECON:
	l.Trace("Writing Message to Queue")

	if p.conn == nil || !p.conn.IsConnected() {
		l.Trace("no connection")
		if p.cfg.AutoReconnect {
			time.Sleep(p.cfg.ReconnectDelay)
			goto RECON
		}

		return nil, nil, ErrNoConnection
	}
	id, tx, err := p.conn.put(msg)
	if err == ErrConnBroken {
		p.conn.reqError()
		if p.cfg.AutoReconnect {
			time.Sleep(p.cfg.ReconnectDelay)
			goto RECON
		}
	}
	return id, tx, err
}

func (c *connection) put(msg *Msg) (MsgID, Tx, error) {
	// The PUT requires control structures, the Message Descriptor (MQMD)
	// and Put Options (MQPMO). Create those with default values.
	putmqmd := ibmmq.NewMQMD()
	pmo := ibmmq.NewMQPMO()
	cmho := ibmmq.NewMQCMHO()

	// set transaction flag in options.
	pmo.Options = ibmmq.MQPMO_SYNCPOINT | ibmmq.MQPMO_NEW_MSG_ID
	// Tell MQ what the message body format is. In this case, a text string
	putmqmd.Format = ibmmq.MQFMT_STRING

	putMsgHandle, err := c.mqQueryManager.CrtMH(cmho)
	if err != nil {
		c.log.Errorf("Ошибка создания объекта свойств сообщения: %s", err)
		if IsConnBroken(err) {
			err = ErrConnBroken
		} else {
			err = ErrPutMsg
		}
		return nil, nil, err
	}

	err = setProps(&putMsgHandle, msg.Props, c.log)
	if err != nil {
		return nil, nil, ErrPutMsg
	}

	pmo.OriginalMsgHandle = putMsgHandle

	if msg.CorrelId != nil {
		putmqmd.CorrelId = msg.CorrelId
	} else {
		pmo.Options |= ibmmq.MQPMO_NEW_CORREL_ID
	}

	var d []byte
	if msg.Payload == nil {
		d = make([]byte, 0)
	} else {
		d = msg.Payload
	}

	c.mutex.RLock()
	defer c.mutex.RUnlock()

	mqObjectPut, err := OpenQueue(c.env, c.mqQueryManager, Put)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		c.isWarn(mqObjectPut.Close(0))
	}()

	err = mqObjectPut.Put(putmqmd, pmo, d)
	if err != nil {
		c.log.Errorf("Ошибка отправки сообщения: %v", err)
		if IsConnBroken(err) {
			err = ErrConnBroken
		} else {
			err = ErrPutMsg
		}
		return nil, nil, err
	}
	c.log.Debugf("Success. MsgId: %x, CorrelId: %x", putmqmd.MsgId, putmqmd.CorrelId)

	tx := createTx(
		c.log.WithField("msgId", hex.EncodeToString(putmqmd.MsgId)),
		c.mqQueryManager)
	return putmqmd.MsgId, tx, nil
}
