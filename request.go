package ibmmqtx

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"github.com/ibm-messaging/mq-golang/v5/ibmmq"
	"strings"
	"time"
)

// Request ...
func (p *MQPro) Request(ctx context.Context, msg *Msg, queueNamePut, queueNameGet string) (*MsgTx, error) {
	logger := p.log.WithField("method", "Request")

	logger.Tracef("Request msg: MsgId=%x, CorrelId=%x", msg.MsgId, msg.CorrelId)

RECON:
	if p.conn == nil || !p.conn.IsConnected() {
		if p.cfg.AutoReconnect {
			time.Sleep(p.cfg.ReconnectDelay)
			goto RECON
		}
		return nil, ErrNoConnection
	}

	logger.Trace("open PutQueue byName ", queueNamePut)

	qObject, err := openPutQueueByName(queueNamePut, p.conn.mqQueryManager) //OpenQueue(&p.cfg.Env, p.conn.mqQueryManager, Put)
	if err != nil {
		logger.Trace("Unable to Open Queue ", queueNamePut)
		if IsConnBroken(err) { // todo: refactor this check error broken
			p.conn.reqError()
			if p.cfg.AutoReconnect {
				time.Sleep(p.cfg.ReconnectDelay)
				goto RECON
			}
		}
		return nil, err
	}
	defer qObject.Close(0)

	logger.Trace("open GetQueue byName ", queueNameGet)
	qObjDynamic, err := openGetQueueByName(queueNameGet, p.conn.mqQueryManager)
	if err != nil {
		logger.Trace("Unable to Open Dynamic Queue")
		if IsConnBroken(err) {
			p.conn.reqError()
			if p.cfg.AutoReconnect {
				time.Sleep(p.cfg.ReconnectDelay)
				goto RECON
			}
		}
		return nil, err
	}
	defer qObjDynamic.Close(0)

	// qObjDynamic тут только нужно Name которые обычно совпадает с put очередью
	msgID, corellationId, err := p.conn.putRequest(qObject, qObjDynamic, msg)
	if err != nil {
		if err == ErrConnBroken {
			p.conn.reqError()
			if p.cfg.AutoReconnect {
				time.Sleep(p.cfg.ReconnectDelay)
				goto RECON
			}
		}

		logger.Trace("Unable to send request")
		return nil, err
	}

	var resCmdID []byte
	if bytes.Equal(corellationId, EmptyCorrelId) {
		resCmdID = msgID
	} else {
		resCmdID = corellationId
	}

	logger.Debugf("find response by correlID=%x", resCmdID)
	msgTx, err := p.conn.awaitResponse(ctx, qObjDynamic, resCmdID)
	if err == ErrConnBroken {
		p.conn.reqError()
		if p.cfg.AutoReconnect {
			time.Sleep(p.cfg.ReconnectDelay)
			goto RECON
		}
	}

	return msgTx, err
}

// putRequest ...
func (c *connection) putRequest(qObject ibmmq.MQObject, qObjDynamic ibmmq.MQObject, msg *Msg) (msgId, corellationId []byte, err error) {
	logger := c.log.WithField("method", "putRequest")
	logger.Traceln("Writing Message to Queue")

	// The PUT requires control structures, the Message Descriptor (MQMD)
	// and Put Options (MQPMO). Create those with default values.
	putmqmd := ibmmq.NewMQMD()
	putmqmd.ReplyToQ = qObjDynamic.Name
	putmqmd.MsgType = ibmmq.MQMT_REQUEST
	putmqmd.CodedCharSetId = EncodingUTF8

	pmo := ibmmq.NewMQPMO()
	cmho := ibmmq.NewMQCMHO()

	pmo.Options = ibmmq.MQPMO_SYNCPOINT |
		ibmmq.MQPMO_NEW_MSG_ID

	// Tell MQ what the message body format is. In this case, a text string
	putmqmd.Format = ibmmq.MQFMT_STRING

	putMsgHandle, err := c.mqQueryManager.CrtMH(cmho)
	if err != nil {
		logger.Errorf("Ошибка создания объекта свойств сообщения: %s", err)
		if IsConnBroken(err) {
			err = ErrConnBroken
		} else {
			err = ErrPutMsg
		}
		return nil, nil, err
	}

	err = setProps(&putMsgHandle, msg.Props, logger)
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
	//
	logger.Traceln("Put message with payload:", strings.TrimSpace(string(d)))
	// Now put the message to the queue
	if err = qObject.Put(putmqmd, pmo, d); err != nil {
		c.isWarn(c.mqQueryManager.Back())
		if IsConnBroken(err) {
			err = ErrConnBroken
		}
		return nil, nil, err
	} else {
		logger.Traceln("Put message to", strings.TrimSpace(qObject.Name))
		logger.Traceln("MsgId:" + hex.EncodeToString(putmqmd.MsgId))
		logger.Traceln("CorrelID:" + hex.EncodeToString(putmqmd.CorrelId))
		corellationId = putmqmd.CorrelId
		msgId = putmqmd.MsgId
		c.isWarn(c.mqQueryManager.Cmit())
	}
	return
}

// awaitResponse ожидание ответа на запрос по corellationId
func (c *connection) awaitResponse(ctx context.Context, qDynamicObject ibmmq.MQObject, correlId []byte) (*MsgTx, error) {
	logger := c.log.WithField("method", "awaitResponse")
	logger.Info("Waiting for a response")
	var (
		err      error
		msgAvail = true
		cmho     = ibmmq.NewMQCMHO()
	)

	c.mutex.Lock()
	getMsgHandle, err := c.mqQueryManager.CrtMH(cmho)
	c.mutex.Unlock()
	if err != nil {
		logger.Errorf("Ошибка создания объекта свойств сообщения: %s", err)
		if IsConnBroken(err) {
			err = ErrConnBroken
		} else {
			err = ErrGetMsg
		}
		return nil, err
	}
	defer func() {
		err := dltMh(getMsgHandle)
		if err != nil {
			logger.Warnf("Ошибка удаления объекта свойств сообщения: %s", err)
		}
	}()

	// Create a buffer for the message data. This one is large enough
	// for the messages put by the amqsput sample.
	buffer := make([]byte, 1024)
	getmqmd := ibmmq.NewMQMD()
	getmqmd.CodedCharSetId = EncodingUTF8

	for msgAvail == true && err == nil {
		var datalen int

		// The PUT requires control structures, the Message Descriptor (MQMD)
		// and Put Options (MQPMO). Create those with default values.
		gmo := ibmmq.NewMQGMO()

		// The default options are OK, but it's always
		// a good idea to be explicit about transactional boundaries as
		// not all platforms behave the same way.
		gmo.Options = ibmmq.MQGMO_WAIT |
			ibmmq.MQGMO_FAIL_IF_QUIESCING |
			ibmmq.MQGMO_SYNCPOINT
		// Set options to wait for a maximum of 10 seconds for any new message to arrive
		gmo.MatchOptions = ibmmq.MQMO_MATCH_CORREL_ID

		gmo.MsgHandle = getMsgHandle
		gmo.Options |= ibmmq.MQGMO_PROPERTIES_IN_HANDLE

		getmqmd.CorrelId = correlId
		gmo.WaitInterval = c.env.WaitGetMsgInterval // The WaitInterval is in milliseconds
		logger.Traceln("Looking for match on Correl ID CorrelID:" + hex.EncodeToString(correlId))

		// Now try to get the message
		datalen, err = qDynamicObject.Get(getmqmd, gmo, buffer)
		if err != nil {
			msgAvail = false
			mqret := err.(*ibmmq.MQReturn)
			logger.Tracef("return code %d, expected %d,", mqret.MQRC, ibmmq.MQRC_NO_MSG_AVAILABLE)

			switch mqret.MQRC {
			case ibmmq.MQRC_TRUNCATED_MSG_FAILED:
				buffer = make([]byte, datalen)
				msgAvail = true
				err = nil

				logger.Tracef("msg dataLen=%d", datalen)
				continue
			case ibmmq.MQRC_NO_MSG_AVAILABLE:
				// If there's no message available, then don't treat that as a real error as
				// it's an expected situation
				msgAvail = true
				err = nil
			default:
				if IsConnBroken(err) {
					err = ErrConnBroken
				}
			}
		} else {
			// Assume the message is a printable string
			buffer = buffer[0:datalen]
			logger.Tracef("Got message of length %d: ", datalen)
			logger.Traceln(strings.TrimSpace(string(buffer)))
			msgAvail = false
		}

		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			logger.Debug("No message")
			return nil, errors.New("did not wait for the reply message")
		}
	}

	if err != nil {
		c.log.Traceln("Back")
		c.isWarn(c.mqQueryManager.Back())
		return nil, err
	}

	props, err := properties(getMsgHandle)
	if err != nil {
		logger.Errorf("Ошибка получения свойств сообщения: %s", err)
		c.isWarn(c.mqQueryManager.Back())

		if IsConnBroken(err) {
			err = ErrConnBroken
		}
		return nil, err
	}

	msg := &Msg{
		Payload:  buffer,
		Props:    props,
		CorrelId: getmqmd.CorrelId,
		MsgId:    getmqmd.MsgId,
		Time:     getmqmd.PutDateTime,
	}
	tx := createTx(logger.WithField("msgId", hex.EncodeToString(msg.MsgId)), c.mqQueryManager)
	return &MsgTx{Msg: msg, Tx: tx}, nil
}
