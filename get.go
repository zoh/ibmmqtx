package ibmmqtx

import (
	"context"
	"encoding/hex"
	"github.com/ibm-messaging/mq-golang/v5/ibmmq"
	"time"
)

// GetMessage получение сообщения из очереди
func (p *MQPro) GetMessage(ctx context.Context) (*MsgTx, bool, error) {
	l := p.log.WithField("method", "GetMessage")
RECON:
	l.Trace("Getting Message to Queue")

	if p.conn == nil || !p.conn.IsConnected() {
		if p.cfg.AutoReconnect {
			time.Sleep(p.cfg.ReconnectDelay)
			goto RECON
		}
		return nil, false, ErrNoConnection
	}
	l.Trace("Getting Message from Queue")
	msg, ok, err := p.conn.get(ctx, operGet, nil)
	if err == ErrConnBroken {
		p.conn.reqError()

		if p.cfg.AutoReconnect {
			time.Sleep(p.cfg.ReconnectDelay)
			goto RECON
		}
	}
	return msg, ok, err
}

// GetMessageByID получение сообщения из очереди по заданному ID
func (p *MQPro) GetMessageByID(ctx context.Context, id []byte) (*MsgTx, bool, error) {
	l := p.log.WithField("method", "GetMessageByID")
RECON:
	l.Trace("Getting Message to Queue by ID")

	if p.conn == nil || !p.conn.IsConnected() {
		if p.cfg.AutoReconnect {
			time.Sleep(p.cfg.ReconnectDelay)
			goto RECON
		}
		return nil, false, ErrNoConnection
	}
	l.Trace("Getting Message from Queue")
	msg, ok, err := p.conn.get(ctx, operGetByMsgId, id)
	if err == ErrConnBroken {
		p.conn.reqError()
		if p.cfg.AutoReconnect {
			time.Sleep(p.cfg.ReconnectDelay)
			goto RECON
		}
	}
	return msg, ok, err
}

func (p *MQPro) GetMessageByCorrelID(ctx context.Context, correlId []byte) (*MsgTx, bool, error) {
	l := p.log.WithField("method", "GetMessageByID")
RECON:
	l.Tracef("Getting Message to Queue by ID %x", correlId)

	if p.conn == nil || !p.conn.IsConnected() {
		if p.cfg.AutoReconnect {
			time.Sleep(p.cfg.ReconnectDelay)
			goto RECON
		}
		return nil, false, ErrNoConnection
	}
	l.Trace("Getting Message from Queue")
	msg, ok, err := p.conn.get(ctx, operGetByCorrelId, correlId)
	if err == ErrConnBroken {
		p.conn.reqError()
		if p.cfg.AutoReconnect {
			time.Sleep(p.cfg.ReconnectDelay)
			goto RECON
		}
	}
	return msg, ok, err
}

func (c *connection) get(ctx context.Context, oper queueOper, id []byte) (*MsgTx, bool, error) {
	l := c.log.WithField("layer", "connection.get")
	if !c.IsConnected() {
		l.Error(ErrNoConnection)
		return nil, false, ErrNoConnection
	}

	l.Tracef("Start get request oper=%d", oper)

	var (
		datalen int
		err     error
		mqrc    *ibmmq.MQReturn
		buffer  = make([]byte, 0, 1024)
	)

	getmqmd := ibmmq.NewMQMD()
	gmo := ibmmq.NewMQGMO()
	cmho := ibmmq.NewMQCMHO()

	qObject, err := OpenGetQueue(c.env, c.mqQueryManager, Get)
	if err != nil {
		return nil, false, err
	}
	defer qObject.Close(0)

	c.mutex.Lock()
	// ? почему тут в лок
	getMsgHandle, err := c.mqQueryManager.CrtMH(cmho)
	c.mutex.Unlock()

	if err != nil {
		l.Errorf("Ошибка создания объекта свойств сообщения: %s", err)
		if IsConnBroken(err) {
			err = ErrConnBroken
		} else {
			err = ErrGetMsg
		}
		return nil, false, err
	}
	defer func() {
		err := dltMh(getMsgHandle)
		if err != nil {
			l.Warnf("Ошибка удаления объекта свойств сообщения: %s", err)
		}
	}()

	gmo.MsgHandle = getMsgHandle
	// запрос с транзакцией
	gmo.Options = ibmmq.MQGMO_SYNCPOINT | ibmmq.MQGMO_PROPERTIES_IN_HANDLE | ibmmq.MQGMO_WAIT
	getmqmd.Format = ibmmq.MQFMT_STRING
	gmo.WaitInterval = c.env.WaitGetMsgInterval // The WaitInterval is in milliseconds

	switch oper {
	case operGet:
	case operGetByMsgId:
		gmo.MatchOptions = ibmmq.MQMO_MATCH_MSG_ID
		getmqmd.MsgId = id
	case operGetByCorrelId:
		gmo.MatchOptions = ibmmq.MQMO_MATCH_CORREL_ID
		getmqmd.CorrelId = id
	case operBrowseFirst:
		gmo.Options |= ibmmq.MQGMO_BROWSE_FIRST
	case operBrowseNext:
		gmo.Options |= ibmmq.MQGMO_BROWSE_NEXT
	default:
		l.Panicf("Unknown operation. queueOper = %v", oper)
	}

loopCtx:
	for {
	loopGet:
		for i := 0; i < 2; i++ {
			buffer, datalen, err = qObject.GetSlice(getmqmd, gmo, buffer)
			if err == nil {
				break loopCtx
			}

			mqrc = err.(*ibmmq.MQReturn)
			l.Trace(mqrc.Error())
			switch mqrc.MQRC {
			case ibmmq.MQRC_TRUNCATED_MSG_FAILED:
				buffer = make([]byte, 0, datalen)
				continue
			case ibmmq.MQRC_NO_MSG_AVAILABLE:
				err = nil
				break loopGet
			}

			l.Error(err)

			if IsConnBroken(err) {
				err = ErrConnBroken
				c.reqError()
			} else {
				err = ErrGetMsg
			}

			return nil, false, err
		}
		select {
		case <-time.After(1):
		case <-ctx.Done():
			l.Debug("No message")
			return nil, false, nil
		}
	}

	props, err := properties(getMsgHandle)
	if err != nil {
		l.Errorf("Ошибка получения свойств сообщения: %s", err)
		c.isWarn(c.mqQueryManager.Back())
		return nil, false, ErrGetMsg
	}

	l.Trace("Success get message")

	ret := &Msg{
		Payload:  buffer,
		Props:    props,
		CorrelId: getmqmd.CorrelId,
		MsgId:    getmqmd.MsgId,
		Time:     getmqmd.PutDateTime,
		ReplyToQ: getmqmd.ReplyToQ,
	}
	tx := createTx(l.WithField("msgId", hex.EncodeToString(ret.MsgId)), c.mqQueryManager)
	return &MsgTx{Msg: ret, Tx: tx}, true, nil
}
