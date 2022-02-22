package ibmmqtx

import (
	"context"
	"github.com/ibm-messaging/mq-golang/v5/ibmmq"
	"time"
)

// BrowseMessages lookup all available messages from queue.
func (p *MQPro) BrowseMessages(ctx context.Context) (<-chan *Msg, error) {
	l := p.log.WithField("method", "BrowseMessages")
RECON:
	l.Trace("Browse Message from Queue")

	if p.conn == nil || !p.conn.IsConnected() {
		if p.cfg.AutoReconnect {
			time.Sleep(p.cfg.ReconnectDelay)
			goto RECON
		}
		return nil, ErrNoConnection
	}

	qObject, err := OpenGetQueue(&p.cfg.Env, p.conn.mqQueryManager, Browse)
	if err != nil {
		l.Trace("Unable to Open Queue ", qObject)
		if IsConnBroken(err) { // todo: refactor this check error broken
			p.conn.reqError()
			if p.cfg.AutoReconnect {
				time.Sleep(p.cfg.ReconnectDelay)
				goto RECON
			}
		}
		return nil, err
	}

	var (
		ch   = make(chan *Msg)
		wait = make(chan struct{})
		//err  error
		ok bool
	)

	go func(w chan struct{}) {
		var msg *Msg
		cx, cancel := context.WithCancel(ctx)
		cancel()
		oper := operBrowseFirst

		for ctx.Err() == nil {
			msg, ok, err = p.conn.browse(cx, qObject, oper)
			if err != nil || !ok {
				break
			}

			if w != nil {
				close(w)
				w = nil
			}
			ch <- msg
			oper = operBrowseNext
		}
		if w != nil {
			close(w)
		}
		close(ch)
		l.Info("Закрытие канала обзора сообщений BROWSE")
	}(wait)

	//msg, ok, err := p.conn.browse(ctx, operBrowseFirst)
	//if err == ErrConnBroken {
	//	p.conn.reqError()
	//
	//	if p.cfg.AutoReconnect {
	//		time.Sleep(p.cfg.ReconnectDelay)
	//		goto RECON
	//	}
	//}
	return ch, err
}

func (c *connection) browse(ctx context.Context, qObject ibmmq.MQObject, oper queueOper) (*Msg, bool, error) {
	l := c.log.WithField("layer", "connection.browse")
	if !c.IsConnected() {
		l.Error(ErrNoConnection)
		return nil, false, ErrNoConnection
	}

	var (
		datalen int
		err     error
		mqrc    *ibmmq.MQReturn
		buffer  = make([]byte, 0, 1024)
	)

	getmqmd := ibmmq.NewMQMD()
	gmo := ibmmq.NewMQGMO()
	cmho := ibmmq.NewMQCMHO()

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
	gmo.Options = ibmmq.MQGMO_NO_SYNCPOINT | ibmmq.MQGMO_PROPERTIES_IN_HANDLE
	getmqmd.Format = ibmmq.MQFMT_STRING

	switch oper {
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

	l.Info("Success")

	ret := &Msg{
		Payload:  buffer,
		Props:    props,
		CorrelId: getmqmd.CorrelId,
		MsgId:    getmqmd.MsgId,
		Time:     getmqmd.PutDateTime,
		ReplyToQ: getmqmd.ReplyToQ,
	}
	return ret, true, nil
}
