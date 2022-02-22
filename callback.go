package ibmmqtx

import (
	"encoding/hex"
	"github.com/ibm-messaging/mq-golang/v5/ibmmq"
)

type RegisterEventHandler func(msg *MsgTx) /*error*/

type callbackOption struct {
	// обработчик
	callbackMessages RegisterEventHandler

	// объект подписки ibmmq
	mqCtlo *ibmmq.MQCTLO

	qObject *ibmmq.MQObject
	cbd     *ibmmq.MQCBD
	getmqmd *ibmmq.MQMD
	gmo     *ibmmq.MQGMO

	deRegisterEven bool
}

// RegisterEvenInMsg Добавляет обработчик входящих сообщений
func (p *MQPro) RegisterEvenInMsg(fn RegisterEventHandler) error {
	if p.conn.callbackOption != nil && !p.conn.callbackOption.deRegisterEven {
		p.log.Panic("Subscription already exists")
	}
	return p.conn.RegisterEventInMsg(fn)
}

// RegisterEventInMsg подписка на входящие сообщения
func (c *connection) RegisterEventInMsg(fn RegisterEventHandler) error {
	c.callbackOption = &callbackOption{
		callbackMessages: fn,
	}

	//if c.IsConnected() {
	if c.registerEventInMsg() != nil {
		c.reqError()
	}

	return nil
}

func (c *connection) registerEventInMsg() error {
	c.log.Trace("Subscribing to incoming messages...")
	err := c._registerEventInMsg()
	if err != nil {
		c.log.Error("Failed to subscribe to incoming messages")
		return err
	}
	c.log.Info("Subscribed to incoming messages")
	return nil
}

// подписка на входящие сообщения
func (c *connection) _registerEventInMsg() error {
	cmho := ibmmq.NewMQCMHO()
	mh, err := c.mqQueryManager.CrtMH(cmho)
	if err != nil {
		c.log.Error("Ошибка создания объекта свойств сообщения", err)
		return err
	}

	cbd := ibmmq.NewMQCBD()
	gmo := ibmmq.NewMQGMO()
	getmqmd := ibmmq.NewMQMD()

	qObject, err := OpenGetQueue(c.env, c.mqQueryManager, Get)
	if err != nil {
		return err
	}
	defer func() {
		// отключаем обьект если гдето есть
		if err != nil {
			c.isWarn(qObject.Close(0))
		}
	}()

	gmo.Options = ibmmq.MQGMO_SYNCPOINT | ibmmq.MQGMO_PROPERTIES_IN_HANDLE
	gmo.MsgHandle = mh
	cbd.CallbackFunction = c.handlerInMsg

	// Wait.
	//gmo.Options |= ibmmq.MQGMO_WAIT
	//gmo.WaitInterval = 10 // The WaitInterval is in milliseconds

	err = qObject.CB(ibmmq.MQOP_REGISTER, cbd, getmqmd, gmo)
	if err != nil {
		return err
	}

	ctlo := ibmmq.NewMQCTLO()
	err = c.mqQueryManager.Ctl(ibmmq.MQOP_START, ctlo)
	if err != nil {
		return err
	}
	c.callbackOption.mqCtlo = ctlo

	// записываем опции чтобы потом можно было отписаться
	c.callbackOption.qObject = &qObject
	c.callbackOption.cbd = cbd
	c.callbackOption.getmqmd = getmqmd
	c.callbackOption.gmo = gmo
	c.callbackOption.deRegisterEven = false

	return nil
}

func (p *MQPro) UnregisterInMsg() {
	p.conn.UnregisterInMsg()
}

func (c *connection) UnregisterInMsg() {
	c.log.Trace("Unsubscribing from incoming messages...")
	c.isWarn(c.deReg())
	c._unregisterInMsg()
	c.log.Info("unsubscribed from incoming messages")

	if c.callbackOption != nil {
		c.callbackOption.deRegisterEven = true
	}
}

func (c *connection) _unregisterInMsg() {
	if c.callbackOption != nil && c.callbackOption.deRegisterEven == false {
		c.mutex.Lock()
		c.isWarn(c.callbackOption.qObject.Close(0))
		c.isWarn(c.mqQueryManager.Ctl(ibmmq.MQOP_STOP, c.callbackOption.mqCtlo))
		//c.mqCtlo = nil
		//c.callbackOption = nil
		c.mutex.Unlock()
	}
}

// Deregister the callback function - have to do this before the message handle can be
// successfully deleted
func (c *connection) deReg() error {
	if c.callbackOption == nil || c.callbackOption.deRegisterEven {
		return nil
	}
	c.log.Traceln("run MQOP_DEREGISTER")
	return c.callbackOption.qObject.CB(
		ibmmq.MQOP_DEREGISTER,
		c.callbackOption.cbd,
		c.callbackOption.getmqmd,
		c.callbackOption.gmo,
	)
}

// внутренний обработчик от mq сообщений
func (c *connection) handlerInMsg(
	mqManger *ibmmq.MQQueueManager,
	_ *ibmmq.MQObject,
	md *ibmmq.MQMD,
	gmo *ibmmq.MQGMO,
	buffer []byte,
	_ *ibmmq.MQCBC,
	err *ibmmq.MQReturn,
) {

	if err.MQRC == ibmmq.MQRC_NO_MSG_AVAILABLE {
		return
	}

	if err.MQRC == ibmmq.MQRC_CONNECTION_BROKEN {
		c.log.Warnf("Ошибка подключения: %v", err)
		c.reqError()
		c.isWarn(mqManger.Back())
		return
	}

	if err.MQCC != ibmmq.MQCC_OK {
		c.log.Warnf("Subscription error: %v", err)
		c.isWarn(mqManger.Back())
		return
	}

	props, err1 := properties(gmo.MsgHandle)
	if err1 != nil {
		c.log.Error("Ошибка при получении свойств сообщения: ", err)
		c.isWarn(mqManger.Back())
		return
	}

	msg := &Msg{
		MsgId:    md.MsgId,
		CorrelId: md.CorrelId,
		Payload:  buffer,
		Props:    props,
	}

	c.log.Tracef("Получено сообщение %x", md.MsgId)

	tx := createTx(c.log.WithField("msgId", hex.EncodeToString(md.MsgId)), mqManger)
	// note: вызов callback вне горутины, иначе будет ошибка
	c.callbackOption.callbackMessages(&MsgTx{Msg: msg, Tx: tx})
}
