package ibmmqtx

import (
	"context"
	"fmt"
	"github.com/ibm-messaging/mq-golang/v5/ibmmq"
	"github.com/sirupsen/logrus"
	"sync"
	"time"
)

// MQPro ...
type MQPro struct {
	log     *logrus.Entry
	rootCtx context.Context
	cfg     *Config

	m    sync.Mutex
	conn *connection
}

type stateConn int

const (
	stateDisconnect stateConn = iota
	stateConnect
	stateErr
)

type queueOper int

const (
	operGet queueOper = iota
	operGetByMsgId
	operGetByCorrelId
	operBrowseFirst
	operBrowseNext
)

type connection struct {
	env       *Env
	log       *logrus.Entry
	ctx       context.Context
	ctxCancel context.CancelFunc

	// повторные подключения при разрыве соединения в данном сообщении
	autoReconnect bool

	reconDelay time.Duration

	// Менеджер очереди
	mqQueryManager *ibmmq.MQQueueManager
	cno            *ibmmq.MQCNO

	stateConn stateConn

	mutex sync.RWMutex

	// Подписка на входящие сообщения из очереди
	callbackOption *callbackOption

	// Пул обработчиков сообщений
	pool *Pool

	// Канал для сигналов о потери соединения
	signalDisconnect chan struct{}
}

// Dial подключение к очереди IBMMQ
func Dial(ctx context.Context, cfg *Config) (*MQPro, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	if cfg.ReconnectDelay == 0 {
		cfg.ReconnectDelay = DefaultReconnectDelay
	}

	if cfg.WaitGetMsgInterval == 0 {
		cfg.WaitGetMsgInterval = DefWaitGetMsgInterval
	}

	mqPro := &MQPro{
		rootCtx: ctx,
		cfg:     cfg,
	}

	if cfg.Logger != nil {
		mqPro.log = cfg.Logger
	} else {
		l := logrus.New()
		if cfg.Env.LogLevel > 0 {
			// default
			l.SetLevel(cfg.Env.LogLevel)
		} else {
			l.SetLevel(logrus.TraceLevel)
		}
		mqPro.log = logrus.NewEntry(l).WithField("pkg", "ibmmqtx")
	}

	if conn, err := mqPro.connect(ctx); err != nil {
		return nil, fmt.Errorf("error connection %w", err)
	} else {
		mqPro.conn = conn
	}

	go mqPro.conn.checkConnection()

	go func() {
		<-mqPro.conn.ctx.Done()
		mqPro.Disconnect()
	}()

	return mqPro, nil
}

// SetLogger установка логера
func (p *MQPro) SetLogger(log *logrus.Entry) {
	p.log = log

	if p.conn != nil {
		p.conn.log = log
	}
}

func (p *MQPro) connect(ctx context.Context) (*connection, error) {
	env_ := p.cfg.Env

	csp := ibmmq.NewMQCSP()
	cno := ibmmq.NewMQCNO()
	cd := ibmmq.NewMQCD()

	// Make the CNO refer to the CSP structure so it gets used during the connection
	cno.SecurityParms = csp

	conn := &connection{
		log:              p.log,
		env:              &p.cfg.Env,
		reconDelay:       p.cfg.ReconnectDelay,
		cno:              cno,
		signalDisconnect: make(chan struct{}),

		autoReconnect: p.cfg.AutoReconnect,
	}

	conn.ctx, conn.ctxCancel = context.WithCancel(ctx)
	conn.pool = NewPool(conn.ctx, p.cfg.Env.PoolSize)

	if username := env_.User; username != "" {
		p.log.Infof("User %s and pass has been specified\n", username)
		csp.AuthenticationType = ibmmq.MQCSP_AUTH_USER_ID_AND_PWD
		csp.UserId = username
		csp.Password = env_.Password
	} else {
		csp.AuthenticationType = ibmmq.MQCSP_AUTH_NONE
	}
	p.log.Debug("CCDT URL export is not set, will be using json envrionment client connections settings")
	// Fill in required fields in the MQCD channel definition structure
	cd.ChannelName = env_.Channel
	cd.ConnectionName = env_.getConnectionUrl()
	p.log.Debugf("Connecting to %s ", cd.ConnectionName)

	if env_.TLS && env_.KeyRepository != "" {
		p.log.Debug("Running in TLS Mode")
		sco := ibmmq.NewMQSCO()
		sco.KeyRepository = env_.KeyRepository
		cno.SSLConfig = sco
		cd.SSLCipherSpec = "ANY_TLS12"
		cd.SSLClientAuth = ibmmq.MQSCA_OPTIONAL
	}

	// Reference the CD structure from the CNO
	cno.ClientConn = cd

	if env_.KeyRepository != "" {
		p.log.Println("Key Repository has been specified")
		sco := ibmmq.NewMQSCO()
		sco.KeyRepository = env_.KeyRepository
		cno.SSLConfig = sco
	}

	// Indicate that we definitely want to use the client connection method.
	cno.Options = ibmmq.MQCNO_CLIENT_BINDING
	var attemp int
	for {
		select {
		case <-ctx.Done():
			return nil, nil
		default:
			attemp++
			// And now we can try to connect. Wait a short time before disconnecting.
			p.log.Infof("Attempting connection to %s %d", env_.QManager, attemp)
			if err := conn._connectManager(); err != nil {
				p.log.Warnf("connect error %v", err)
				continue
			}
			return conn, nil
		}
	}
}

func (c *connection) _connectManager() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.log.Traceln("request on connection")
	qMgr, err := ibmmq.Connx(c.env.QManager, c.cno)
	if err != nil {
		time.Sleep(c.reconDelay)
		return err
	}
	c.mqQueryManager = &qMgr
	c.stateConn = stateConnect
	c.log.Infof("MQ succeeded connect")

	//
	if c.callbackOption != nil && !c.callbackOption.deRegisterEven {
		c.isWarn(c.registerEventInMsg())
	}

	return nil
}

// получаем сообщение о потери сообщения и делаем реконект
func (c *connection) checkConnection() {
	for {
		select {
		case _, ok := <-c.signalDisconnect:
			c.log.Traceln("checkConnection: get signalDisconnect, channel=", ok)
			if !ok {
				// channel is closed
				return
			}
			if c.IsConnected() {
				continue
			}
			if err := c._connectManager(); err != nil {
				c.pool.Schedule(func() {
					c.signalDisconnect <- struct{}{}
				})
			}
		case <-c.ctx.Done():
			return
		}
	}
}
