package ibmmqtx

import (
	"context"
	"errors"
	"github.com/sirupsen/logrus"
	"sync"
)

type MQProPool interface {

	// Get получить соединения из пула
	Get() (*MQPro, error)

	Put(*MQPro) error

	Size() struct {
		Available int
		InWork    int
	}

	// Flush сбросить лишние > min
	Flush() error

	// Disconnect закрыть все соединения
	Disconnect()
}

type mqPool struct {
	cfg      *Config
	min, max uint

	Logger *logrus.Entry

	m   sync.Mutex
	ctx context.Context

	// количество выпушенных
	inWork chan struct{}

	pool chan *MQPro
}

func CreatePool(ctx context.Context, cfg *Config, min, max uint) (MQProPool, error) {
	if min > max {
		return nil, errors.New("min > max")
	}

	if max == 0 {
		return nil, errors.New("pool.max не может быть нулевой")
	}

	m := &mqPool{
		ctx: ctx,
		cfg: cfg,
		min: min,
		max: max,

		inWork: make(chan struct{}, max),
		pool:   make(chan *MQPro, max),
	}

	if cfg.Logger != nil {
		m.Logger = cfg.Logger
	} else {
		l := logrus.New()
		l.SetLevel(cfg.LogLevel)

		m.Logger = l.WithField("layer", "mq-pool")
	}

	m.Logger.Debugf("создаём сразу %d коннектов", min)
	for i := 0; i < int(min); i++ {
		if c, err := m.makeDial(ctx); err != nil {
			return nil, err
		} else {
			m.pool <- c
		}
	}

	return m, nil
}

func (m *mqPool) makeDial(ctx context.Context) (*MQPro, error) {
	m.Logger.Traceln("dial up new mq connect")
	mq, err := Dial(ctx, m.cfg)
	if err != nil {
		return nil, err
	}
	return mq, nil
}

func (m *mqPool) Get() (*MQPro, error) {
	for {
		select {
		case <-m.ctx.Done():
			return nil, m.ctx.Err()
		// ожидаем пока осводобится
		case m.inWork <- struct{}{}:
			if m.Size().Available == 0 {
				m.Logger.Debugf("свободных нет, создаём новый")
				return m.makeDial(m.ctx)
			}
			m.Logger.Traceln("получаем свободный")
			return <-m.pool, nil
		}
	}
}

func (m *mqPool) Put(c *MQPro) error {
	select {
	case <-m.ctx.Done():
		return m.ctx.Err()
	case m.pool <- c:
		<-m.inWork
		m.Logger.Traceln("вернули в пул коннект")
	}
	return nil
}

func (m *mqPool) Flush() error {
	for {
		if m.Size().Available > int(m.min) {
			c := <-m.pool
			c.Disconnect()
		} else {
			return nil
		}
	}
}

func (m *mqPool) Size() struct {
	Available int
	InWork    int
} {
	return struct {
		Available int
		InWork    int
	}{
		Available: len(m.pool),
		InWork:    len(m.inWork),
	}
}

func (m *mqPool) Disconnect() {
	close(m.inWork)
	close(m.pool)

	for v := range m.pool {
		go v.Disconnect()
	}
}
