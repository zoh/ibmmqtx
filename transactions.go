package ibmmqtx

import (
	"github.com/ibm-messaging/mq-golang/v5/ibmmq"
	"github.com/sirupsen/logrus"
)

// Tx интерфейс подтверждения или отката транзакции
type Tx interface {
	// Commit подтвердить получение сообщения
	Commit() error

	// Rollback Отменить получение сообщения
	Rollback() error
}

type transaction struct {
	log      *logrus.Entry
	qManager *ibmmq.MQQueueManager
}

func (t *transaction) Commit() error {
	t.log.Trace("tx commit")

	if t.qManager != nil {
		return t.qManager.Cmit()
	}
	return nil
}

func (t *transaction) Rollback() error {
	t.log.Trace("tx rollback")
	return t.qManager.Back()
}

func createTx(log *logrus.Entry, qManager *ibmmq.MQQueueManager) Tx {
	return &transaction{log, qManager}
}
