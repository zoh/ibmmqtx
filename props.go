package ibmmqtx

import (
	"github.com/ibm-messaging/mq-golang/v5/ibmmq"
	"github.com/sirupsen/logrus"
)

func setProps(h *ibmmq.MQMessageHandle, props map[string]interface{}, l *logrus.Entry) error {
	if props == nil {
		return nil
	}

	var err error
	smpo := ibmmq.NewMQSMPO()
	pd := ibmmq.NewMQPD()

	for k, v := range props {
		if v == nil {
			continue
		}
		err = h.SetMP(smpo, k, pd, v)
		if err != nil {
			l.Errorf("Failed to set message property '%s' value '%v': %v", k, v, err)
			return err
		}
	}

	return nil
}
