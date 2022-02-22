package ibmmqtx

import (
	"github.com/ibm-messaging/mq-golang/v5/ibmmq"
)

func (c *connection) isWarn(err error) {
	if err != nil {
		c.log.Warn(err)
	}
}

func (c *connection) isWarnConn(err error) {
	if err != nil {
		mqret := err.(*ibmmq.MQReturn)
		if mqret == nil || mqret.MQRC != ibmmq.MQRC_CONNECTION_BROKEN {
			c.log.Warn(err)
		}
	}
}

func IsConnBroken(err error) bool {
	mqrc := err.(*ibmmq.MQReturn).MQRC
	return mqrc == ibmmq.MQRC_CONNECTION_BROKEN || mqrc == ibmmq.MQRC_CONNECTION_QUIESCING
}

func (c *connection) IsConnected() bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	return c.stateConn == stateConnect
}

func (c *connection) IsDisconnected() bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	return c.stateConn == stateDisconnect
}

func dltMh(mh ibmmq.MQMessageHandle) error {
	dmho := ibmmq.NewMQDMHO()
	return mh.DltMH(dmho)
}

func properties(getMsgHandle ibmmq.MQMessageHandle) (map[string]interface{}, error) {
	impo := ibmmq.NewMQIMPO()
	pd := ibmmq.NewMQPD()
	props := make(map[string]interface{})

	impo.Options = ibmmq.MQIMPO_CONVERT_VALUE | ibmmq.MQIMPO_INQ_FIRST
	for {
		name, value, err := getMsgHandle.InqMP(impo, pd, "%")
		impo.Options = ibmmq.MQIMPO_CONVERT_VALUE | ibmmq.MQIMPO_INQ_NEXT
		if err != nil {
			mqret := err.(*ibmmq.MQReturn)
			if mqret.MQRC != ibmmq.MQRC_PROPERTY_NOT_AVAILABLE {
				return nil, err
			}
			break
		}
		props[name] = value
	}
	return props, nil
}
