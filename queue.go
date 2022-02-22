package ibmmqtx

import (
	"github.com/ibm-messaging/mq-golang/v5/ibmmq"
	"github.com/sirupsen/logrus"
)

const (
	Put      = "PUT"
	Get      = "GET"
	Browse   = "Browse"
	Dynamic  = "Dynamic"
	Response = "Response"
)

// OpenDynamicQueue Opens a Dynamic Queue as part of a response in a request / response pattern
func OpenDynamicQueue(env *Env, qMgrObject *ibmmq.MQQueueManager, queueName string) (ibmmq.MQObject, error) {
	return openQueue(env, qMgrObject, queueName, Response)
}

// OpenQueue the queue. No queueName is provided.
func OpenQueue(env *Env, qMgrObject *ibmmq.MQQueueManager, msgStyle string) (ibmmq.MQObject, error) {
	return openQueue(env, qMgrObject, "", msgStyle)
}

// OpenGetQueue Opens the queue. No queueName is provided.
func OpenGetQueue(env *Env, qMgrObject *ibmmq.MQQueueManager, msgStyle string) (ibmmq.MQObject, error) {
	return openQueue(env, qMgrObject, "", msgStyle)
}

func openPutQueueByName(qNamePut string, qMgrObject *ibmmq.MQQueueManager) (qObject ibmmq.MQObject, err error) {
	mqod := ibmmq.NewMQOD()
	openOptions := ibmmq.MQOO_OUTPUT
	mqod.ObjectType = ibmmq.MQOT_Q
	mqod.ObjectName = qNamePut

	qObject, err = qMgrObject.Open(mqod, openOptions)
	if err != nil {
		return
	}
	return qObject, err
}

func openGetQueueByName(qName string, qMgrObject *ibmmq.MQQueueManager) (qObject ibmmq.MQObject, err error) {
	mqod := ibmmq.NewMQOD()
	openOptions := ibmmq.MQOO_INPUT_SHARED
	mqod.ObjectType = ibmmq.MQOT_Q
	mqod.ObjectName = qName

	qObject, err = qMgrObject.Open(mqod, openOptions)
	if err != nil {
		return
	}
	return qObject, err
}

func openQueue(env *Env, qMgrObject *ibmmq.MQQueueManager, replyToQ string, msgStyle string) (qObject ibmmq.MQObject, err error) {
	mqod := ibmmq.NewMQOD()
	openOptions := ibmmq.MQOO_OUTPUT

	switch msgStyle {
	case Put:
		mqod.ObjectType = ibmmq.MQOT_Q
		mqod.ObjectName = env.PutQueueName
	case Get:
		openOptions = ibmmq.MQOO_INPUT_SHARED
		mqod.ObjectType = ibmmq.MQOT_Q
		mqod.ObjectName = env.GetQueueName
	case Browse:
		openOptions = ibmmq.MQOO_BROWSE
		mqod.ObjectType = ibmmq.MQOT_Q
		mqod.ObjectName = env.BrowseQueueName

	case Dynamic:
		openOptions = ibmmq.MQOO_INPUT_SHARED
		mqod.ObjectName = env.GetQueueName
		// обновляем префикс если задан
		if env.DynamicQueueName != "" {
			logrus.Infoln("env.DynamicQueueName=", env.DynamicQueueName)
			mqod.DynamicQName = env.DynamicQueueName
		}
	case Response:
		mqod.ObjectType = ibmmq.MQOT_Q
		mqod.ObjectName = replyToQ
	}

	qObject, err = qMgrObject.Open(mqod, openOptions)
	if err != nil {
		return
	}
	return qObject, err
}
