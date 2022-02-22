#
NETWORK := "mq-test-network"

## ibmmq
.PHONY: ibmmq
ibmmq:
	@docker run -it --rm \
		--name pkg-mqpro-sample-ibmmq \
		--network $(NETWORK) \
		--hostname mq0 \
		--env LICENSE=accept \
		--env MQ_QMGR_NAME=QM1 \
		--env MQ_ADMIN_PASSWORD=22 \
		--env MQ_APP_PASSWORD=11 \
		--env MQ_ENABLE_METRICS=true \
		--env MQ_ENABLE_EMBEDDED_WEB_SERVER=true \
		--publish 9443:9443 \
		--publish 1414:1414 \
	ibmcom/mq:9.2.2.0-r1


## create-network
.PHONY: create-network
create-network:
	@docker network create --driver overlay --attachable $(NETWORK)


.PHONY: run
run:
	echo "run file $(file)"
	MQ_USER=app \
	MQ_PASS=11 \
	MQ_MANAGER=QM1 \
	MQ_HOST=localhost \
	MQ_PORT=1414 \
	MQ_CHANNEL=DEV.APP.SVRCONN \
	MQ_GET_Q=DEV.QUEUE.2 \
	MQ_BROWSE_Q=DEV.QUEUE.1 \
	MQ_PUT_Q=DEV.QUEUE.1 \
	go run $(file)
