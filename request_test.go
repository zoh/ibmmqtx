package ibmmqtx_test

import (
	"context"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zoh/ibmmqtx"
	"os"
	"testing"
)

func TestMQPro_Request(t *testing.T) {
	t.Parallel()
	logrus.SetLevel(logrus.DebugLevel)

	mq1 := integrationMQClient(t)
	mq2 := integrationMQClient(t)

	go workResponse(t, mq2)

	msgTx, err := mq1.Request(context.Background(), &ibmmqtx.Msg{
		CorrelId: ibmmqtx.EmptyCorrelId,
		Payload:  []byte("Test"),
	},
		os.Getenv("MQ_PUT_Q"),
		os.Getenv("MQ_GET_Q"),
	)
	require.NoError(t, err)
	require.NoError(t, msgTx.Commit())

	assert.Equal(t, msgTx.Payload, []byte("Response: Test"))
}

func workResponse(t *testing.T, mq *ibmmqtx.MQPro) {
	// wait msg and make response
	msg, ok, err := mq.GetMessage(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Error("no message")
	}

	// put response
	id, tx, err := mq.PutMessage(&ibmmqtx.Msg{
		CorrelId: msg.MsgId, // put msgID from request to correlId to
		Payload:  append([]byte("Response: "), msg.Payload...),
	})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	assert.NotEmpty(t, id)
}
