package ibmmqtx_test

import (
	"context"
	"encoding/hex"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zoh/ibmmqtx"
	"github.com/zoh/ibmmqtx/_examples/config"
	"testing"
	"time"
)

func TestHex(t *testing.T) {
	correlId, _ := hex.DecodeString("000000000000000000000000000000000000000000000000")

	assert.Equal(t, len(ibmmqtx.EmptyCorrelId), 24)
	assert.Equal(t, correlId, ibmmqtx.EmptyCorrelId)
}

func integrationMQClient(t *testing.T) *ibmmqtx.MQPro {
	cfg := config.GetMqConfig(logrus.TraceLevel)

	ctx, _ := context.WithTimeout(context.Background(), 30*time.Second)

	c, err := ibmmqtx.Dial(ctx, cfg)
	if err != nil {
		t.Fatalf("MQ server is not available: %v \n use # source _examples/.env.testing for envirioment variables", err)
	}
	return c
}

func TestMQPro_PutMessage(t *testing.T) {
	mq := integrationMQClient(t)
	logrus.SetLevel(logrus.DebugLevel)

	logrus.Debugln("put message with nullable correlId")
	msgID, tx, err := mq.PutMessage(&ibmmqtx.Msg{
		Payload:  []byte("test"),
		CorrelId: ibmmqtx.EmptyCorrelId,
	})
	if err != nil {
		t.Fatal(err)
	}
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	assert.NotEmpty(t, msgID)

	msgTx, ok, err := mq.GetMessageByID(context.Background(), msgID)
	require.NoError(t, err)
	assert.True(t, ok)
	require.NoError(t, msgTx.Commit())

	logrus.Debugf("correlId=%x", msgTx.CorrelId)
	assert.Equal(t, msgTx.CorrelId, ibmmqtx.EmptyCorrelId)

	logrus.Debugln("put message without correl")
	msgID, tx, err = mq.PutMessage(&ibmmqtx.Msg{Payload: []byte("test2")})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	msgTx, _, _ = mq.GetMessageByID(context.Background(), msgID)
	require.NoError(t, msgTx.Commit())

	logrus.Debugf("correlId=%x", msgTx.CorrelId)

}
