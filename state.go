/*
**/

package ibmmqtx

func (c *connection) reqError() {
	if c.stateConn == stateDisconnect {
		return
	}
	if c.stateConn == stateConnect {
		c.log.Traceln("reqError, _disconnect")
		c._disconnect()
	}

	c.mutex.Lock()
	c.stateConn = stateErr
	c.mutex.Unlock()

	if c.autoReconnect {
		c.reconnect()
	}
}

func (c *connection) reconnect() {
	c.log.Traceln("send run_reconnect signal")

	c.pool.Schedule(func() { c.signalDisconnect <- struct{}{} })
}
