package ibmmqtx

func (p *MQPro) Disconnect() {
	p.log.Debug("Request disconnect...")

	if p.conn == nil {
		p.log.Warnf("Already disconnected")
		return
	}

	if p.conn.ctx == nil {
		p.log.Debug("Already disconnected")
		return
	}
	if p.conn.ctx.Err() == nil {
		p.conn.ctxCancel()
	}

	p.m.Lock()
	defer p.m.Unlock()

	p.conn.ctx = nil
	p.conn.ctxCancel = nil

	p.conn.disconnect()

	p.log.Info("Disconnected")
}

func (c *connection) disconnect() {
	if c.stateConn == stateConnect {
		c.log.Trace("Disconnecting...")
		c._disconnect()
		c.log.Info("connection disconnected")

		c.stateConn = stateDisconnect
	}
}

func (c *connection) _disconnect() {
	// убираем подписку на калбеки
	c.isWarn(c.deReg())
	c._unregisterInMsg()

	if c.mqQueryManager != nil {
		c.isWarnConn(c.mqQueryManager.Disc())
		c.mqQueryManager = nil
	}
}
