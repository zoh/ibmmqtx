package ibmmqtx

import "context"

type Pool struct {
	work chan func()
	sem  chan struct{}

	rootCtx context.Context
}

func NewPool(ctx context.Context, size int) *Pool {
	if size <= 0 {
		size = DefaultPoolSize
	}

	return &Pool{
		work:    make(chan func()),
		sem:     make(chan struct{}, size),
		rootCtx: ctx,
	}
}

func (p *Pool) Schedule(task func()) {
	select {
	case p.work <- task:
	case p.sem <- struct{}{}:
		go p.worker(task)
	}
}

func (p *Pool) worker(task func()) {
	defer func() { <-p.sem }()
	for {
		select {
		case <-p.rootCtx.Done():
			return
		default:
			task()
			task = <-p.work
		}
	}
}
