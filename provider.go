package ebsd

import (
	"sync"
	"sync/atomic"
)

type BaseProvider struct {
	doneChan   chan struct{}
	updateChan chan ServerInfo
	closeOnce  sync.Once
	closed     atomic.Bool
	infoFunc   func() ServerInfo
}

func NewBaseProvider(infoFunc func() ServerInfo) ServerInfoProvider {
	return &BaseProvider{
		infoFunc:   infoFunc,
		doneChan:   make(chan struct{}),
		updateChan: make(chan ServerInfo),
	}
}

func (p *BaseProvider) Update() <-chan ServerInfo {
	return p.updateChan
}

func (p *BaseProvider) Closed() bool {
	return p.closed.Load()
}

func (p *BaseProvider) NtfUpdate() {
	if p.Closed() {
		return
	}
	p.updateChan <- p.ServerInfo()
}

func (p *BaseProvider) ServerInfo() ServerInfo {
	return p.infoFunc()
}

func (p *BaseProvider) Done() <-chan struct{} {
	return p.doneChan
}

func (p *BaseProvider) Close() {
	p.closeOnce.Do(func() {
		close(p.doneChan)
		close(p.updateChan)
		p.closed.Store(true)
	})
}
