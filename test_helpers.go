package rdsd

import (
	"sync"
	"time"
)

// 测试用的ServerInfo实现
type testServerInfo struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	LastUpdate int64  `json:"last_update_time"`
	VersionNum string `json:"version"`
}

func (t *testServerInfo) GetID() string {
	return t.ID
}

func (t *testServerInfo) GetName() string {
	return t.Name
}

func (t *testServerInfo) GetUpdateTime() int64 {
	return t.LastUpdate
}

func (t *testServerInfo) GetVersion() string {
	return t.VersionNum
}

func (t *testServerInfo) Clone() ServerInfo {
	return &testServerInfo{
		ID:         t.ID,
		Name:       t.Name,
		LastUpdate: t.LastUpdate,
		VersionNum: t.VersionNum,
	}
}

// 测试用的ServerInfoProvider实现
type testServerInfoProvider struct {
	info       *testServerInfo
	doneChan   chan struct{}
	updateChan chan ServerInfo
	mutex      sync.RWMutex
	closed     bool
}

func newTestServerInfoProvider(id, name, version string) *testServerInfoProvider {
	return &testServerInfoProvider{
		info: &testServerInfo{
			ID:         id,
			Name:       name,
			LastUpdate: time.Now().Unix(),
			VersionNum: version,
		},
		doneChan:   make(chan struct{}),
		updateChan: make(chan ServerInfo),
	}
}

func (t *testServerInfoProvider) Update() <-chan ServerInfo {
	return t.updateChan
}

func (t *testServerInfoProvider) NtfUpdate() {
	t.mutex.RLock()
	t.updateChan <- t.info
	t.mutex.RUnlock()
	time.Sleep(time.Millisecond * 100)
}

func (t *testServerInfoProvider) ServerInfo() ServerInfo {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	return t.info
}

func (t *testServerInfoProvider) Done() <-chan struct{} {
	return t.doneChan
}

func (t *testServerInfoProvider) Close() {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if !t.closed {
		close(t.doneChan)
		t.closed = true
	}
}

func (t *testServerInfoProvider) UpdateVersion(version string) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.info.VersionNum = version
	t.info.LastUpdate = time.Now().Unix()
}

// 测试用的Listener实现
type testListener struct {
	watchNames   []string
	addEvents    []ServerInfo
	removeEvents []ServerInfo
	updateEvents []ServerInfo
	mutex        sync.RWMutex
}

func newTestListener(watchNames ...string) *testListener {
	return &testListener{
		watchNames:   watchNames,
		addEvents:    make([]ServerInfo, 0),
		removeEvents: make([]ServerInfo, 0),
		updateEvents: make([]ServerInfo, 0),
	}
}

func (t *testListener) WatchNames() []string {
	return t.watchNames
}

func (t *testListener) OnAdd(info ServerInfo) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.addEvents = append(t.addEvents, info)
}

func (t *testListener) OnRemove(info ServerInfo) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.removeEvents = append(t.removeEvents, info)
}

func (t *testListener) OnUpdate(info ServerInfo) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.updateEvents = append(t.updateEvents, info)
}

func (t *testListener) GetAddEvents() []ServerInfo {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	events := make([]ServerInfo, len(t.addEvents))
	copy(events, t.addEvents)
	return events
}

func (t *testListener) GetRemoveEvents() []ServerInfo {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	events := make([]ServerInfo, len(t.removeEvents))
	copy(events, t.removeEvents)
	return events
}

func (t *testListener) GetUpdateEvents() []ServerInfo {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	events := make([]ServerInfo, len(t.updateEvents))
	copy(events, t.updateEvents)
	return events
}

func (t *testListener) ClearEvents() {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.addEvents = t.addEvents[:0]
	t.removeEvents = t.removeEvents[:0]
	t.updateEvents = t.updateEvents[:0]
}