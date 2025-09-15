package rdsd

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// 实现 Discovery 接口
// 使用etcd存储服务信息。key: /rdsd/service/{name}/{id}, value: json
// 使用etcd的lease机制实现服务过期
// 使用etcd的watch机制监听服务变化
// 注册服务时创建lease并定时续约
// 取消注册时删除对应的key

var _ Discovery = (*EtcdDiscovery)(nil)

type EtcdDiscovery struct {
	client    *clientv3.Client
	marshaler Marshaller
	lg        *logrus.Entry
	closeOnce sync.Once
	done      chan struct{}
	// 统一的读写锁
	lock sync.RWMutex
	// lease TTL时间
	leaseTTL int64
	// 监听器列表
	listeners []Listener
	// 本地缓存的服务信息 map[serviceName]map[serviceID]ServerInfo
	cache map[string]map[string]ServerInfo
	// 本地注册的服务信息 map[serviceName]map[serviceID]ServerInfo
	localServices map[string]map[string]ServerInfoProvider
	// 服务对应的lease ID map[serviceName]map[serviceID]clientv3.LeaseID
	leases map[string]map[string]clientv3.LeaseID
	// watch相关
	watchCtx    context.Context
	watchCancel context.CancelFunc
}

// NewEtcdDiscovery 创建基于etcd的服务发现实例
func NewEtcdDiscovery(client *clientv3.Client, marshaler Marshaller, lg *logrus.Entry) *EtcdDiscovery {
	if lg == nil {
		lg = logrus.NewEntry(logrus.StandardLogger())
	}

	watchCtx, watchCancel := context.WithCancel(context.Background())

	d := &EtcdDiscovery{
		client:        client,
		marshaler:     marshaler,
		lg:            lg.WithField("component", "etcd-discovery"),
		done:          make(chan struct{}),
		leaseTTL:      5, // 5秒TTL
		listeners:     make([]Listener, 0),
		cache:         make(map[string]map[string]ServerInfo),
		localServices: make(map[string]map[string]ServerInfoProvider),
		leases:        make(map[string]map[string]clientv3.LeaseID),
		watchCtx:      watchCtx,
		watchCancel:   watchCancel,
	}

	// 启动watch监听
	go d.startWatcher()

	return d
}

// serviceKey 生成服务在etcd中的key
func (e *EtcdDiscovery) serviceKey(name, id string) string {
	return fmt.Sprintf("/rdsd/service/%s/%s", name, id)
}

// servicePrefix 生成服务名称的前缀
func (e *EtcdDiscovery) servicePrefix(name string) string {
	return fmt.Sprintf("/rdsd/service/%s/", name)
}

// parseServiceKey 解析服务key，返回服务名和ID
func (e *EtcdDiscovery) parseServiceKey(key string) (name, id string, ok bool) {
	if !strings.HasPrefix(key, "/rdsd/service/") {
		return "", "", false
	}
	parts := strings.Split(strings.TrimPrefix(key, "/rdsd/service/"), "/")
	if len(parts) != 2 {
		return "", "", false
	}
	return parts[0], parts[1], true
}

// startWatcher 启动etcd watch监听
func (e *EtcdDiscovery) startWatcher() {
	defer func() {
		if r := recover(); r != nil {
			e.lg.Errorf("watcher panic: %v", r)
		}
	}()

	watchChan := e.client.Watch(e.watchCtx, "/rdsd/service/", clientv3.WithPrefix())

	for {
		select {
		case <-e.done:
			return
		case watchResp := <-watchChan:
			if watchResp.Err() != nil {
				e.lg.Errorf("watch error: %v", watchResp.Err())
				continue
			}

			for _, event := range watchResp.Events {
				e.handleWatchEvent(event)
			}
		}
	}
}

// handleWatchEvent 处理watch事件
func (e *EtcdDiscovery) handleWatchEvent(event *clientv3.Event) {
	name, id, ok := e.parseServiceKey(string(event.Kv.Key))
	if !ok {
		return
	}

	e.lock.Lock()
	defer e.lock.Unlock()

	switch event.Type {
	case clientv3.EventTypePut:
		// 服务添加或更新
		var info ServerInfo
		if len(event.Kv.Value) > 0 {
			var err error
			info, err = e.marshaler.Unmarshal(event.Kv.Value)
			if err != nil {
				e.lg.Errorf("unmarshal server info error: %v", err)
				return
			}
		}

		// 更新缓存
		if e.cache[name] == nil {
			e.cache[name] = make(map[string]ServerInfo)
		}
		_, exists := e.cache[name][id]
		e.cache[name][id] = info

		// 通知监听器
		for _, listener := range e.listeners {
			watchNames := listener.WatchNames()
			for _, watchName := range watchNames {
				if watchName == name {
					if exists {
						listener.OnUpdate(info)
					} else {
						listener.OnAdd(info)
					}
					break
				}
			}
		}

	case clientv3.EventTypeDelete:
		// 服务删除
		if e.cache[name] != nil {
			if info, exists := e.cache[name][id]; exists {
				delete(e.cache[name], id)
				if len(e.cache[name]) == 0 {
					delete(e.cache, name)
				}

				// 通知监听器
				for _, listener := range e.listeners {
					watchNames := listener.WatchNames()
					for _, watchName := range watchNames {
						if watchName == name {
							listener.OnRemove(info)
							break
						}
					}
				}
			}
		}
	}
}

// GetServer 根据服务名称和ID获取指定的服务信息
func (e *EtcdDiscovery) GetServer(name, id string) (info ServerInfo) {
	e.lock.RLock()
	defer e.lock.RUnlock()

	if serviceMap, exists := e.cache[name]; exists {
		if serverInfo, exists := serviceMap[id]; exists {
			return serverInfo
		}
	}
	return nil
}

// GetServers 根据服务名称获取所有相关的服务列表
func (e *EtcdDiscovery) GetServers(name string) (list []ServerInfo) {
	e.lock.RLock()
	defer e.lock.RUnlock()

	if serviceMap, exists := e.cache[name]; exists {
		for _, info := range serviceMap {
			list = append(list, info)
		}
	}
	return list
}

// LocalServers 获取通过Register注册的本地服务信息
func (e *EtcdDiscovery) LocalServers() (list []ServerInfo) {
	e.lock.RLock()
	defer e.lock.RUnlock()

	for _, serviceMap := range e.localServices {
		for _, provider := range serviceMap {
			list = append(list, provider.ServerInfo())
		}
	}
	return list
}

// AddListener 添加服务监听器
func (e *EtcdDiscovery) AddListener(l Listener) {
	e.lock.Lock()
	defer e.lock.Unlock()

	e.listeners = append(e.listeners, l)

	// 为新监听器同步当前缓存的服务
	watchNames := l.WatchNames()
	for _, name := range watchNames {
		if serviceMap, exists := e.cache[name]; exists {
			for _, info := range serviceMap {
				l.OnAdd(info)
			}
		} else {
			// 如果缓存中没有，从etcd同步
			go e.syncServiceByName(name)
		}
	}
}

// syncServiceByName 同步指定服务名的所有服务信息
func (e *EtcdDiscovery) syncServiceByName(name string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := e.client.Get(ctx, e.servicePrefix(name), clientv3.WithPrefix())
	if err != nil {
		e.lg.Errorf("sync service %s error: %v", name, err)
		return
	}

	e.lock.Lock()
	defer e.lock.Unlock()

	if e.cache[name] == nil {
		e.cache[name] = make(map[string]ServerInfo)
	}

	for _, kv := range resp.Kvs {
		_, id, ok := e.parseServiceKey(string(kv.Key))
		if !ok {
			continue
		}

		info, err := e.marshaler.Unmarshal(kv.Value)
		if err != nil {
			e.lg.Errorf("unmarshal server info error: %v", err)
			continue
		}

		e.cache[name][id] = info
	}
}

// SyncServers 手动触发服务同步
func (e *EtcdDiscovery) SyncServers() {
	e.lock.RLock()
	listeners := make([]Listener, len(e.listeners))
	copy(listeners, e.listeners)
	e.lock.RUnlock()

	// 收集所有需要监听的服务名
	watchNamesSet := make(map[string]bool)
	for _, listener := range listeners {
		for _, name := range listener.WatchNames() {
			watchNamesSet[name] = true
		}
	}

	// 同步每个服务
	for name := range watchNamesSet {
		go e.syncServiceByName(name)
	}
}

// Register 注册服务信息到发现服务中
func (e *EtcdDiscovery) Register(provider ServerInfoProvider) (err error) {
	info := provider.ServerInfo()
	name := info.GetName()
	id := info.GetID()

	e.lock.Lock()
	defer e.lock.Unlock()

	// 添加到本地服务列表
	if e.localServices[name] == nil {
		e.localServices[name] = make(map[string]ServerInfoProvider)
	}
	if e.leases[name] == nil {
		e.leases[name] = make(map[string]clientv3.LeaseID)
	}
	e.localServices[name][id] = provider

	// 创建lease
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	leaseResp, err := e.client.Grant(ctx, e.leaseTTL)
	if err != nil {
		return fmt.Errorf("create lease error: %v", err)
	}

	e.leases[name][id] = leaseResp.ID

	// 立即将服务信息写入etcd
	if err := e.updateServiceToEtcd(info, leaseResp.ID); err != nil {
		e.lg.Warnf("initial service update to etcd failed: %v", err)
	}

	// 启动续约和更新goroutine
	go e.maintainService(name, id, provider, leaseResp.ID)

	return nil
}

// maintainService 维护服务注册（续约和更新）
func (e *EtcdDiscovery) maintainService(name, id string, provider ServerInfoProvider, leaseID clientv3.LeaseID) {
	defer func() {
		if r := recover(); r != nil {
			e.lg.Errorf("maintain service %s/%s panic: %v", name, id, r)
		}
	}()

	// 启动lease续约
	keepAliveCh, kaerr := e.client.KeepAlive(context.Background(), leaseID)
	if kaerr != nil {
		e.lg.Errorf("keep alive lease error: %v", kaerr)
		return
	}

	// 消费keepalive响应
	go func() {
		for ka := range keepAliveCh {
			// 处理keepalive响应（可以记录日志等）
			_ = ka
		}
	}()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	lastVersion := ""

	for {
		select {
		case <-e.done:
			return
		case <-provider.Done():
			// 服务提供者关闭，注销服务
			e.unregisterService(name, id)
			return
		case info := <-provider.Update():
			// 服务信息更新
			e.updateServiceToEtcd(info, leaseID)
			lastVersion = info.GetVersion()
		case <-ticker.C:
			// 定时更新服务信息
			info := provider.ServerInfo()
			versionChanged := info.GetVersion() != lastVersion
			e.updateServiceToEtcd(info, leaseID)
			lastVersion = info.GetVersion()
			_ = versionChanged // 可以用于优化，版本未变化时减少更新频率
		}
	}
}

// updateServiceToEtcd 更新服务信息到etcd
func (e *EtcdDiscovery) updateServiceToEtcd(info ServerInfo, leaseID clientv3.LeaseID) error {
	data, err := e.marshaler.Marshal(info)
	if err != nil {
		return fmt.Errorf("marshal server info error: %v", err)
	}

	key := e.serviceKey(info.GetName(), info.GetID())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err = e.client.Put(ctx, key, string(data), clientv3.WithLease(leaseID))
	if err != nil {
		e.lg.Errorf("update service to etcd error: %v", err)
		return err
	}

	return nil
}

// unregisterService 注销服务
func (e *EtcdDiscovery) unregisterService(name, id string) {
	e.lock.Lock()
	defer e.lock.Unlock()
	e.unregisterServiceUnsafe(name, id)
}

// unregisterServiceUnsafe 注销服务（不加锁版本，用于内部调用）
func (e *EtcdDiscovery) unregisterServiceUnsafe(name, id string) {
	// 删除etcd中的key
	key := e.serviceKey(name, id)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err := e.client.Delete(ctx, key)
	if err != nil {
		e.lg.Errorf("delete service from etcd error: %v", err)
	}

	// 撤销lease
	if e.leases[name] != nil {
		if leaseID, exists := e.leases[name][id]; exists {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_, err := e.client.Revoke(ctx, leaseID)
			if err != nil {
				e.lg.Errorf("revoke lease error: %v", err)
			}
			delete(e.leases[name], id)
			if len(e.leases[name]) == 0 {
				delete(e.leases, name)
			}
		}
	}

	// 删除本地服务记录
	if e.localServices[name] != nil {
		delete(e.localServices[name], id)
		if len(e.localServices[name]) == 0 {
			delete(e.localServices, name)
		}
	}
}

// Close 关闭服务发现，清理资源
func (e *EtcdDiscovery) Close() error {
	e.closeOnce.Do(func() {
		close(e.done)
		e.watchCancel()

		e.lock.Lock()
		defer e.lock.Unlock()

		// 注销所有本地服务（使用不加锁版本避免死锁）
		for name, serviceMap := range e.localServices {
			for id := range serviceMap {
				e.unregisterServiceUnsafe(name, id)
			}
		}

		// 清理资源
		e.listeners = nil
		e.cache = nil
		e.localServices = nil
		e.leases = nil
	})

	return nil
}