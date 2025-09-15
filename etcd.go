package rdsd

import (
	"context"
	"fmt"
	"slices"
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

type serverCaches map[string]map[string]ServerInfo

func (cs serverCaches) Compare(cs1 serverCaches) (changes serverChanges) {
	for name, ids := range cs {
		for id, info := range ids {
			if _, ok := cs1[name][id]; !ok {
				changes.delete = append(changes.delete, info)
			}
		}
	}
	for name, ids := range cs1 {
		for id, info := range ids {
			if oldInfo, ok := cs[name][id]; !ok {
				changes.add = append(changes.add, info)
			} else if oldInfo.GetVersion() != info.GetVersion() {
				changes.update = append(changes.update, info)
			}
		}
	}
	return
}

type EtcdDiscovery struct {
	client    *clientv3.Client
	marshaler Marshaller
	prefix    string
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
	cache serverCaches
	// 本地注册的服务信息 map[serviceName]map[serviceID]ServerInfo
	localServices map[string]map[string]ServerInfoProvider
	localIDs      map[string]struct{}
	// 统一的lease ID，所有服务共享
	globalLease clientv3.LeaseID
	// lease是否已创建
	leaseCreated bool
	// watch相关
	watchCtx    context.Context
	watchCancel context.CancelFunc
}

// NewEtcdDiscovery 创建基于etcd的服务发现实例
func NewEtcdDiscovery(client *clientv3.Client, marshaler Marshaller, prefix string, lg *logrus.Entry) *EtcdDiscovery {
	if prefix == "" {
		prefix = "/rdsd"
	}
	if lg == nil {
		lg = logrus.NewEntry(logrus.StandardLogger())
	}

	watchCtx, watchCancel := context.WithCancel(context.Background())

	d := &EtcdDiscovery{
		client:        client,
		marshaler:     marshaler,
		prefix:        prefix,
		lg:            lg.WithField("component", "etcd-discovery"),
		done:          make(chan struct{}),
		leaseTTL:      10, // 5秒TTL
		listeners:     make([]Listener, 0),
		cache:         make(serverCaches),
		localServices: make(map[string]map[string]ServerInfoProvider),
		localIDs:      make(map[string]struct{}),
		globalLease:   0, // 初始化为0，表示未创建
		leaseCreated:  false,
		watchCtx:      watchCtx,
		watchCancel:   watchCancel,
	}

	// 启动watch监听
	go d.startWatcher()

	return d
}

// serviceKey 生成服务在etcd中的key
func (e *EtcdDiscovery) serviceKey(name, id string) string {
	//return fmt.Sprintf(e.prefix+"/service/%s/%s", name, id)
	return e.servicePrefix(name) + "/" + id
}

// servicePrefix 生成服务名称的前缀
func (e *EtcdDiscovery) servicePrefix(name string) string {
	return e.serviceAllPrefix() + "/" + name
}

// serviceAllPrefix 生成服务名称的前缀
func (e *EtcdDiscovery) serviceAllPrefix() string {
	return e.prefix + "/service"
}

// parseServiceKey 解析服务key，返回服务名和ID
func (e *EtcdDiscovery) parseServiceKey(key string) (name, id string, ok bool) {
	prefix := e.serviceAllPrefix()
	if !strings.HasPrefix(key, prefix) {
		return "", "", false
	}
	parts := strings.Split(strings.TrimPrefix(key, prefix+"/"), "/")
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

	watchChan := e.client.Watch(e.watchCtx, e.prefix+"/service/", clientv3.WithPrefix())

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

type serverChanges struct {
	delete []ServerInfo
	add    []ServerInfo
	update []ServerInfo
}

func (b serverChanges) notifyListeners(listeners []Listener) {
	if len(listeners) == 0 {
		return
	}
	namesByWatch := make(map[string][]Listener)
	for _, listener := range listeners {
		for _, watchName := range listener.WatchNames() {
			namesByWatch[watchName] = append(namesByWatch[watchName], listener)
		}
	}
	for _, info := range b.add {
		for _, l := range namesByWatch[info.GetName()] {
			l.OnAdd(info)
		}
	}
	for _, info := range b.update {
		for _, l := range namesByWatch[info.GetName()] {
			l.OnUpdate(info)
		}
	}
	for _, info := range b.delete {
		for _, l := range namesByWatch[info.GetName()] {
			l.OnRemove(info)
		}
	}
}

func (e *EtcdDiscovery) ntfChange(buff serverChanges) {
	e.lock.RLock()
	ls := make([]Listener, len(e.listeners))
	copy(ls, e.listeners)
	buff.add = slices.DeleteFunc(buff.add, func(info ServerInfo) bool {
		_, ok := e.localIDs[info.GetID()]
		return ok
	})
	buff.update = slices.DeleteFunc(buff.update, func(info ServerInfo) bool {
		_, ok := e.localIDs[info.GetID()]
		return ok
	})
	buff.delete = slices.DeleteFunc(buff.delete, func(info ServerInfo) bool {
		_, ok := e.localIDs[info.GetID()]
		return ok
	})
	e.lock.RUnlock()
	go buff.notifyListeners(ls)
}

// handleWatchEvent 处理watch事件
func (e *EtcdDiscovery) handleWatchEvent(event *clientv3.Event) {
	name, id, ok := e.parseServiceKey(string(event.Kv.Key))
	if !ok {
		return
	}
	var changes serverChanges
	defer func() {
		e.ntfChange(changes)
	}()
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
		if exists {
			changes.update = append(changes.update, info)
		} else {
			changes.add = append(changes.add, info)
		}
		e.cache[name][id] = info
	case clientv3.EventTypeDelete:
		// 服务删除
		if e.cache[name] != nil {
			if info, exists := e.cache[name][id]; exists {
				delete(e.cache[name], id)
				changes.delete = append(changes.delete, info)
				if len(e.cache[name]) == 0 {
					delete(e.cache, name)
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
}

// SyncServers 手动触发服务同步
func (e *EtcdDiscovery) SyncServers() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := e.client.Get(ctx, e.serviceAllPrefix(), clientv3.WithPrefix())
	if err != nil {
		e.lg.WithError(err).Error("sync error")
		return
	}
	cs := make(serverCaches)
	for _, kv := range resp.Kvs {
		_, id, ok := e.parseServiceKey(string(kv.Key))
		if !ok {
			continue
		}
		info, uerr := e.marshaler.Unmarshal(kv.Value)
		if uerr != nil {
			e.lg.WithError(err).Error("unmarshal server info error")
			continue
		}
		name := info.GetName()
		if cs[name] == nil {
			cs[name] = make(map[string]ServerInfo)
		}
		cs[name][id] = info
	}
	e.lock.Lock()
	changes := e.cache.Compare(cs)
	e.cache = cs
	e.lock.Unlock()
	e.ntfChange(changes)
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
	e.localServices[name][id] = provider
	e.localIDs[id] = struct{}{}
	// 如果全局lease还未创建，则创建它
	if !e.leaseCreated {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		leaseResp, err := e.client.Grant(ctx, e.leaseTTL)
		if err != nil {
			return fmt.Errorf("create global lease error: %v", err)
		}

		e.globalLease = leaseResp.ID
		e.leaseCreated = true

		// 启动全局lease的续约
		go e.maintainGlobalLease()
	}

	// 立即将服务信息写入etcd
	if err := e.updateServiceToEtcd(info, e.globalLease); err != nil {
		e.lg.Warnf("initial service update to etcd failed: %v", err)
	}

	// 启动服务更新goroutine
	go e.maintainService(name, id, provider, e.globalLease)

	return nil
}

// maintainService 维护服务注册（续约和更新）
// maintainGlobalLease 维护全局lease的续约
func (e *EtcdDiscovery) maintainGlobalLease() {
	defer func() {
		if r := recover(); r != nil {
			e.lg.Errorf("maintain global lease panic: %v", r)
		}
	}()

	// 启动lease续约
	keepAliveCh, kaerr := e.client.KeepAlive(context.Background(), e.globalLease)
	if kaerr != nil {
		e.lg.Errorf("keep alive global lease error: %v", kaerr)
		return
	}

	// 消费keepalive响应
	for {
		select {
		case <-e.done:
			return
		case ka := <-keepAliveCh:
			if ka == nil {
				e.lg.Warn("global lease keepalive channel closed")
				return
			}
			// 处理keepalive响应（可以记录日志等）
		}
	}
}

func (e *EtcdDiscovery) maintainService(name, id string, provider ServerInfoProvider, leaseID clientv3.LeaseID) {
	defer func() {
		if r := recover(); r != nil {
			e.lg.Errorf("maintain service %s/%s panic: %v", name, id, r)
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
			if versionChanged {
				e.updateServiceToEtcd(info, leaseID)
				lastVersion = info.GetVersion()
			}
			//_ = versionChanged // 可以用于优化，版本未变化时减少更新频率
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

	// 注意：不再撤销lease，因为使用全局lease，其他服务可能还在使用

	// 删除本地服务记录
	if e.localServices[name] != nil {
		delete(e.localServices[name], id)
		delete(e.localIDs, id)
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

		// 撤销全局lease
		if e.leaseCreated && e.globalLease != 0 {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_, err := e.client.Revoke(ctx, e.globalLease)
			if err != nil {
				e.lg.Errorf("revoke global lease error: %v", err)
			}
		}

		// 清理资源
		e.listeners = nil
		e.cache = nil
		e.localServices = nil
		e.leaseCreated = false
		e.globalLease = 0
	})

	return nil
}
