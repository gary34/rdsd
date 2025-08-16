package rdsd

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type Marshaler interface {
	Marshal(v ServerInfo) ([]byte, error)
	Unmarshal(data []byte) (v ServerInfo, err error)
}

func NewJSONMarshaler(infoMaker func() ServerInfo) Marshaler {
	return &jsonMarshaler{
		infoMaker: infoMaker,
	}
}

type jsonMarshaler struct {
	infoMaker func() ServerInfo
}

// 实现 Marshaler 接口
func (j *jsonMarshaler) Marshal(v ServerInfo) ([]byte, error) {
	return json.Marshal(v)
}

func (j *jsonMarshaler) Unmarshal(data []byte) (v ServerInfo, err error) {
	if j.infoMaker == nil {
		return nil, fmt.Errorf("infoMaker is nil")
	}
	v = j.infoMaker()
	err = json.Unmarshal(data, v)
	return
}

// 实现 Discovery 接口
// 使用redis.hash 存储服务信息。key: rdsd:service:name, field: id, value: json, 并设置5秒的过期时间
// 定时扫描所有服务，比对本地缓存，有变化则通知监听器
// 监听服务变化事件，有变化则更新本地缓存
// 注册服务的时候启动一个goroutine，定时更新服务信息到redis中，过期时间为5秒
// 取消注册服务的时候，删除redis中的服务信息

var _ Discovery = (*RedisDiscovery)(nil)

type RedisDiscovery struct {
	client    *redis.Client
	marshaler Marshaler
	// 统一的读写锁
	lock sync.RWMutex
	// 过期时间
	expire time.Duration
	// 监听器列表
	listeners []Listener
	// 本地缓存的服务信息 map[serviceName]map[serviceID]ServerInfo
	cache map[string]map[string]ServerInfo
	// 本地注册的服务信息 map[serviceName]map[serviceID]ServerInfo
	localServices map[string]map[string]ServerInfo
	// 注册的服务提供者 map[serviceName]map[serviceID]ServerInfoProvider
	providers map[string]map[string]ServerInfoProvider
}

func NewRedisDiscovery(client *redis.Client, marshaler Marshaler) *RedisDiscovery {
	r := &RedisDiscovery{
		client:        client,
		marshaler:     marshaler,
		expire:        10 * time.Second,
		listeners:     make([]Listener, 0),
		cache:         make(map[string]map[string]ServerInfo),
		localServices: make(map[string]map[string]ServerInfo),
		providers:     make(map[string]map[string]ServerInfoProvider),
	}
	go func() {
		ticker := time.NewTicker(r.expire)
		defer ticker.Stop()
		for range ticker.C {
			r.scanServers()
		}
	}()
	return r
}

func (r *RedisDiscovery) scanServers() {
	r.lock.RLock()
	listeners := make([]Listener, len(r.listeners))
	copy(listeners, r.listeners)
	r.lock.RUnlock()

	// 获取所有listener需要监听的服务名称
	watchNames := make(map[string][]Listener)
	for _, listener := range listeners {
		for _, name := range listener.WatchNames() {
			watchNames[name] = append(watchNames[name], listener)
		}
	}

	// 扫描所有服务名称
	for name, listeners := range watchNames {
		r.scanServiceByName(name, listeners)
	}
}

func (r *RedisDiscovery) key(name string) string {
	return fmt.Sprintf("rdsd:service:%s", name)
}

func (r *RedisDiscovery) scanServiceByName(name string, listeners []Listener) {
	key := r.key(name)
	ctx := context.Background()

	// 从Redis获取所有服务信息
	result, err := r.client.HVals(ctx, key).Result()
	if err != nil {
		return
	}

	r.lock.Lock()
	defer r.lock.Unlock()

	// 获取当前缓存
	oldCache := r.cache[name]
	if oldCache == nil {
		oldCache = make(map[string]ServerInfo)
	}

	// 新的缓存
	newCache := make(map[string]ServerInfo)

	// 解析Redis中的服务信息
	for _, data := range result {
		// 这里需要具体的ServerInfo实现来反序列化
		v, err := r.marshaler.Unmarshal([]byte(data))
		if err != nil {
			continue
		}
		newCache[v.GetID()] = v
	}

	// 更新缓存
	r.cache[name] = newCache

	// 比较变化并通知监听器
	for id, info := range newCache {
		if _, exists := oldCache[id]; !exists {
			// 新增服务
			for _, listener := range listeners {
				listener.OnAdd(info)
			}
		} else if info != oldCache[id] {
			// 服务更新
			for _, listener := range listeners {
				listener.OnUpdate(info)
			}
		}
	}

	// 检查移除的服务
	for id, info := range oldCache {
		if _, exists := newCache[id]; !exists {
			// 服务移除
			for _, listener := range listeners {
				listener.OnRemove(info)
			}
		}
	}
}

// AddListener implements Discovery.
func (r *RedisDiscovery) AddListener(l Listener) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.listeners = append(r.listeners, l)
}

// GetServer implements Discovery. 只从本地缓存中获取服务信息
func (r *RedisDiscovery) GetServer(name string, id string) (info ServerInfo) {
	r.lock.RLock()
	defer r.lock.RUnlock()

	if services, exists := r.cache[name]; exists {
		if service, exists := services[id]; exists {
			return service
		}
	}
	return nil
}

// GetServers implements Discovery. 只从本地缓存中获取服务信息
func (r *RedisDiscovery) GetServers(name string) (list []ServerInfo) {
	r.lock.RLock()
	defer r.lock.RUnlock()

	if services, exists := r.cache[name]; exists {
		list = make([]ServerInfo, 0, len(services))
		for _, service := range services {
			list = append(list, service)
		}
	}
	return list
}

// LocalServers implements Discovery. 获取通过Register注册的本地服务信息
func (r *RedisDiscovery) LocalServers() (list []ServerInfo) {
	r.lock.RLock()
	defer r.lock.RUnlock()

	list = make([]ServerInfo, 0)
	for _, services := range r.localServices {
		for _, service := range services {
			list = append(list, service)
		}
	}
	return list
}

// Register implements Discovery.
func (r *RedisDiscovery) Register(provider ServerInfoProvider) (err error) {
	info := provider.ServerInfo()
	if info == nil {
		return fmt.Errorf("server info is nil")
	}

	name := info.GetName()
	id := info.GetID()

	r.lock.Lock()
	// 初始化map
	if r.localServices[name] == nil {
		r.localServices[name] = make(map[string]ServerInfo)
	}
	if r.providers[name] == nil {
		r.providers[name] = make(map[string]ServerInfoProvider)
	}

	// 保存本地服务信息和提供者
	r.localServices[name][id] = info
	r.providers[name][id] = provider
	r.lock.Unlock()

	// 启动goroutine定时更新服务信息到Redis
	go func() {
		ticker := time.NewTicker(r.expire / 2) // 更新频率为过期时间的一半
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				// 定时更新服务信息到Redis
				currentInfo := provider.ServerInfo()
				if currentInfo != nil {
					r.updateServiceToRedis(currentInfo)
					// 更新本地缓存
					r.lock.Lock()
					r.localServices[name][id] = currentInfo
					r.lock.Unlock()
				}
			case <-provider.Done():
				// 服务关闭，清理资源
				r.unregisterService(name, id)
				return
			}
		}
	}()

	// 立即注册一次
	return r.updateServiceToRedis(info)
}

func (r *RedisDiscovery) updateServiceToRedis(info ServerInfo) error {
	key := fmt.Sprintf("rdsd:service:%s", info.GetName())
	ctx := context.Background()

	// 序列化服务信息
	data, err := r.marshaler.Marshal(info)
	if err != nil {
		return fmt.Errorf("marshal server info failed: %w", err)
	}

	// 存储到Redis Hash中
	err = r.client.HSet(ctx, key, info.GetID(), data).Err()
	if err != nil {
		return fmt.Errorf("redis hset failed: %w", err)
	}

	// 设置过期时间
	err = r.client.Expire(ctx, key, r.expire).Err()
	if err != nil {
		return fmt.Errorf("redis expire failed: %w", err)
	}

	return nil
}

func (r *RedisDiscovery) unregisterService(name, id string) {
	key := fmt.Sprintf("rdsd:service:%s", name)
	ctx := context.Background()

	// 从Redis中删除服务信息
	r.client.HDel(ctx, key, id)

	r.lock.Lock()
	defer r.lock.Unlock()

	// 清理本地缓存
	if r.localServices[name] != nil {
		delete(r.localServices[name], id)
		if len(r.localServices[name]) == 0 {
			delete(r.localServices, name)
		}
	}

	// 清理提供者
	if r.providers[name] != nil {
		delete(r.providers[name], id)
		if len(r.providers[name]) == 0 {
			delete(r.providers, name)
		}
	}
}
