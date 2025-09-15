package rdsd

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

// 实现 Discovery 接口
// 使用redis.hash 存储服务信息。key: rdsd:service:name, field: id, value: json, 并设置5秒的过期时间
// 定时扫描所有服务，比对本地缓存，有变化则通知监听器
// 监听服务变化事件，有变化则更新本地缓存
// 注册服务的时候启动一个goroutine，定时更新服务信息到redis中，过期时间为5秒
// 取消注册服务的时候，删除redis中的服务信息

var _ Discovery = (*RedisDiscovery)(nil)

type RedisDiscovery struct {
	client    redis.UniversalClient
	marshaler Marshaller
	lg        *logrus.Entry
	closeOnce sync.Once
	done      chan struct{}
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
	// pub/sub相关
	pubsub    *redis.PubSub
	subCtx    context.Context
	subCancel context.CancelFunc
}

// SyncServers implements Discovery.
func (r *RedisDiscovery) SyncServers() {
	r.lg.Debug("手动触发服务同步")
	// r.syncCh <- struct{}{}
	r.scanServers()
}

func NewRedisDiscovery(client redis.UniversalClient, marshaler Marshaller, lg *logrus.Entry) *RedisDiscovery {
	lg.Info("开始创建RedisDiscovery实例")

	subCtx, subCancel := context.WithCancel(context.Background())
	r := &RedisDiscovery{
		client:        client,
		marshaler:     marshaler,
		expire:        10 * time.Second,
		lg:            lg,
		listeners:     make([]Listener, 0),
		cache:         make(map[string]map[string]ServerInfo),
		localServices: make(map[string]map[string]ServerInfo),
		providers:     make(map[string]map[string]ServerInfoProvider),
		subCtx:        subCtx,
		subCancel:     subCancel,
		done:          make(chan struct{}),
	}

	r.lg.WithFields(logrus.Fields{
		"expire_time":    r.expire,
		"marshaler_type": fmt.Sprintf("%T", r.marshaler),
	}).Info("RedisDiscovery配置完成")
	wg := sync.WaitGroup{}
	wg.Add(2)
	// 启动pub/sub订阅器
	r.lg.Info("启动pub/sub订阅器")
	go r.startSubscriber(&wg)

	// 启动定时扫描器
	r.lg.Info("启动定时扫描器")
	go r.startScanner(&wg)
	wg.Wait()
	r.lg.Info("RedisDiscovery实例创建完成")
	return r
}

func (r *RedisDiscovery) startScanner(wg *sync.WaitGroup) {
	// 由于有了pub/sub主动通知，降低定时扫描频率，作为兜底机制
	scanInterval := r.expire * 3 // 扫描间隔为过期时间的3倍
	r.lg.WithFields(logrus.Fields{
		"scan_interval": scanInterval,
		"expire_time":   r.expire,
	}).Info("定时扫描器启动（作为pub/sub的兜底机制）")

	ticker := time.NewTicker(scanInterval)
	defer ticker.Stop()
	doScan := func() {
		r.lg.Debug("定时扫描器触发，开始扫描服务")
		start := time.Now()
		r.scanServers()
		duration := time.Since(start)
		r.lg.WithFields(logrus.Fields{
			"scan_duration": duration,
		}).Debug("定时扫描完成")
	}
	wg.Done()
	for range ticker.C {
		doScan()
	}
}

func (r *RedisDiscovery) scanServers() {
	r.lg.Debug("开始扫描Redis中的服务信息")

	r.lock.RLock()
	listeners := make([]Listener, len(r.listeners))
	copy(listeners, r.listeners)
	r.lock.RUnlock()

	r.lg.WithFields(logrus.Fields{
		"listener_count": len(listeners),
	}).Debug("获取到监听器列表")

	// 获取所有listener需要监听的服务名称
	watchNames := make(map[string][]Listener)
	for _, listener := range listeners {
		for _, name := range listener.WatchNames() {
			watchNames[name] = append(watchNames[name], listener)
		}
	}

	r.lg.WithFields(logrus.Fields{
		"watch_service_count": len(watchNames),
		"watch_services": func() []string {
			names := make([]string, 0, len(watchNames))
			for name := range watchNames {
				names = append(names, name)
			}
			return names
		}(),
	}).Debug("获取到需要监听的服务名称列表")

	// 扫描所有服务名称
	for name, listeners := range watchNames {
		r.lg.WithFields(logrus.Fields{
			"service_name":   name,
			"listener_count": len(listeners),
		}).Debug("开始扫描服务")
		r.scanServiceByName(name, listeners)
	}

	r.lg.Debug("服务扫描完成")
}

func (r *RedisDiscovery) key(name string) string {
	return fmt.Sprintf("rdsd:service:%s", name)
}

func (r *RedisDiscovery) notifyChannel(name string) string {
	return fmt.Sprintf("rdsd:notify:%s", name)
}

// startSubscriber 启动Redis pub/sub订阅器
func (r *RedisDiscovery) startSubscriber(wg *sync.WaitGroup) {
	r.lg.Info("pub/sub订阅器启动")

	// 订阅所有服务变化通知
	r.pubsub = r.client.PSubscribe(r.subCtx, "rdsd:notify:*")
	defer func() {
		if r.pubsub != nil {
			r.pubsub.Close()
		}
	}()

	r.lg.Info("开始监听Redis pub/sub消息")
	ch := r.pubsub.Channel()
	wg.Done()
	for {
		select {
		case msg := <-ch:
			if msg == nil {
				continue
			}
			r.lg.WithFields(logrus.Fields{
				"channel": msg.Channel,
				"payload": msg.Payload,
			}).Debug("收到pub/sub通知")
			r.handlePubSubMessage(msg)
		case <-r.subCtx.Done():
			r.lg.Info("pub/sub订阅器停止")
			return
		}
	}
}

// handlePubSubMessage 处理pub/sub消息
func (r *RedisDiscovery) handlePubSubMessage(msg *redis.Message) {
	// 从频道名称中提取服务名称
	// 频道格式: rdsd:notify:serviceName
	if len(msg.Channel) < 13 { // "rdsd:notify:".length = 12
		return
	}
	serviceName := msg.Channel[12:] // 去掉"rdsd:notify:"前缀

	r.lg.WithFields(logrus.Fields{
		"service_name": serviceName,
		"action":       msg.Payload,
	}).Debug("处理服务变化通知")

	// 获取该服务的监听器
	r.lock.RLock()
	var targetListeners []Listener
	for _, listener := range r.listeners {
		for _, watchName := range listener.WatchNames() {
			if watchName == serviceName {
				targetListeners = append(targetListeners, listener)
				break
			}
		}
	}
	r.lock.RUnlock()

	if len(targetListeners) > 0 {
		r.lg.WithFields(logrus.Fields{
			"service_name":   serviceName,
			"listener_count": len(targetListeners),
		}).Debug("触发服务扫描")
		// 立即扫描该服务
		r.scanServiceByName(serviceName, targetListeners)
	}
}

// publishServiceChange 发布服务变化通知
func (r *RedisDiscovery) publishServiceChange(serviceName, action string) {
	channel := r.notifyChannel(serviceName)
	ctx := context.Background()

	r.lg.WithFields(logrus.Fields{
		"service_name": serviceName,
		"action":       action,
		"channel":      channel,
	}).Debug("发布服务变化通知")

	err := r.client.Publish(ctx, channel, action).Err()
	if err != nil {
		r.lg.WithFields(logrus.Fields{
			"service_name": serviceName,
			"action":       action,
			"channel":      channel,
			"error":        err,
		}).Error("发布服务变化通知失败")
	} else {
		r.lg.WithFields(logrus.Fields{
			"service_name": serviceName,
			"action":       action,
			"channel":      channel,
		}).Debug("服务变化通知发布成功")
	}
}

// Close 关闭RedisDiscovery，清理资源
func (r *RedisDiscovery) Close() error {
	var err error
	r.closeOnce.Do(func() {
		r.lg.Info("开始关闭RedisDiscovery")
		// 取消订阅上下文
		if r.subCancel != nil {
			r.subCancel()
		}
		close(r.done)
		// 关闭pub/sub连接
		if r.pubsub != nil {
			err = r.pubsub.Close()
			if err != nil {
				r.lg.WithFields(logrus.Fields{
					"error": err,
				}).Error("关闭pub/sub连接失败")
				// return err
				return
			}
		}
		r.lg.Info("RedisDiscovery关闭完成")
	})

	return nil
}

func (r *RedisDiscovery) scanServiceByName(name string, listeners []Listener) {
	key := r.key(name)
	ctx := context.Background()

	r.lg.WithFields(logrus.Fields{
		"service_name": name,
		"redis_key":    key,
	}).Debug("开始扫描指定服务")

	// 从Redis获取所有服务信息
	result, err := r.client.HVals(ctx, key).Result()
	if err != nil {
		r.lg.WithFields(logrus.Fields{
			"service_name": name,
			"redis_key":    key,
			"error":        err,
		}).Error("从Redis获取服务信息失败")
		return
	}

	r.lg.WithFields(logrus.Fields{
		"service_name":   name,
		"instance_count": len(result),
	}).Debug("获取到服务实例列表")

	r.lock.Lock()
	defer r.lock.Unlock()

	// 获取当前缓存
	oldCache := r.cache[name]
	if oldCache == nil {
		oldCache = make(map[string]ServerInfo)
	}

	r.lg.WithFields(logrus.Fields{
		"service_name":    name,
		"old_cache_count": len(oldCache),
	}).Debug("获取到当前缓存信息")

	// 新的缓存
	newCache := make(map[string]ServerInfo)

	// 解析Redis中的服务信息
	successCount := 0
	for _, data := range result {
		// 这里需要具体的ServerInfo实现来反序列化
		v, err := r.marshaler.Unmarshal([]byte(data))
		if err != nil {
			r.lg.WithFields(logrus.Fields{
				"service_name": name,
				"error":        err,
			}).Error("反序列化服务信息失败")
			continue
		}
		newCache[v.GetID()] = v
		successCount++
	}

	r.lg.WithFields(logrus.Fields{
		"service_name":    name,
		"success_count":   successCount,
		"total_count":     len(result),
		"new_cache_count": len(newCache),
	}).Debug("服务信息解析完成")

	// 更新缓存
	r.cache[name] = newCache

	// 比较变化并通知监听器
	addCount := 0
	updateCount := 0
	for id, info := range newCache {
		if _, exists := oldCache[id]; !exists {
			// 新增服务
			r.lg.WithFields(logrus.Fields{
				"service_name": name,
				"service_id":   id,
				"version":      info.GetVersion(),
			}).Debug("检测到新增服务实例")
			for _, listener := range listeners {
				listener.OnAdd(info)
			}
			addCount++
		} else if info.GetVersion() != oldCache[id].GetVersion() {
			// 服务更新
			r.lg.WithFields(logrus.Fields{
				"service_name": name,
				"service_id":   id,
				"old_version":  oldCache[id].GetVersion(),
				"new_version":  info.GetVersion(),
			}).Debug("检测到服务实例更新")
			for _, listener := range listeners {
				listener.OnUpdate(info)
			}
			updateCount++
		}
	}

	// 检查移除的服务
	removeCount := 0
	for id, info := range oldCache {
		if _, exists := newCache[id]; !exists {
			// 服务移除
			r.lg.WithFields(logrus.Fields{
				"service_name": name,
				"service_id":   id,
				"version":      info.GetVersion(),
			}).Debug("检测到服务实例移除")
			for _, listener := range listeners {
				listener.OnRemove(info)
			}
			removeCount++
		}
	}

	r.lg.WithFields(logrus.Fields{
		"service_name":   name,
		"listener_count": len(listeners),
		"add_count":      addCount,
		"update_count":   updateCount,
		"remove_count":   removeCount,
	}).Debug("服务变更检测完成，已通知监听器")
}

// AddListener implements Discovery.
func (r *RedisDiscovery) AddListener(l Listener) {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.lg.WithFields(logrus.Fields{
		"watch_services": l.WatchNames(),
	}).Debug("开始添加服务监听器")

	r.listeners = append(r.listeners, l)

	r.lg.WithFields(logrus.Fields{
		"total_listener_count": len(r.listeners),
		"watch_services":       l.WatchNames(),
	}).Debug("服务监听器添加完成")
}

// GetServer implements Discovery. 只从本地缓存中获取服务信息
func (r *RedisDiscovery) GetServer(name string, id string) (info ServerInfo) {
	r.lock.RLock()
	defer r.lock.RUnlock()

	r.lg.WithFields(logrus.Fields{
		"service_name": name,
		"service_id":   id,
	}).Debug("开始获取服务实例")

	if services, exists := r.cache[name]; exists {
		if service, exists := services[id]; exists {
			r.lg.WithFields(logrus.Fields{
				"service_name": name,
				"service_id":   id,
				"version":      service.GetVersion(),
			}).Debug("获取到服务实例")
			return service
		}
	}

	r.lg.WithFields(logrus.Fields{
		"service_name": name,
		"service_id":   id,
	}).Debug("服务实例不存在")
	return nil
}

// GetServers implements Discovery. 只从本地缓存中获取服务信息
func (r *RedisDiscovery) GetServers(name string) (list []ServerInfo) {
	r.lock.RLock()
	defer r.lock.RUnlock()

	r.lg.WithFields(logrus.Fields{
		"service_name": name,
	}).Debug("开始获取服务实例列表")

	if services, exists := r.cache[name]; exists {
		list = make([]ServerInfo, 0, len(services))
		for _, service := range services {
			list = append(list, service)
		}
		r.lg.WithFields(logrus.Fields{
			"service_name":   name,
			"instance_count": len(list),
		}).Debug("获取到服务实例列表")
	} else {
		r.lg.WithFields(logrus.Fields{
			"service_name": name,
		}).Debug("服务不存在")
	}
	return list
}

// LocalServers implements Discovery. 获取通过Register注册的本地服务信息
func (r *RedisDiscovery) LocalServers() (list []ServerInfo) {
	r.lock.RLock()
	defer r.lock.RUnlock()

	r.lg.Debug("开始获取所有本地服务实例列表")

	list = make([]ServerInfo, 0)
	for serviceName, services := range r.localServices {
		for _, service := range services {
			list = append(list, service)
		}
		r.lg.WithFields(logrus.Fields{
			"service_name":   serviceName,
			"instance_count": len(services),
		}).Debug("获取到本地服务实例")
	}

	r.lg.WithFields(logrus.Fields{
		"total_instance_count": len(list),
	}).Debug("获取所有本地服务实例列表完成")

	return list
}

// Register implements Discovery.
func (r *RedisDiscovery) Register(provider ServerInfoProvider) (err error) {
	info := provider.ServerInfo()
	if info == nil {
		r.lg.Error("注册服务失败: 服务信息为空")
		return fmt.Errorf("server info is nil")
	}

	name := info.GetName()
	id := info.GetID()

	r.lg.WithFields(logrus.Fields{
		"service_name": name,
		"service_id":   id,
		"version":      info.GetVersion(),
	}).Info("开始注册服务")

	r.lock.Lock()
	// 初始化map
	if r.localServices[name] == nil {
		r.localServices[name] = make(map[string]ServerInfo)
	}
	if r.providers[name] == nil {
		r.providers[name] = make(map[string]ServerInfoProvider)
	}

	// 保存本地服务信息和提供者
	r.localServices[name][id] = info.Clone()
	r.providers[name][id] = provider
	r.lock.Unlock()

	r.lg.WithFields(logrus.Fields{
		"service_name": name,
		"service_id":   id,
	}).Debug("服务信息已保存到本地缓存")

	// 立即注册一次
	err = r.updateServiceToRedis(info, true)
	if err != nil {
		r.lg.WithFields(logrus.Fields{
			"service_name": name,
			"service_id":   id,
			"error":        err,
		}).Error("立即注册服务到Redis失败")
		return err
	}
	// 启动goroutine定时更新服务信息到Redis
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		r.lg.WithFields(logrus.Fields{
			"service_name": name,
			"service_id":   id,
			"interval":     r.expire / 2,
		}).Debug("启动服务定时更新协程")
		wg.Done()
		ticker := time.NewTicker(r.expire / 2) // 更新频率为过期时间的一半
		defer func() {
			ticker.Stop()
			// 服务关闭，清理资源
			r.lg.WithFields(logrus.Fields{
				"service_name": name,
				"service_id":   id,
			}).Info("服务提供者关闭，开始清理资源")
			r.unregisterService(name, id)
		}()
		for {
			select {
			case currentInfo := <-provider.Update():
				_ = r.updateService(name, id, currentInfo)
			case <-ticker.C:
				// 定时更新服务信息到Redis
				_ = r.updateService(name, id, provider.ServerInfo())
			case <-provider.Done():
				r.lg.WithField("name", name).Info("provider 主动关闭")
				return
			case <-r.done:
				return
			}
		}
	}()
	wg.Wait()
	r.lg.WithFields(logrus.Fields{
		"service_name": name,
		"service_id":   id,
	}).Info("服务注册成功")
	return nil
}

func (r *RedisDiscovery) updateService(name, id string, info ServerInfo) error {
	if info == nil {
		return nil
	}
	// 更新本地缓存
	r.lock.Lock()
	versionChange := r.localServices[name][id].GetVersion() != info.GetVersion()
	r.localServices[name][id] = info.Clone()
	r.lock.Unlock()
	r.lg.WithFields(logrus.Fields{
		"service_name": name,
		"service_id":   id,
		"version":      info.GetVersion(),
	}).Debug("更新服务信息到Redis")
	err := r.updateServiceToRedis(info, versionChange)
	if err != nil {
		r.lg.WithFields(logrus.Fields{
			"service_name": name,
			"service_id":   id,
			"error":        err,
		}).Error("更新服务信息到Redis失败")
	}
	return nil
}

func (r *RedisDiscovery) updateServiceToRedis(info ServerInfo, versionChange bool) error {
	key := r.key(info.GetName())
	ctx := context.Background()

	r.lg.WithFields(logrus.Fields{
		"service_name": info.GetName(),
		"service_id":   info.GetID(),
		"version":      info.GetVersion(),
		"redis_key":    key,
	}).Debug("开始更新服务信息到Redis")

	// 序列化服务信息
	data, err := r.marshaler.Marshal(info)
	if err != nil {
		r.lg.WithFields(logrus.Fields{
			"service_name": info.GetName(),
			"service_id":   info.GetID(),
			"error":        err,
		}).Error("序列化服务信息失败")
		return fmt.Errorf("marshal server info failed: %w", err)
	}

	// 使用pipeline合并写入和过期时间设置
	pipe := r.client.Pipeline()
	pipe.HSet(ctx, key, info.GetID(), data)
	pipe.HExpire(ctx, key, r.expire, info.GetID())
	// 执行pipeline
	_, err = pipe.Exec(ctx)
	if err != nil {
		r.lg.WithFields(logrus.Fields{
			"service_name": info.GetName(),
			"service_id":   info.GetID(),
			"redis_key":    key,
			"error":        err,
		}).Error("Redis pipeline执行失败")
		return fmt.Errorf("redis pipeline failed: %w", err)
	}

	r.lg.WithFields(logrus.Fields{
		"service_name": info.GetName(),
		"service_id":   info.GetID(),
		"redis_key":    key,
		"expire_time":  r.expire,
	}).Debug("服务信息已更新到Redis")

	// 发布服务变化通知
	if versionChange {
		r.publishServiceChange(info.GetName(), "update")
	}

	return nil
}

func (r *RedisDiscovery) unregisterService(name, id string) {
	key := r.key(name)
	ctx := context.Background()

	r.lg.WithFields(logrus.Fields{
		"service_name": name,
		"service_id":   id,
		"redis_key":    key,
	}).Info("开始注销服务")

	// 从Redis中删除服务信息
	err := r.client.HDel(ctx, key, id).Err()
	if err != nil {
		r.lg.WithFields(logrus.Fields{
			"service_name": name,
			"service_id":   id,
			"redis_key":    key,
			"error":        err,
		}).Error("从Redis删除服务信息失败")
	} else {
		r.lg.WithFields(logrus.Fields{
			"service_name": name,
			"service_id":   id,
			"redis_key":    key,
		}).Debug("服务信息已从Redis删除")
		// 发布服务移除通知
		r.publishServiceChange(name, "remove")
	}

	r.lock.Lock()
	defer r.lock.Unlock()

	// 清理本地缓存
	if r.localServices[name] != nil {
		delete(r.localServices[name], id)
		r.lg.WithFields(logrus.Fields{
			"service_name": name,
			"service_id":   id,
		}).Debug("服务信息已从本地缓存删除")
		if len(r.localServices[name]) == 0 {
			delete(r.localServices, name)
			r.lg.WithFields(logrus.Fields{
				"service_name": name,
			}).Debug("服务名称的所有实例已清理，删除服务名称缓存")
		}
	}

	// 清理提供者
	if r.providers[name] != nil {
		delete(r.providers[name], id)
		r.lg.WithFields(logrus.Fields{
			"service_name": name,
			"service_id":   id,
		}).Debug("服务提供者已清理")
		if len(r.providers[name]) == 0 {
			delete(r.providers, name)
			r.lg.WithFields(logrus.Fields{
				"service_name": name,
			}).Debug("服务名称的所有提供者已清理，删除提供者缓存")
		}
	}

	r.lg.WithFields(logrus.Fields{
		"service_name": name,
		"service_id":   id,
	}).Info("服务注销完成")
}
