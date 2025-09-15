package rdsd

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// 创建测试用的Redis客户端
func setupTestRedis(t *testing.T) *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   1, // 使用测试数据库
	})

	// 测试连接
	ctx := context.Background()
	err := client.Ping(ctx).Err()
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}

	// 清空测试数据库
	client.FlushDB(ctx)

	return client
}

// 创建测试用的RedisDiscovery
func setupTestDiscovery(t *testing.T) (*RedisDiscovery, *redis.Client) {
	client := setupTestRedis(t)
	marshaler := NewJSONMarshaller(func() ServerInfo {
		return &testServerInfo{}
	})
	// lg := logrus.New()
	// lg.SetReportCaller(true)
	// lg.SetLevel(logrus.DebugLevel)
	lg := logrus.NewEntry(logrus.New())
	lg.Level = logrus.DebugLevel
	discovery := NewRedisDiscovery(client, marshaler, lg)
	return discovery, client
}

func TestRedisDiscovery_Register(t *testing.T) {
	discovery, client := setupTestDiscovery(t)
	defer client.Close()

	// 创建测试服务提供者
	provider := newTestServerInfoProvider("service1", "test-service", "v1.0.0")
	defer provider.Close()

	// 注册服务
	err := discovery.Register(provider)
	require.NoError(t, err)

	// 验证本地服务列表
	localServices := discovery.LocalServers()
	assert.Len(t, localServices, 1)
	assert.Equal(t, "service1", localServices[0].GetID())
	assert.Equal(t, "test-service", localServices[0].GetName())
	assert.Equal(t, "v1.0.0", localServices[0].GetVersion())

	// 等待一段时间确保数据写入Redis
	time.Sleep(100 * time.Millisecond)

	// 验证Redis中的数据
	ctx := context.Background()
	key := "rdsd:service:test-service"
	result, err := client.HGet(ctx, key, "service1").Result()
	require.NoError(t, err)
	assert.NotEmpty(t, result)
}

func TestRedisDiscovery_GetServer(t *testing.T) {
	discovery, client := setupTestDiscovery(t)
	defer client.Close()

	// 添加监听器以触发扫描
	listener := newTestListener("test-service")
	discovery.AddListener(listener)

	// 直接向Redis写入测试数据
	ctx := context.Background()
	testInfo := &testServerInfo{
		ID:         "service1",
		Name:       "test-service",
		LastUpdate: time.Now().Unix(),
		VersionNum: "v1.0.0",
	}

	marshaler := NewJSONMarshaller(func() ServerInfo {
		return &testServerInfo{}
	})
	data, err := marshaler.Marshal(testInfo)
	require.NoError(t, err)

	key := "rdsd:service:test-service"
	err = client.HSet(ctx, key, "service1", data).Err()
	require.NoError(t, err)

	// 手动触发服务同步
	discovery.SyncServers()

	// 等待扫描器发现服务（扫描间隔为10秒）
	// time.Sleep(11 * time.Second)

	// 测试GetServer
	info := discovery.GetServer("test-service", "service1")
	assert.NotNil(t, info)
	assert.Equal(t, "service1", info.GetID())
	assert.Equal(t, "test-service", info.GetName())
	assert.Equal(t, "v1.0.0", info.GetVersion())

	// 测试不存在的服务
	info = discovery.GetServer("test-service", "nonexistent")
	assert.Nil(t, info)

	info = discovery.GetServer("nonexistent-service", "service1")
	assert.Nil(t, info)
}

func TestRedisDiscovery_GetServers(t *testing.T) {
	discovery, client := setupTestDiscovery(t)
	defer client.Close()

	// 添加监听器以触发扫描
	listener := newTestListener("test-service")
	discovery.AddListener(listener)

	// 直接向Redis写入多个测试数据
	ctx := context.Background()
	key := "rdsd:service:test-service"

	marshaler := NewJSONMarshaller(func() ServerInfo {
		return &testServerInfo{}
	})

	// 添加多个服务实例
	for i := 1; i <= 3; i++ {
		testInfo := &testServerInfo{
			ID:         fmt.Sprintf("service%d", i),
			Name:       "test-service",
			LastUpdate: time.Now().Unix(),
			VersionNum: fmt.Sprintf("v1.0.%d", i),
		}

		data, err := marshaler.Marshal(testInfo)
		require.NoError(t, err)

		err = client.HSet(ctx, key, testInfo.GetID(), data).Err()
		require.NoError(t, err)
	}

	// 手动触发服务同步
	discovery.SyncServers()

	// 测试GetServers
	services := discovery.GetServers("test-service")
	assert.Len(t, services, 3)

	// 验证所有服务都被正确获取
	serviceIDs := make(map[string]bool)
	for _, service := range services {
		assert.Equal(t, "test-service", service.GetName())
		serviceIDs[service.GetID()] = true
	}

	assert.True(t, serviceIDs["service1"])
	assert.True(t, serviceIDs["service2"])
	assert.True(t, serviceIDs["service3"])

	// 测试不存在的服务
	services = discovery.GetServers("nonexistent-service")
	assert.Len(t, services, 0)
}

func TestRedisDiscovery_LocalServers(t *testing.T) {
	discovery, client := setupTestDiscovery(t)
	defer client.Close()

	// 初始状态应该没有本地服务
	localServices := discovery.LocalServers()
	assert.Len(t, localServices, 0)

	// 注册多个服务
	providers := make([]*testServerInfoProvider, 3)
	for i := 0; i < 3; i++ {
		providers[i] = newTestServerInfoProvider(
			fmt.Sprintf("service%d", i+1),
			fmt.Sprintf("test-service-%d", i+1),
			fmt.Sprintf("v1.0.%d", i+1),
		)
		err := discovery.Register(providers[i])
		require.NoError(t, err)
	}

	// 验证本地服务列表
	localServices = discovery.LocalServers()
	assert.Len(t, localServices, 3)

	// 关闭一个服务
	providers[0].Close()
	time.Sleep(100 * time.Millisecond)

	// 验证本地服务列表减少
	localServices = discovery.LocalServers()
	assert.Len(t, localServices, 2)

	// 清理剩余服务
	for i := 1; i < 3; i++ {
		providers[i].Close()
	}
}

func TestRedisDiscovery_AddListener(t *testing.T) {
	discovery, client := setupTestDiscovery(t)
	defer client.Close()
	defer discovery.Close()

	// 创建监听器
	listener := newTestListener("test-service")
	discovery.AddListener(listener)

	// 直接向Redis添加服务
	ctx := context.Background()
	key := "rdsd:service:test-service"

	testInfo := &testServerInfo{
		ID:         "service1",
		Name:       "test-service",
		LastUpdate: time.Now().Unix(),
		VersionNum: "v1.0.0",
	}

	marshaler := NewJSONMarshaller(func() ServerInfo {
		return &testServerInfo{}
	})
	data, err := marshaler.Marshal(testInfo)
	require.NoError(t, err)

	err = client.HSet(ctx, key, "service1", data).Err()
	require.NoError(t, err)

	// 手动同步服务发现
	discovery.SyncServers()
	// 验证添加事件
	addEvents := listener.GetAddEvents()
	assert.Len(t, addEvents, 1)
	assert.Equal(t, "service1", addEvents[0].GetID())
	assert.Equal(t, "test-service", addEvents[0].GetName())

	// 更新服务
	listener.ClearEvents()
	testInfo.VersionNum = "v1.0.1"
	testInfo.LastUpdate = time.Now().Unix()
	data, err = marshaler.Marshal(testInfo)
	require.NoError(t, err)

	err = client.HSet(ctx, key, "service1", data).Err()
	require.NoError(t, err)

	// 手动同步服务发现
	discovery.SyncServers()

	// 验证更新事件
	updateEvents := listener.GetUpdateEvents()
	assert.Len(t, updateEvents, 1)
	assert.Equal(t, "v1.0.1", updateEvents[0].GetVersion())

	// 删除服务
	listener.ClearEvents()
	err = client.HDel(ctx, key, "service1").Err()
	require.NoError(t, err)

	// 手动同步服务发现
	discovery.SyncServers()

	// 验证删除事件
	removeEvents := listener.GetRemoveEvents()
	assert.Len(t, removeEvents, 1)
	assert.Equal(t, "service1", removeEvents[0].GetID())
}

func TestRedisDiscovery_ServiceLifecycle(t *testing.T) {
	discovery, client := setupTestDiscovery(t)
	defer client.Close()
	defer discovery.Close()

	// 创建监听器
	listener := newTestListener("test-service")
	discovery.AddListener(listener)

	// 注册服务
	provider := newTestServerInfoProvider("service1", "test-service", "v1.0.0")
	err := discovery.Register(provider)
	require.NoError(t, err)
	// provider.NtfUpdate()
	// 等待服务注册和扫描（扫描间隔为10秒）
	// time.Sleep(11 * time.Second)
	discovery.SyncServers()
	// 验证服务可以被获取
	info := discovery.GetServer("test-service", "service1")
	assert.NotNil(t, info)
	assert.Equal(t, "v1.0.0", info.GetVersion())

	// 验证监听器收到添加事件
	addEvents := listener.GetAddEvents()
	assert.Len(t, addEvents, 1)

	// 更新服务版本
	fmt.Println("provider update --------------------------")
	listener.ClearEvents()
	provider.UpdateVersion("v1.0.1")

	// 等待服务更新到Redis和缓存刷新（定时更新5秒 + 扫描间隔10秒）
	// time.Sleep(12 * time.Second)
	provider.NtfUpdate()
	time.Sleep(time.Second)
	// discovery.SyncServers()
	// 验证服务版本已更新
	info = discovery.GetServer("test-service", "service1")
	assert.NotNil(t, info)
	assert.Equal(t, "v1.0.1", info.GetVersion())

	// 关闭服务
	listener.ClearEvents()
	fmt.Println("provider close --------------------------")
	provider.Close()

	// 等待服务清理（扫描间隔为10秒）
	// time.Sleep(11 * time.Second)
	// discovery.SyncServers()
	time.Sleep(time.Second)
	// 验证服务已被移除
	info = discovery.GetServer("test-service", "service1")
	assert.Nil(t, info)

	// 验证本地服务列表为空
	localServices := discovery.LocalServers()
	assert.Len(t, localServices, 0)

	// 验证监听器收到删除事件
	removeEvents := listener.GetRemoveEvents()
	assert.Len(t, removeEvents, 1)
}

// TestRedisDiscovery_PubSubNotification 测试Redis pub/sub通知功能
func TestRedisDiscovery_PubSubNotification(t *testing.T) {
	discovery1, client := setupTestDiscovery(t)
	defer client.Close()
	defer discovery1.Close()

	// 创建第二个discovery实例来模拟分布式环境
	marshaler := NewJSONMarshaller(func() ServerInfo {
		return &testServerInfo{}
	})
	lg := logrus.NewEntry(logrus.New())
	lg.Level = logrus.DebugLevel
	discovery2 := NewRedisDiscovery(client, marshaler, lg)
	defer discovery2.Close()

	// 在discovery2上添加监听器
	listener := newTestListener("test-service")
	discovery2.AddListener(listener)

	// 等待pub/sub连接建立
	// time.Sleep(100 * time.Millisecond)

	// 在discovery1上注册服务（这会触发pub/sub通知）
	provider := newTestServerInfoProvider("service1", "test-service", "v1.0.0")
	defer provider.Close()
	err := discovery1.Register(provider)
	require.NoError(t, err)

	// 等待pub/sub通知传播和处理
	time.Sleep(100 * time.Millisecond)

	// 验证discovery2通过pub/sub通知感知到了服务变化
	info := discovery2.GetServer("test-service", "service1")
	assert.NotNil(t, info, "discovery2应该通过pub/sub通知感知到服务注册")
	if info != nil {
		assert.Equal(t, "v1.0.0", info.GetVersion())
	}

	// 验证监听器收到添加事件
	addEvents := listener.GetAddEvents()
	assert.Len(t, addEvents, 1, "监听器应该收到服务添加事件")
	if len(addEvents) > 0 {
		assert.Equal(t, "service1", addEvents[0].GetID())
	}

	// 测试服务注销的pub/sub通知
	listener.ClearEvents()
	provider.Close()

	// 等待服务注销和pub/sub通知传播
	time.Sleep(200 * time.Millisecond)

	// 验证discovery2感知到了服务移除
	info = discovery2.GetServer("test-service", "service1")
	assert.Nil(t, info, "discovery2应该通过pub/sub通知感知到服务注销")

	// 验证监听器收到删除事件
	removeEvents := listener.GetRemoveEvents()
	assert.Len(t, removeEvents, 1, "监听器应该收到服务删除事件")
	if len(removeEvents) > 0 {
		assert.Equal(t, "service1", removeEvents[0].GetID())
	}
}

// TestRedisDiscovery_Close 测试Close方法
func TestRedisDiscovery_Close(t *testing.T) {
	discovery, client := setupTestDiscovery(t)
	defer client.Close()

	// 注册一个服务
	provider := newTestServerInfoProvider("service1", "test-service", "v1.0.0")
	defer provider.Close()
	err := discovery.Register(provider)
	require.NoError(t, err)

	// 验证服务正常工作
	localServices := discovery.LocalServers()
	assert.Len(t, localServices, 1)

	// 关闭discovery
	err = discovery.Close()
	assert.NoError(t, err, "Close方法应该成功执行")

	// 再次调用Close应该不会出错
	err = discovery.Close()
	assert.NoError(t, err, "重复调用Close应该不会出错")
}
