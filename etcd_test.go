package rdsd

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// 创建测试用的etcd客户端
func setupTestEtcd(t *testing.T) *clientv3.Client {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Skipf("Failed to connect to etcd: %v", err)
	}

	// 测试连接
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	_, err = client.Status(ctx, "127.0.0.1:2379")
	if err != nil {
		client.Close()
		t.Skipf("Etcd not available: %v", err)
	}

	// 清空测试数据
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	_, err = client.Delete(ctx, "/rdsd/", clientv3.WithPrefix())
	if err != nil {
		t.Logf("Warning: failed to clean test data: %v", err)
	}

	return client
}

// 创建测试用的EtcdDiscovery
func setupTestEtcdDiscovery(t *testing.T) (*EtcdDiscovery, *clientv3.Client) {
	client := setupTestEtcd(t)
	marshaler := NewJSONMarshaller(func() ServerInfo {
		return &testServerInfo{}
	})
	logger := logrus.NewEntry(logrus.StandardLogger())
	logger.Logger.SetLevel(logrus.DebugLevel)

	discovery := NewEtcdDiscovery(client, marshaler, "/rdsd", logger)
	return discovery, client
}

// 测试租约过期
func TestEtcdDiscovery_Lease(t *testing.T) {
	discovery, client := setupTestEtcdDiscovery(t)
	defer client.Close()
	defer discovery.Close()
	discovery1, _ := setupTestEtcdDiscovery(t)
	defer discovery1.Close()
	// 创建测试服务提供者
	provider := newTestServerInfoProvider("server1", "test-service", "v1.0")
	defer provider.Close()
	// 创建监听器
	listener := newTestListener("test-service")
	discovery1.AddListener(listener)
	// 注册服务
	err := discovery.Register(provider)
	require.NoError(t, err)
	time.Sleep(time.Second * 10)
	list := discovery1.GetServers("test-service")
	assert.Len(t, list, 1)
	assert.Equal(t, "server1", list[0].GetID())
}

func TestEtcdDiscovery_Register(t *testing.T) {
	discovery, client := setupTestEtcdDiscovery(t)
	defer client.Close()
	defer discovery.Close()

	// 创建测试服务提供者
	provider := newTestServerInfoProvider("server1", "test-service", "v1.0")
	defer provider.Close()

	// 注册服务
	err := discovery.Register(provider)
	require.NoError(t, err)

	// 等待服务注册到etcd
	time.Sleep(100 * time.Millisecond)

	// 验证本地服务列表
	localServers := discovery.LocalServers()
	assert.Len(t, localServers, 1)
	assert.Equal(t, "server1", localServers[0].GetID())
	assert.Equal(t, "test-service", localServers[0].GetName())

	// 验证etcd中的数据
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := client.Get(ctx, "/rdsd/service/test-service/server1")
	require.NoError(t, err)
	assert.Len(t, resp.Kvs, 1)
}

func TestEtcdDiscovery_GetServer(t *testing.T) {
	discovery, client := setupTestEtcdDiscovery(t)
	defer client.Close()
	defer discovery.Close()

	// 等待监听器初始化
	time.Sleep(100 * time.Millisecond)

	// 创建并注册服务
	provider := newTestServerInfoProvider("server1", "test-service", "v1.0")
	defer provider.Close()
	err := discovery.Register(provider)
	require.NoError(t, err)

	// 等待服务注册和watch事件处理
	time.Sleep(time.Second)

	// 测试获取服务
	server := discovery.GetServer("test-service", "server1")
	require.NotNil(t, server)
	assert.Equal(t, "server1", server.GetID())
	assert.Equal(t, "test-service", server.GetName())
	assert.Equal(t, "v1.0", server.GetVersion())

	// 测试获取不存在的服务
	server = discovery.GetServer("test-service", "nonexistent")
	assert.Nil(t, server)

	// 测试获取不存在服务名的服务
	server = discovery.GetServer("nonexistent-service", "server1")
	assert.Nil(t, server)
}

func TestEtcdDiscovery_GetServers(t *testing.T) {
	discovery, client := setupTestEtcdDiscovery(t)
	defer client.Close()
	defer discovery.Close()

	//// 创建监听器
	//listener := newTestListener("test-service")
	//discovery.AddListener(listener)

	// 等待监听器初始化
	//time.Sleep(100 * time.Millisecond)

	// 注册多个服务
	provider1 := newTestServerInfoProvider("server1", "test-service", "v1.0")
	provider2 := newTestServerInfoProvider("server2", "test-service", "v1.1")
	provider3 := newTestServerInfoProvider("server3", "other-service", "v1.0")
	defer provider1.Close()
	defer provider2.Close()
	defer provider3.Close()

	err := discovery.Register(provider1)
	require.NoError(t, err)
	err = discovery.Register(provider2)
	require.NoError(t, err)
	err = discovery.Register(provider3)
	require.NoError(t, err)

	// 等待服务注册和watch事件处理
	time.Sleep(100 * time.Millisecond)

	// 测试获取test-service的所有服务
	servers := discovery.GetServers("test-service")
	assert.Len(t, servers, 2)

	// 验证服务信息
	serverIDs := make([]string, len(servers))
	for i, server := range servers {
		serverIDs[i] = server.GetID()
		assert.Equal(t, "test-service", server.GetName())
	}
	assert.Contains(t, serverIDs, "server1")
	assert.Contains(t, serverIDs, "server2")

	// 测试获取不存在的服务
	servers = discovery.GetServers("nonexistent-service")
	assert.Len(t, servers, 0)
}

func TestEtcdDiscovery_LocalServers(t *testing.T) {
	discovery, client := setupTestEtcdDiscovery(t)
	defer client.Close()
	defer discovery.Close()

	// 初始状态应该没有本地服务
	localServers := discovery.LocalServers()
	assert.Len(t, localServers, 0)

	// 注册服务
	provider1 := newTestServerInfoProvider("server1", "service1", "v1.0")
	provider2 := newTestServerInfoProvider("server2", "service2", "v1.0")
	defer provider1.Close()
	defer provider2.Close()

	err := discovery.Register(provider1)
	require.NoError(t, err)
	err = discovery.Register(provider2)
	require.NoError(t, err)

	// 验证本地服务列表
	localServers = discovery.LocalServers()
	assert.Len(t, localServers, 2)

	serverNames := make([]string, len(localServers))
	for i, server := range localServers {
		serverNames[i] = server.GetName()
	}
	assert.Contains(t, serverNames, "service1")
	assert.Contains(t, serverNames, "service2")
}

func TestEtcdDiscovery_AddListener(t *testing.T) {
	discovery, client := setupTestEtcdDiscovery(t)
	defer client.Close()
	defer discovery.Close()
	discovery1, _ := setupTestEtcdDiscovery(t)
	//defer client.Close()
	defer discovery1.Close()
	// 创建监听器
	listener := newTestListener("test-service")
	discovery1.AddListener(listener)

	// 等待监听器初始化
	time.Sleep(100 * time.Millisecond)

	// 注册服务
	provider := newTestServerInfoProvider("server1", "test-service", "v1.0")
	defer provider.Close()
	err := discovery.Register(provider)
	require.NoError(t, err)

	// 等待watch事件处理
	time.Sleep(200 * time.Millisecond)

	// 验证监听器收到添加事件
	addEvents := listener.GetAddEvents()
	assert.Len(t, addEvents, 1)
	assert.Equal(t, "server1", addEvents[0].GetID())
	assert.Equal(t, "test-service", addEvents[0].GetName())

	// 更新服务版本
	provider.UpdateVersion("v2.0")
	provider.NtfUpdate()

	// 等待更新事件处理
	time.Sleep(200 * time.Millisecond)

	// 验证监听器收到更新事件
	updateEvents := listener.GetUpdateEvents()
	assert.GreaterOrEqual(t, len(updateEvents), 1)

	// 关闭服务提供者（模拟服务下线）
	provider.Close()

	// 等待lease过期和删除事件处理
	time.Sleep(7 * time.Second) // 等待lease过期

	// 验证监听器收到删除事件
	removeEvents := listener.GetRemoveEvents()
	assert.Len(t, removeEvents, 1)
	assert.Equal(t, "server1", removeEvents[0].GetID())
}

func TestEtcdDiscovery_ServiceLifecycle(t *testing.T) {
	discovery, client := setupTestEtcdDiscovery(t)
	defer client.Close()
	defer discovery.Close()

	// 创建监听器
	listener := newTestListener("test-service")
	discovery.AddListener(listener)

	// 等待监听器初始化
	time.Sleep(100 * time.Millisecond)

	// 注册服务
	provider := newTestServerInfoProvider("server1", "test-service", "v1.0")
	err := discovery.Register(provider)
	require.NoError(t, err)

	// 等待服务注册
	time.Sleep(200 * time.Millisecond)

	// 验证服务可以被获取
	server := discovery.GetServer("test-service", "server1")
	require.NotNil(t, server)
	assert.Equal(t, "v1.0", server.GetVersion())

	// 更新服务信息
	provider.UpdateVersion("v2.0")
	provider.NtfUpdate()

	// 等待更新处理
	time.Sleep(200 * time.Millisecond)

	// 验证服务信息已更新
	server = discovery.GetServer("test-service", "server1")
	require.NotNil(t, server)
	assert.Equal(t, "v2.0", server.GetVersion())

	// 关闭服务提供者
	provider.Close()

	// 等待lease过期
	time.Sleep(7 * time.Second)

	// 验证服务已被删除
	server = discovery.GetServer("test-service", "server1")
	assert.Nil(t, server)

	// 验证本地服务列表为空
	localServers := discovery.LocalServers()
	assert.Len(t, localServers, 0)
}

func TestEtcdDiscovery_WatchEvents(t *testing.T) {
	discovery, client := setupTestEtcdDiscovery(t)
	defer client.Close()
	defer discovery.Close()

	// 创建监听器
	listener := newTestListener("test-service")
	discovery.AddListener(listener)

	// 等待监听器初始化
	time.Sleep(100 * time.Millisecond)

	// 直接向etcd写入数据（模拟外部服务注册）
	marshaler := NewJSONMarshaller(func() ServerInfo {
		return &testServerInfo{}
	})
	testInfo := &testServerInfo{
		ID:         "external-server",
		Name:       "test-service",
		LastUpdate: time.Now().Unix(),
		VersionNum: "v1.0",
	}

	data, err := marshaler.Marshal(testInfo)
	require.NoError(t, err)

	// 创建lease并写入数据
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	leaseResp, err := client.Grant(ctx, 10)
	require.NoError(t, err)

	_, err = client.Put(ctx, "/rdsd/service/test-service/external-server", string(data), clientv3.WithLease(leaseResp.ID))
	require.NoError(t, err)

	// 等待watch事件处理
	time.Sleep(200 * time.Millisecond)

	// 验证监听器收到添加事件
	addEvents := listener.GetAddEvents()
	assert.Len(t, addEvents, 1)
	assert.Equal(t, "external-server", addEvents[0].GetID())

	// 验证可以通过Discovery获取到该服务
	server := discovery.GetServer("test-service", "external-server")
	require.NotNil(t, server)
	assert.Equal(t, "external-server", server.GetID())

	// 删除服务
	_, err = client.Delete(ctx, "/rdsd/service/test-service/external-server")
	require.NoError(t, err)

	// 等待删除事件处理
	time.Sleep(200 * time.Millisecond)

	// 验证监听器收到删除事件
	removeEvents := listener.GetRemoveEvents()
	assert.Len(t, removeEvents, 1)
	assert.Equal(t, "external-server", removeEvents[0].GetID())

	// 验证服务已被删除
	server = discovery.GetServer("test-service", "external-server")
	assert.Nil(t, server)
}

func TestEtcdDiscovery_SyncServers(t *testing.T) {
	discovery, client := setupTestEtcdDiscovery(t)
	defer client.Close()
	defer discovery.Close()

	// 先向etcd写入一些数据
	marshaler := NewJSONMarshaller(func() ServerInfo {
		return &testServerInfo{}
	})
	testInfo := &testServerInfo{
		ID:         "existing-server",
		Name:       "test-service",
		LastUpdate: time.Now().Unix(),
		VersionNum: "v1.0",
	}

	data, err := marshaler.Marshal(testInfo)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	leaseResp, err := client.Grant(ctx, 10)
	require.NoError(t, err)

	_, err = client.Put(ctx, "/rdsd/service/test-service/existing-server", string(data), clientv3.WithLease(leaseResp.ID))
	require.NoError(t, err)

	// 创建监听器（这会触发同步）
	listener := newTestListener("test-service")
	discovery.AddListener(listener)

	// 等待同步完成
	time.Sleep(200 * time.Millisecond)

	// 验证服务已被同步到缓存
	server := discovery.GetServer("test-service", "existing-server")
	require.NotNil(t, server)
	assert.Equal(t, "existing-server", server.GetID())

	// 手动触发同步
	discovery.SyncServers()

	// 等待同步完成
	time.Sleep(200 * time.Millisecond)

	// 验证服务仍然存在
	server = discovery.GetServer("test-service", "existing-server")
	require.NotNil(t, server)
	assert.Equal(t, "existing-server", server.GetID())
}

func TestEtcdDiscovery_Close(t *testing.T) {
	discovery, client := setupTestEtcdDiscovery(t)
	defer client.Close()

	// 注册服务
	provider := newTestServerInfoProvider("server1", "test-service", "v1.0")
	defer provider.Close()
	err := discovery.Register(provider)
	require.NoError(t, err)

	// 等待服务注册
	time.Sleep(200 * time.Millisecond)

	// 验证服务存在于etcd中
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := client.Get(ctx, "/rdsd/service/test-service/server1")
	require.NoError(t, err)
	assert.Len(t, resp.Kvs, 1)

	// 关闭Discovery
	err = discovery.Close()
	assert.NoError(t, err)

	// 等待清理完成
	time.Sleep(200 * time.Millisecond)

	// 验证服务已从etcd中删除
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err = client.Get(ctx, "/rdsd/service/test-service/server1")
	require.NoError(t, err)
	assert.Len(t, resp.Kvs, 0)

	// 验证本地服务列表为空
	localServers := discovery.LocalServers()
	assert.Len(t, localServers, 0)
}

func TestEtcdDiscovery_ConcurrentOperations(t *testing.T) {
	discovery, client := setupTestEtcdDiscovery(t)
	defer client.Close()
	defer discovery.Close()

	// 创建监听器
	listener := newTestListener("test-service")
	discovery.AddListener(listener)

	// 等待监听器初始化
	time.Sleep(100 * time.Millisecond)

	// 并发注册多个服务
	var wg sync.WaitGroup
	providers := make([]*testServerInfoProvider, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			provider := newTestServerInfoProvider(fmt.Sprintf("server%d", index), "test-service", "v1.0")
			providers[index] = provider
			err := discovery.Register(provider)
			assert.NoError(t, err)
		}(i)
	}

	wg.Wait()

	// 等待所有服务注册完成
	time.Sleep(500 * time.Millisecond)

	// 验证所有服务都已注册
	localServers := discovery.LocalServers()
	assert.Len(t, localServers, 10)

	servers := discovery.GetServers("test-service")
	assert.Len(t, servers, 10)

	// 清理
	for _, provider := range providers {
		if provider != nil {
			provider.Close()
		}
	}
}
