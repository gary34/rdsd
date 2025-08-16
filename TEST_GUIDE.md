# Discovery 接口测试指南

## 测试概述

本项目为 `Discovery` 接口提供了完整的测试用例，涵盖了服务注册、发现、监听等核心功能。

## 测试文件结构

### redis_test.go

包含以下测试组件：

#### 测试辅助类型

1. **testServerInfo**: 实现 `ServerInfo` 接口的测试结构体
2. **testServerInfoProvider**: 实现 `ServerInfoProvider` 接口的测试结构体
3. **testListener**: 实现 `Listener` 接口的测试结构体

#### 测试用例

1. **TestRedisDiscovery_Register**
   - 测试服务注册功能
   - 验证本地服务列表
   - 验证Redis中的数据存储

2. **TestRedisDiscovery_GetServer**
   - 测试单个服务查询功能
   - 验证存在和不存在的服务查询

3. **TestRedisDiscovery_GetServers**
   - 测试批量服务查询功能
   - 验证多个服务实例的正确返回

4. **TestRedisDiscovery_LocalServers**
   - 测试本地服务列表功能
   - 验证服务注册和注销对本地列表的影响

5. **TestRedisDiscovery_AddListener**
   - 测试事件监听功能
   - 验证服务添加、更新、删除事件的正确触发

6. **TestRedisDiscovery_ServiceLifecycle**
   - 测试完整的服务生命周期
   - 包括注册、更新、监听、注销的完整流程

## 运行测试

### 前置条件

1. **启动Redis服务器**：
   ```bash
   # 使用Docker（推荐）
   docker run -d -p 6379:6379 redis:latest
   
   # 或本地安装的Redis
   redis-server
   ```

2. **安装依赖**：
   ```bash
   go mod tidy
   ```

### 执行测试

```bash
# 运行所有测试
go test -v

# 运行特定测试
go test -v -run TestRedisDiscovery_Register

# 运行测试并显示覆盖率
go test -v -cover

# 运行测试多次（压力测试）
go test -v -count=10
```

### 测试输出说明

- **PASS**: 测试通过
- **SKIP**: 测试跳过（通常是因为Redis不可用）
- **FAIL**: 测试失败

## 测试特性

### 自动跳过机制

如果Redis服务器不可用，测试会自动跳过而不是失败：

```
--- SKIP: TestRedisDiscovery_Register (0.15s)
    redis_test.go:162: Redis not available: dial tcp [::1]:6379: connectex: No connection could be made because the target machine actively refused it.
```

### 测试隔离

- 每个测试使用独立的Redis数据库（DB 1）
- 测试开始前自动清空测试数据库
- 测试结束后自动清理资源

### 并发安全测试

测试用例包含了并发操作的验证：
- 多个服务同时注册
- 并发的服务查询
- 同时进行的事件监听

## 测试覆盖的功能点

### Discovery 接口方法

- ✅ `GetServer(serviceName, serverID string) ServerInfo`
- ✅ `GetServers(serviceName string) []ServerInfo`
- ✅ `Register(provider ServerInfoProvider) error`
- ✅ `LocalServers() []ServerInfo`
- ✅ `AddListener(listener Listener)`

### 核心功能

- ✅ 服务注册和自动清理
- ✅ 服务发现和缓存
- ✅ 事件监听和通知
- ✅ 并发安全保护
- ✅ Redis数据持久化
- ✅ 服务生命周期管理

### 边界条件

- ✅ 不存在的服务查询
- ✅ 空服务列表处理
- ✅ 服务提供者异常退出
- ✅ Redis连接异常处理

## 扩展测试

如需添加新的测试用例，可以参考现有测试的结构：

```go
func TestYourNewFeature(t *testing.T) {
    discovery, client := setupTestDiscovery(t)
    defer client.Close()
    
    // 你的测试逻辑
    
    // 使用 assert 和 require 进行断言
    assert.Equal(t, expected, actual)
    require.NoError(t, err)
}
```

## 故障排除

### 常见问题

1. **Redis连接失败**
   - 确保Redis服务器正在运行
   - 检查端口6379是否被占用
   - 验证防火墙设置

2. **测试超时**
   - 增加测试中的等待时间
   - 检查Redis服务器性能

3. **并发测试失败**
   - 检查锁的使用是否正确
   - 验证资源清理是否完整

### 调试技巧

```bash
# 启用详细日志
go test -v -args -test.v

# 运行单个测试并显示详细输出
go test -v -run TestRedisDiscovery_Register -args -test.v
```