package rdsd

// ServerInfo 服务信息接口
type ServerInfo interface {
	// GetID 获取服务ID
	GetID() string
	// GetName 获取服务名称
	GetName() string
	// LastUpdateTime 获取最后更新时间
	LastUpdateTime() int64
	//版本号
	Version() string
}

type ServerInfoProvider interface {
	// 获取最新的服务信息
	ServerInfo() ServerInfo
	// 服务信息关闭通道
	Done() <-chan struct{}
}

// Discovery 服务发现接口，提供服务注册、查询和监听功能
type Discovery interface {
	// GetServer 根据服务名称和ID获取指定的服务信息
	GetServer(name, id string) (info ServerInfo)
	// GetServers 根据服务名称获取所有相关的服务列表
	GetServers(name string) (list []ServerInfo)
	// Register 注册服务信息到发现服务中
	Register(provider ServerInfoProvider) (err error)
	// LocalServers 获取通过Register注册的本地服务信息
	LocalServers() (list []ServerInfo)
	// AddListener 添加服务变化监听器
	AddListener(l Listener)
}

// Listener 服务变化监听器接口，用于监听服务的添加和移除事件
type Listener interface {
	// WatchNames 需要监听的服务名称列表
	WatchNames() (names []string)
	// OnAdd 当有新服务添加时触发的回调方法
	OnAdd(ServerInfo)
	// OnRemove 当服务被移除时触发的回调方法
	OnRemove(ServerInfo)
	// OnUpdate 当服务更新时触发的回调方法
	OnUpdate(ServerInfo)
}
