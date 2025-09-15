package rdsd

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

type EBServerInfo struct {
	BaseServerInfo `json:",inline"`
	// Addr 服务地址，格式为 host:port
	Addr string `json:"addr"`
	// 负载
	Load int `json:"load"`
	// 权重
	Power int `json:"power"`
	// 排序
	Order int `json:"order"`
}

var _ ServerInfo = (*EBServerInfo)(nil)

func (b *EBServerInfo) Clone() ServerInfo {
	return &EBServerInfo{
		BaseServerInfo: b.BaseServerInfo,
		Addr:           b.Addr,
	}
}

func NewEBServerInfo() *EBServerInfo {
	return &EBServerInfo{}
}

type EBDiscovery struct {
	lg *logrus.Entry
	Discovery
}

func NewEBDiscovery(client redis.UniversalClient, lg *logrus.Entry) *EBDiscovery {
	return &EBDiscovery{
		lg: lg,
		Discovery: NewRedisDiscovery(client, NewJSONMarshaller(func() ServerInfo {
			return NewEBServerInfo()
		}), lg),
	}
}

func (sd *EBDiscovery) GetServer(name, id string) (info *EBServerInfo) {
	s := sd.Discovery.GetServer(name, id)
	if s == nil {
		return nil
	}
	return s.(*EBServerInfo)
}

func (sd *EBDiscovery) GetServers(name string) (list []*EBServerInfo) {
	for _, s := range sd.Discovery.GetServers(name) {
		list = append(list, s.(*EBServerInfo))
	}
	return list
}

func (sd *EBDiscovery) AllocService(name string, lbs ...EBLoadBalancer) (info *EBServerInfo) {
	lb := defaultBalancer
	if len(lbs) > 0 {
		lb = lbs[0]
	}
	list, _ := sd.GetServicesByName(name)
	if len(list) == 0 {
		return nil
	}
	return lb.Select(list)
}

func (sd *EBDiscovery) CurrentService() (info *EBServerInfo) {
	for _, serverInfo := range sd.LocalServers() {
		return serverInfo.(*EBServerInfo)
	}
	return nil
}

func (sd *EBDiscovery) Deregister(ctx context.Context) (err error) {
	return sd.Close()
}

func (sd *EBDiscovery) GetServicesByName(name string) (list []*EBServerInfo, err error) {
	//for _, serverInfo := range sd.GetServers(name) {
	//	list = append(list, serverInfo.(*EBServerInfo))
	//}
	//return list, nil
	return sd.GetServers(name), nil
}

func (sd *EBDiscovery) WaitService(name ...string) (done chan struct{}) {
	done = make(chan struct{})
	defer func() {
		close(done)
	}()
	if len(name) == 0 {
		return
	}
	wg := &sync.WaitGroup{}
	check := func(name string) (ok bool) {
		list, _ := sd.GetServicesByName(name)
		if len(list) > 0 {
			sd.lg.WithField("service", name).Info("wait service ok")
			return true
		}
		return false
	}
	wg.Add(len(name))
	for _, n := range name {
		go func(name string, wg *sync.WaitGroup) {
			defer wg.Done()
			for {
				if check(name) {
					return
				}
				time.Sleep(time.Second)
			}
		}(n, wg)
	}
	wg.Wait()
	return
}
