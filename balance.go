package ebsd

import (
	"slices"
	"sync/atomic"
)

type EBLoadBalancer interface {
	Select(list []*EBServerInfo) (info *EBServerInfo)
} // Deregister 注销服务

type EBLoadBalancerFunc func(list []*EBServerInfo) (info *EBServerInfo)

func (f EBLoadBalancerFunc) Select(list []*EBServerInfo) (info *EBServerInfo) {
	return f(list)
}

var defaultBalancer EBLoadBalancer = NewFirstBalancer()

func NewFirstBalancer() EBLoadBalancerFunc {
	return func(list []*EBServerInfo) (info *EBServerInfo) {
		if len(list) > 0 {
			return list[0]
		}
		return nil
	}
}

type roundRobinBalancer struct {
	idx atomic.Int32
}

func (r *roundRobinBalancer) Select(list []*EBServerInfo) (info *EBServerInfo) {
	if len(list) == 0 {
		return nil
	}
	idx := r.idx.Add(1) - 1
	idx %= int32(len(list))
	return list[idx]
}

func NewRoundRobinBalancer() EBLoadBalancer {
	return &roundRobinBalancer{}
}

// BalanceSortByType 排序类型
type BalanceSortByType = uint8

const (
	BalanceSortByTypeLoad  BalanceSortByType = 0
	BalanceSortByTypePower BalanceSortByType = 1
	BalanceSortByTypeOrder BalanceSortByType = 2
)

type BalanceSortOrderType = uint8

const (
	BalanceSortOrderTypeAsc  = 0
	BalanceSortOrderTypeDesc = 1
)

type BalanceSorter struct {
	By    BalanceSortByType
	Order BalanceSortOrderType
}

func NewBalanceSorter(by BalanceSortByType, order BalanceSortOrderType) BalanceSorter {
	return BalanceSorter{
		By:    by,
		Order: order,
	}
}

func (s BalanceSorter) SortValue(info *EBServerInfo) int {
	switch s.By {
	case BalanceSortByTypeLoad:
		return info.Load
	case BalanceSortByTypePower:
		return info.Power
	case BalanceSortByTypeOrder:
		return info.Order
	}
	return 0
}

func (s BalanceSorter) Compare(a, b *EBServerInfo) int {
	aVal := s.SortValue(a)
	bVal := s.SortValue(b)
	if aVal == bVal {
		return 0
	}
	ret := aVal - bVal
	if s.Order == BalanceSortOrderTypeDesc {
		ret = -ret
	}
	return ret
}

type sorterBalancer struct {
	Sorters         []BalanceSorter
	defaultBalancer EBLoadBalancer
}

func NewSorterBalancer(sorters ...BalanceSorter) EBLoadBalancer {
	return &sorterBalancer{
		Sorters:         sorters,
		defaultBalancer: NewRoundRobinBalancer(),
	}
}

func (s sorterBalancer) Select(list []*EBServerInfo) (info *EBServerInfo) {
	if len(list) == 0 {
		return nil
	}
	if len(s.Sorters) == 0 {
		return s.defaultBalancer.Select(list)
	}
	for _, sorter := range s.Sorters {
		slices.SortFunc(list, sorter.Compare)
	}
	return list[0]
}

//
//func (l *loadBalancer) Select(list []*EBServerInfo) (info *EBServerInfo) {
//	if len(list) == 0 {
//		return nil
//	}
//	for _, i := range list {
//		if info == nil || i.Load < info.Load {
//			info = i
//		}
//	}
//	return
//}
//
//func NewLoadBalancer() EBLoadBalancer {
//	return &loadBalancer{}
//}
