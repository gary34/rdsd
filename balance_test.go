package ebsd

import (
	"fmt"
	"testing"
)

// 创建测试用的EBServerInfo实例
func createTestServerInfo(id, name string, load, power, order int) *EBServerInfo {
	return &EBServerInfo{
		BaseServerInfo: BaseServerInfo{
			ID:   id,
			Name: name,
		},
		Addr:  "127.0.0.1:8080",
		Load:  load,
		Power: power,
		Order: order,
	}
}

func TestSorterBalancer_Select_EmptyList(t *testing.T) {
	// 测试空列表情况
	sorters := []BalanceSorter{
		NewBalanceSorter(BalanceSortByTypeLoad, BalanceSortOrderTypeAsc),
	}
	balancer := NewSorterBalancer(sorters...)

	result := balancer.Select([]*EBServerInfo{})
	if result != nil {
		t.Errorf("Expected nil for empty list, got %v", result)
	}
}

func TestSorterBalancer_Select_NilList(t *testing.T) {
	// 测试nil列表情况
	sorters := []BalanceSorter{
		NewBalanceSorter(BalanceSortByTypeLoad, BalanceSortOrderTypeAsc),
	}
	balancer := NewSorterBalancer(sorters...)

	result := balancer.Select(nil)
	if result != nil {
		t.Errorf("Expected nil for nil list, got %v", result)
	}
}

func TestSorterBalancer_Select_SingleServer(t *testing.T) {
	// 测试单个服务器情况
	server := createTestServerInfo("server1", "test", 10, 5, 1)
	list := []*EBServerInfo{server}

	sorters := []BalanceSorter{
		NewBalanceSorter(BalanceSortByTypeLoad, BalanceSortOrderTypeAsc),
	}
	balancer := NewSorterBalancer(sorters...)

	result := balancer.Select(list)
	if result != server {
		t.Errorf("Expected %v, got %v", server, result)
	}
}

func TestSorterBalancer_Select_SortByLoad_Asc(t *testing.T) {
	// 测试按负载升序排序
	server1 := createTestServerInfo("server1", "test", 30, 5, 1)
	server2 := createTestServerInfo("server2", "test", 10, 5, 2)
	server3 := createTestServerInfo("server3", "test", 20, 5, 3)
	list := []*EBServerInfo{server1, server2, server3}

	sorters := []BalanceSorter{
		NewBalanceSorter(BalanceSortByTypeLoad, BalanceSortOrderTypeAsc),
	}
	balancer := NewSorterBalancer(sorters...)

	result := balancer.Select(list)
	if result.ID != "server2" {
		t.Errorf("Expected server2 (lowest load), got %s", result.ID)
	}
	if result.Load != 10 {
		t.Errorf("Expected load 10, got %d", result.Load)
	}
}

func TestSorterBalancer_Select_SortByLoad_Desc(t *testing.T) {
	// 测试按负载降序排序
	server1 := createTestServerInfo("server1", "test", 30, 5, 1)
	server2 := createTestServerInfo("server2", "test", 10, 5, 2)
	server3 := createTestServerInfo("server3", "test", 20, 5, 3)
	list := []*EBServerInfo{server1, server2, server3}

	sorters := []BalanceSorter{
		NewBalanceSorter(BalanceSortByTypeLoad, BalanceSortOrderTypeDesc),
	}
	balancer := NewSorterBalancer(sorters...)

	result := balancer.Select(list)
	if result.ID != "server1" {
		t.Errorf("Expected server1 (highest load), got %s", result.ID)
	}
	if result.Load != 30 {
		t.Errorf("Expected load 30, got %d", result.Load)
	}
}

func TestSorterBalancer_Select_SortByPower_Asc(t *testing.T) {
	// 测试按权重升序排序
	server1 := createTestServerInfo("server1", "test", 10, 30, 1)
	server2 := createTestServerInfo("server2", "test", 10, 10, 2)
	server3 := createTestServerInfo("server3", "test", 10, 20, 3)
	list := []*EBServerInfo{server1, server2, server3}

	sorters := []BalanceSorter{
		NewBalanceSorter(BalanceSortByTypePower, BalanceSortOrderTypeAsc),
	}
	balancer := NewSorterBalancer(sorters...)

	result := balancer.Select(list)
	if result.ID != "server2" {
		t.Errorf("Expected server2 (lowest power), got %s", result.ID)
	}
	if result.Power != 10 {
		t.Errorf("Expected power 10, got %d", result.Power)
	}
}

func TestSorterBalancer_Select_SortByOrder_Asc(t *testing.T) {
	// 测试按排序升序排序
	server1 := createTestServerInfo("server1", "test", 10, 10, 30)
	server2 := createTestServerInfo("server2", "test", 10, 10, 10)
	server3 := createTestServerInfo("server3", "test", 10, 10, 20)
	list := []*EBServerInfo{server1, server2, server3}

	sorters := []BalanceSorter{
		NewBalanceSorter(BalanceSortByTypeOrder, BalanceSortOrderTypeAsc),
	}
	balancer := NewSorterBalancer(sorters...)

	result := balancer.Select(list)
	if result.ID != "server2" {
		t.Errorf("Expected server2 (lowest order), got %s", result.ID)
	}
	if result.Order != 10 {
		t.Errorf("Expected order 10, got %d", result.Order)
	}
}

func TestSorterBalancer_Select_MultipleSorters(t *testing.T) {
	// 测试多个排序器组合：先按负载升序，再按权重降序
	// 注意：多个排序器会依次对整个列表进行排序，后面的排序器会重新排序整个列表
	server1 := createTestServerInfo("server1", "test", 10, 30, 1) // load=10, power=30
	server2 := createTestServerInfo("server2", "test", 10, 20, 2) // load=10, power=20
	server3 := createTestServerInfo("server3", "test", 20, 40, 3) // load=20, power=40
	list := []*EBServerInfo{server1, server2, server3}

	sorters := []BalanceSorter{
		NewBalanceSorter(BalanceSortByTypeLoad, BalanceSortOrderTypeAsc),   // 先按负载升序
		NewBalanceSorter(BalanceSortByTypePower, BalanceSortOrderTypeDesc), // 再按权重降序
	}
	balancer := NewSorterBalancer(sorters...)

	result := balancer.Select(list)
	// 排序逻辑：
	// 1. 第一次排序按负载升序：server1(10), server2(10), server3(20)
	// 2. 第二次排序按权重降序：server3(40), server1(30), server2(20)
	// 最终结果：权重最高的server3被选中
	if result.ID != "server3" {
		t.Errorf("Expected server3 (highest power), got %s (load=%d, power=%d)", result.ID, result.Load, result.Power)
	}
}

func TestSorterBalancer_Select_ComplexMultipleSorters(t *testing.T) {
	// 测试复杂的多排序器组合：负载升序 -> 权重升序 -> 排序降序
	server1 := createTestServerInfo("server1", "test", 10, 20, 30)
	server2 := createTestServerInfo("server2", "test", 10, 20, 40)
	server3 := createTestServerInfo("server3", "test", 10, 30, 20)
	server4 := createTestServerInfo("server4", "test", 20, 10, 10)
	list := []*EBServerInfo{server1, server2, server3, server4}

	sorters := []BalanceSorter{
		NewBalanceSorter(BalanceSortByTypeLoad, BalanceSortOrderTypeAsc),   // 负载升序
		NewBalanceSorter(BalanceSortByTypePower, BalanceSortOrderTypeAsc),  // 权重升序
		NewBalanceSorter(BalanceSortByTypeOrder, BalanceSortOrderTypeDesc), // 排序降序
	}
	balancer := NewSorterBalancer(sorters...)

	result := balancer.Select(list)
	// 期望结果：
	// 1. 负载最小的是server1,2,3 (load=10)
	// 2. 在这些中权重最小的是server1,2 (power=20)
	// 3. 在这些中排序最大的是server2 (order=40)
	if result.ID != "server2" {
		t.Errorf("Expected server2, got %s (load=%d, power=%d, order=%d)", result.ID, result.Load, result.Power, result.Order)
	}
}

func TestSorterBalancer_Select_NoSorters(t *testing.T) {
	// 测试没有排序器的情况
	server1 := createTestServerInfo("server1", "test", 30, 5, 1)
	server2 := createTestServerInfo("server2", "test", 10, 5, 2)
	list := []*EBServerInfo{server1, server2}

	sorters := []BalanceSorter{} // 空排序器列表
	balancer := NewSorterBalancer(sorters...)

	result := balancer.Select(list)
	// 没有排序器时，应该返回原列表的第一个元素
	if result.ID != "server1" {
		t.Errorf("Expected server1 (first in original list), got %s", result.ID)
	}
}

func TestNewSorterBalancer(t *testing.T) {
	// 测试构造函数
	sorters := []BalanceSorter{
		NewBalanceSorter(BalanceSortByTypeLoad, BalanceSortOrderTypeAsc),
		NewBalanceSorter(BalanceSortByTypePower, BalanceSortOrderTypeDesc),
	}

	balancer := NewSorterBalancer(sorters...)
	if balancer == nil {
		t.Error("Expected non-nil balancer")
	}

	// 验证类型
	sorterBalancer, ok := balancer.(*sorterBalancer)
	if !ok {
		t.Error("Expected *sorterBalancer type")
	}

	// 验证排序器数量
	if len(sorterBalancer.Sorters) != 2 {
		t.Errorf("Expected 2 sorters, got %d", len(sorterBalancer.Sorters))
	}
}

// 基准测试
func BenchmarkSorterBalancer_Select(b *testing.B) {
	// 创建测试数据
	servers := make([]*EBServerInfo, 100)
	for i := 0; i < 100; i++ {
		servers[i] = createTestServerInfo(
			fmt.Sprintf("server%d", i),
			"test",
			i%50, // load: 0-49
			i%30, // power: 0-29
			i%20, // order: 0-19
		)
	}

	sorters := []BalanceSorter{
		NewBalanceSorter(BalanceSortByTypeLoad, BalanceSortOrderTypeAsc),
		NewBalanceSorter(BalanceSortByTypePower, BalanceSortOrderTypeDesc),
	}
	balancer := NewSorterBalancer(sorters...)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// 每次测试都使用原始列表的副本，避免排序影响下次测试
		testList := make([]*EBServerInfo, len(servers))
		copy(testList, servers)
		balancer.Select(testList)
	}
}
