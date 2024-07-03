package consistentHash

import (
	"fmt"
	"hash/crc32"
	"sort"
)

type HashFunc func(data []byte) uint32 //哈希函数

type NodeMap struct {
	hashFunc    HashFunc
	nodeHashs   []int          //节点哈希（位置）列表。！因为要对节点的哈希值进行排序，sort函数默认不能实现排序。解决方法1.类型转换2.将uint32实现sort接口
	nodeHashMap map[int]string //string记录的节点名字、地址
}

func (m *NodeMap) IsEmpty() bool { //判断nodeHashs
	return len(m.nodeHashs) == 0
}

func NewNodeMap(hf HashFunc) *NodeMap {
	if hf == nil {
		hf = crc32.ChecksumIEEE //默认哈希函数
	}

	return &NodeMap{
		hashFunc:    hf,
		nodeHashMap: make(map[int]string),
	}
}

func (m *NodeMap) AddNode(keys ...string) { //传入名称或地址。将节点加入到列表中并重新排序
	for _, key := range keys {
		if key == "" {
			continue
		}
		hash := int(m.hashFunc([]byte(key)))    //根据NodeMap自身维护的哈希函数对传入的key【字符串】进行哈希，得到哈希值。
		m.nodeHashs = append(m.nodeHashs, hash) //将计算出的哈希值添加到节点list中
		m.nodeHashMap[hash] = key               //记录key和hash的对应关系，以便根据hash得到key。
	}

	sort.Ints(m.nodeHashs) //将节点的哈希值进行排序
}

func (m *NodeMap) PickNode(key string) string { //返回string是目标节点的地址
	if m.IsEmpty() {
		return ""
	}
	hash := int(m.hashFunc([]byte(key)))
	fmt.Println("根据key的hash值为", hash)
	nodeIDX := sort.Search(len(m.nodeHashs), func(i int) bool {
		return m.nodeHashs[i] >= hash //找到大于该hash的第一个哈希，也就是找到了节点
	}) //返回满足（条件函数）的第一个下标
	fmt.Println("找到的大于该hash的第一个下标为", nodeIDX)
	if nodeIDX == len(m.nodeHashs) {
		nodeIDX = 0
	}
	fmt.Println("PickNode选择的结点为:", m.nodeHashMap[m.nodeHashs[nodeIDX]])
	return m.nodeHashMap[m.nodeHashs[nodeIDX]]
}

func (m *NodeMap) ShowNodeHashMap() {
	fmt.Println("集群的HashMap为：")
	for k, v := range m.nodeHashMap {
		fmt.Printf("[%d]%s\n", k, v)
	}
	fmt.Println("集群的nodeHashes哈希值为", m.nodeHashs)
}
