package consistentHash

import (
	"hash/crc32"
	"sort"
)

type HashFunc func(data []byte) uint32

type NodeMap struct {
	hashFunc    HashFunc
	nodeHashes  []int
	nodeHashMap map[int]string
}

func NewNodeMap(fc HashFunc) *NodeMap {
	nodeMap := &NodeMap{
		hashFunc:    fc,
		nodeHashMap: make(map[int]string),
	}
	if nodeMap.hashFunc == nil {
		nodeMap.hashFunc = crc32.ChecksumIEEE
	}
	return nodeMap
}

func (m *NodeMap) IsEmpty() bool {
	return len(m.nodeHashMap) == 0
}
func (m *NodeMap) AddNodes(keys ...string) {
	for _, key := range keys {
		if key == "" {
			continue
		}
		hash := int(m.hashFunc([]byte(key)))
		m.nodeHashes = append(m.nodeHashes, hash)
		m.nodeHashMap[hash] = key
	}
	sort.Ints(m.nodeHashes)
}

func (m *NodeMap) PickNode(key string) string {
	if m.IsEmpty() {
		return ""
	}
	hash := int(m.hashFunc([]byte(key)))
	index := sort.Search(len(m.nodeHashes), func(i int) bool {
		return hash <= i
	})
	if index == len(m.nodeHashes) {
		index = 0
	}
	return m.nodeHashMap[m.nodeHashes[index]]
}
