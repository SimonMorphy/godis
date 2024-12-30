package dict

import "sync"

type SyncDict struct {
	m sync.Map
}

func MakeSyncDic() *SyncDict {
	return &SyncDict{}
}

func (s *SyncDict) Get(key string) (val interface{}, exists bool) {
	value, ok := s.m.Load(key)
	return value, ok
}

func (s *SyncDict) Len() int {
	length := 0
	s.m.Range(func(key, value interface{}) bool {
		length++
		return true
	})
	return length
}

func (s *SyncDict) Put(key string, val interface{}) (result int) {
	_, ok := s.m.Load(key)
	s.m.Store(key, val)
	if ok {
		return 0
	}
	return 1
}

func (s *SyncDict) PutIfAbsent(key string, val interface{}) (result int) {
	_, ok := s.m.Load(key)
	if ok {
		return 0
	}
	s.m.Store(key, val)
	return 1
}

func (s *SyncDict) PutIfExist(key string, val interface{}) (result int) {
	_, ok := s.m.Load(key)
	if ok {
		s.m.Store(key, val)
		return 1
	}
	return 0
}

func (s *SyncDict) Remove(key string) (result int) {
	_, ok := s.m.Load(key)
	if ok {
		s.m.Delete(key)
		return 1
	}
	return 0
}

func (s *SyncDict) ForEach(consumer Consumer) {
	s.m.Range(func(key, value interface{}) bool {
		consumer(key.(string), value)
		return true
	})
}

func (s *SyncDict) Keys() []string {
	keys := make([]string, s.Len())
	i := 0
	s.m.Range(func(key, value interface{}) bool {
		keys[i] = key.(string)
		i++
		return true
	})
	return keys
}

func (s *SyncDict) RandomKey(limit int) []string {
	keys := make([]string, s.Len())
	for i := 0; i < limit; i++ {
		s.m.Range(func(key, value any) bool {
			keys[i] = key.(string)
			return false
		})
	}
	return keys
}

func (s *SyncDict) RandomDistinctKey(limit int) []string {
	keys := make([]string, s.Len())
	i := 0
	s.m.Range(func(key, value any) bool {
		keys[i] = key.(string)
		i++
		if i == limit {
			return false
		}
		return true
	})
	return keys
}

func (s *SyncDict) Clear() {
	*s = *MakeSyncDic()
}
