package set

import "sync"

type set struct {
	m map[interface{}]struct{}
	sync.RWMutex
}

func New() *set {
	return &set{
		m: map[interface{}]struct{}{},
	}
}

func (s *set) Add(item interface{}) {
	s.Lock()
	defer s.Unlock()
	s.m[item] = struct{}{}
}

func (s *set) Remove(item interface{}) {
	s.Lock()
	defer s.Unlock()
	delete(s.m, item)
}

func (s *set) Has(item interface{}) bool {
	s.RLock()
	defer s.RUnlock()
	_, ok := s.m[item]
	return ok
}

func (s *set) Len() int {
	return len(s.List())
}

func (s *set) Clear() {
	s.Lock()
	defer s.Unlock()
	s.m = map[interface{}]struct{}{}
}

func (s *set) IsEmpty() bool{
	if s.Len() == 0 {
		return true
	}
	return false
}

func (s *set) List() []interface{} {
	s.RLock()
	defer s.Unlock()
	list := []interface{}{}
	for item :=  range s.m {
		list = append(list, item)
	}

	return list
}
