package utils

import "sync"

type SafeBool struct {
	mu    sync.Mutex
	value bool
}

func (sb *SafeBool) Set(val bool) {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	sb.value = val
}

func (sb *SafeBool) Get() bool {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	return sb.value
}
