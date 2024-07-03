package utils

import "sync"

type SafeBool struct {
	Mu    sync.Mutex
	Value bool
}
