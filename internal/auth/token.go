package auth

import "sync"

type TokenStore struct {
	mu    sync.RWMutex
	token string
}

func NewTokenStore() *TokenStore       { return &TokenStore{} }
func (s *TokenStore) Set(token string) { s.mu.Lock(); defer s.mu.Unlock(); s.token = token }
func (s *TokenStore) Get() string      { s.mu.RLock(); defer s.mu.RUnlock(); return s.token }
