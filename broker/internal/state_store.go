package synapse

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrCapacityExceeded is returned by Set when the key is brand new and the store is already full
var ErrCapacityExceeded = errors.New("state store capacity exceeded")

// ErrValueTooLarge is returned by Set when value exceeds the configured max value size
var ErrValueTooLarge = errors.New("state store value too large")

// stateEntry is one stored key/value pair with its absolute expiry, computed once at set time rather than a per-node timer
type stateEntry struct {
	Value    []byte    `json:"value"`
	ExpireAt time.Time `json:"expire_at"`
}

// StateStore is a single, non-namespaced key/value store replicated via the same Raft group as the message brokers
// Reads are served from local state directly, only mutations (Set/Delete) go through consensus
type StateStore struct {
	mu            sync.Mutex
	entries       map[string]stateEntry
	maxEntries    int
	maxValueBytes int
	defaultTTL    time.Duration
	sweepInterval time.Duration
	stopCh        chan struct{}
	stopOnce      sync.Once
}

// defaultSweepInterval is used when sweepInterval is non-positive
const defaultSweepInterval = 30 * time.Second

func NewStateStore(maxEntries int, maxValueBytes int, defaultTTL time.Duration, sweepInterval time.Duration) *StateStore {
	if sweepInterval <= 0 {
		sweepInterval = defaultSweepInterval
	}
	return &StateStore{
		entries:       make(map[string]stateEntry),
		maxEntries:    maxEntries,
		maxValueBytes: maxValueBytes,
		defaultTTL:    defaultTTL,
		sweepInterval: sweepInterval,
		stopCh:        make(chan struct{}),
	}
}

// Set stores value under key, replacing any existing entry and refreshing its TTL
// Updating an existing key is always allowed regardless of capacity, only a brand new key can be rejected
func (s *StateStore) Set(key string, value []byte) error {
	if len(value) > s.maxValueBytes {
		return ErrValueTooLarge
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.entries[key]; !exists && len(s.entries) >= s.maxEntries {
		return ErrCapacityExceeded
	}

	s.entries[key] = stateEntry{Value: value, ExpireAt: time.Now().Add(s.defaultTTL)}
	return nil
}

// Get returns the value for key, or false if it's missing or its expiry has already passed
func (s *StateStore) Get(key string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.entries[key]
	if !exists || time.Now().After(entry.ExpireAt) {
		return nil, false
	}
	return entry.Value, true
}

// Delete removes key if present, deleting a missing key is a no-op
func (s *StateStore) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.entries, key)
}

// sweep drops locally expired entries, this is local-only bookkeeping, same philosophy as Broker.Deliver's non-replicated retry state, small clock skew between nodes is fine here
func (s *StateStore) sweep() {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, entry := range s.entries {
		if now.After(entry.ExpireAt) {
			delete(s.entries, key)
		}
	}
}

// StartSweeper launches the background expiry sweep, call once per StateStore
func (s *StateStore) StartSweeper() {
	go func() {
		ticker := time.NewTicker(s.sweepInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s.sweep()
			case <-s.stopCh:
				return
			}
		}
	}()
}

// Stop halts the background sweeper, safe to call more than once
func (s *StateStore) Stop() {
	s.stopOnce.Do(func() { close(s.stopCh) })
}

// Dump serializes the live, non-expired entries for snapshot persistence
func (s *StateStore) Dump() ([]byte, error) {
	now := time.Now()
	s.mu.Lock()
	live := make(map[string]stateEntry, len(s.entries))
	for key, entry := range s.entries {
		if now.After(entry.ExpireAt) {
			continue
		}
		live[key] = entry
	}
	s.mu.Unlock()

	// live is an independent copy, marshaling it outside the lock avoids blocking Get/Set/Delete for the duration
	data, err := json.Marshal(live)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal state snapshot: %w", err)
	}
	return data, nil
}

// Load replaces the store's contents from a snapshot produced by Dump
func (s *StateStore) Load(data []byte) error {
	loaded := make(map[string]stateEntry)
	if len(data) > 0 {
		if err := json.Unmarshal(data, &loaded); err != nil {
			return fmt.Errorf("failed to unmarshal state snapshot: %w", err)
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = loaded
	return nil
}
