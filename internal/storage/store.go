package storage

import (
	"sync"

	"github.com/berylm1/iot-analytics-dashboard/internal/models"
)

// Store is an interface - this defines what our storage can do
type Store interface {
	Add(data models.AggregatedData)
	GetRecent(limit int) []models.AggregatedData
	GetAll() []models.AggregatedData
}

// InMemoryStore stores data in RAM (memory)
type InMemoryStore struct {
	mu      sync.RWMutex               // Lock for thread safety
	data    []models.AggregatedData    // Slice to hold our data
	maxSize int                        // Maximum records to keep
}

// NewInMemoryStore creates a new storage instance
func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		data:    make([]models.AggregatedData, 0),
		maxSize: 1000, // Keep last 1000 aggregations
	}
}

// Add adds new aggregated data to storage
func (s *InMemoryStore) Add(aggregation models.AggregatedData) {
	s.mu.Lock()   // Lock for writing
	defer s.mu.Unlock()  // Unlock when done

	s.data = append(s.data, aggregation)

	// Keep only the most recent maxSize items
	if len(s.data) > s.maxSize {
		s.data = s.data[len(s.data)-s.maxSize:]
	}
}

// GetRecent returns the most recent N items
func (s *InMemoryStore) GetRecent(limit int) []models.AggregatedData {
	s.mu.RLock()  // Lock for reading
	defer s.mu.RUnlock()

	if len(s.data) == 0 {
		return []models.AggregatedData{}
	}

	start := len(s.data) - limit
	if start < 0 {
		start = 0
	}

	// Make a copy to avoid race conditions
	result := make([]models.AggregatedData, len(s.data[start:]))
	copy(result, s.data[start:])

	// Reverse to get newest first
	for i := 0; i < len(result)/2; i++ {
		j := len(result) - i - 1
		result[i], result[j] = result[j], result[i]
	}

	return result
}

// GetAll returns all stored data
func (s *InMemoryStore) GetAll() []models.AggregatedData {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]models.AggregatedData, len(s.data))
	copy(result, s.data)
	return result
}
