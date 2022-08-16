package swim

import (
	"math"
	"sort"
	"sync"
)

const (
	JOIN    = iota
	FAILURE = iota
	LEAVE   = iota
)

type DisseminationBufferConfig struct {
	Lambda float64
}

type DisseminationBuffer struct {
	MemberCount int
	buffer      []Update
	config      DisseminationBufferConfig
	lock        sync.Mutex
}

func NewDisseminationBuffer(config DisseminationBufferConfig) *DisseminationBuffer {
	return &DisseminationBuffer{
		buffer: make([]Update, 0),
		config: config,
		lock:   sync.Mutex{},
	}
}

func (db *DisseminationBuffer) Add(update Update) {
	db.lock.Lock()
	defer db.lock.Unlock()
	db.buffer = append(db.buffer, update)
}

// Get a set of messages to disseminate
func (db *DisseminationBuffer) Get(n int) []Update {
	db.lock.Lock()
	defer db.lock.Unlock()
	// 1. Clean buffer
	db.CleanUp()
	// 2. Sort by dissemination count
	sort.Slice(db.buffer, func(i, j int) bool {
		return db.buffer[i].Count < db.buffer[j].Count
	})
	// 3. Return the most recent ones
	// TODO: decrement dissemination count.
	if len(db.buffer) == 0 {
		return make([]Update, 0)
	} else if len(db.buffer) < n {
		return db.buffer
	} else {
		return db.buffer[:n]
	}
}

// Remove elements from the buffer that have reached the dissemination capacity.
func (db *DisseminationBuffer) CleanUp() {
	keep := make([]Update, 0)
	// Remove elements from the buffer that have reached the dissemination capacity.
	for _, update := range db.buffer {
		if float64(update.Count) < db.config.Lambda*math.Log(float64(db.MemberCount)) {
			keep = append(keep, update)
		}
	}
	db.buffer = keep
}
