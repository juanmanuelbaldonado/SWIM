package swim

import (
	"fmt"
	"hash/fnv"
	"math/rand"
	"sync"
)

type AtomicCounter struct {
	value int64
	lock  sync.Mutex
}

func NewAtomicCounter() *AtomicCounter {
	return &AtomicCounter{
		value: 0,
		lock:  sync.Mutex{},
	}
}

func (counter *AtomicCounter) GetAndAdd() int64 {
	counter.lock.Lock()
	defer counter.lock.Unlock()

	value := counter.value
	counter.value += 1
	return value
}

// Sample n integers without replacement
func Sample(n int, max int64) []int64 {
	sampleIdxs := make([]int64, 0)
	sampled := make(map[int64]bool)
	for {
		if len(sampleIdxs) == n {
			break
		}
		candidate := rand.Int63n(max)
		if _, exists := sampled[candidate]; exists {
			sampled[candidate] = true
			sampleIdxs = append(sampleIdxs, candidate)
		}
	}
	return sampleIdxs
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func memberIdFromAddress(host string, port int) int64 {
	return int64(hash(fmt.Sprintf("%s:%d", host, port)))
}
