package swim

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

const (
	STATE_ALIVE      = iota
	STATE_SUSPICIOUS = iota
	STATE_DEAD       = iota
)

type NodeInfo struct {
	Id      int64
	Address net.UDPAddr
	LastAck time.Time
	State   int32
	lock    sync.Mutex
}

type Tracker struct {
	members map[int64]*NodeInfo
}

func NewTracker() *Tracker {
	return &Tracker{
		make(map[int64]*NodeInfo),
	}
}

func (tracker *Tracker) AddMember(id int64, address net.UDPAddr) {
	if _, ok := tracker.members[id]; !ok {
		tracker.members[id] = &NodeInfo{
			Id:      id,
			Address: address,
			LastAck: time.Now(),
			State:   STATE_ALIVE,
			lock:    sync.Mutex{},
		}
	}
}

func (tracker *Tracker) Update(id int64, state int32) error {
	if node, ok := tracker.members[id]; ok {
		node.lock.Lock()
		defer node.lock.Unlock()
		node.State = state
		node.LastAck = time.Now()
		return nil
	} else {
		return errors.New("Node not found")
	}
}

func (tracker *Tracker) Get(id int64) (*NodeInfo, bool) {
	node, ok := tracker.members[id]
	return node, ok
}

func (tracker *Tracker) Delete(id int64) {
	delete(tracker.members, id)
}

func (tracker *Tracker) GetNodes() []int64 {
	keys := make([]int64, 0, len(tracker.members))
	for k := range tracker.members {
		keys = append(keys, k)
	}
	return keys
}

func (tracker *Tracker) processUpdate(update *Update, from *net.UDPAddr) {
	switch update.Event {
	case JOIN:
		tracker.AddMember(int64(update.Member), *from)
		break
	case FAILURE:
		tracker.Update(int64(update.Member), STATE_DEAD)
		break
	case LEAVE:
		tracker.Delete(int64(update.Member))
	}
}

func NodeInfoFromAdress(host string, port int) (*NodeInfo, error) {
	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		return nil, err
	}
	return &NodeInfo{
		Id:      int64(hash(fmt.Sprintf("%s:%d", host, port))),
		Address: *addr,
		LastAck: time.Now(),
		lock:    sync.Mutex{},
		State:   STATE_ALIVE,
	}, nil
}
