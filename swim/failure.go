package swim

import (
	"fmt"
	"log"
	"math/rand"
	"time"
)

type FailureDetectorConfig struct {
	// Protocol params
	Period    time.Duration
	GroupSize int32
	Timeout   time.Duration
}

type FailureDetector struct {
	Config  FailureDetectorConfig
	Tracker *Tracker
	Router  *Router
}

func NewFailureDetector(config *FailureDetectorConfig, router *Router, tracker *Tracker) *FailureDetector {
	return &FailureDetector{
		Tracker: tracker,
		Router:  router,
		Config:  *config,
	}
}

// Returns true if the peer is found, otherwise false
func (fd *FailureDetector) pingPeer(peerId int64, msgType int) bool {
	log.Println(fmt.Sprintf("[PingPeer] Sending ping request to node %d.", peerId))
	callback := make(chan Message)
	fd.Router.Send(int64(peerId), int64(msgType), callback)
	select {
	case response := <-callback:
		if response.Type == ACK {
			log.Println(fmt.Sprintf("[PingPeer] Request to node %d was successful.", peerId))
			return true
		}
	case <-time.After(fd.Config.Timeout):
		// call timed out
		log.Println(fmt.Sprintf("[PingPeer] Request to node %d timed out.", peerId))
		break
	}
	return false
}

func (fd *FailureDetector) Ping(clusterMembers []int64) (int64, bool) {
	peerIdx := clusterMembers[rand.Intn(len(clusterMembers))]
	log.Println(fmt.Sprintf("[Ping] Probing node %d", peerIdx))
	// 1. Direct ping
	if fd.pingPeer(peerIdx, PING) {
		log.Println(fmt.Sprintf("[Ping] Success. Node %d is alive", peerIdx))
		return int64(peerIdx), true
	}
	// 2. Indirect ping
	peerIdxs := Sample(int(fd.Config.GroupSize), int64(len(clusterMembers)))
	log.Println(fmt.Sprintf("[Ping] Request to node %d timeout. Routing request to group members..", peerIdx))
	found := make(chan bool)
	for _, peer := range peerIdxs {
		go func(nodeId int64) {
			if fd.pingPeer(nodeId, PING_REQ) {
				found <- true
			}
		}(peer)
	}
	select {
	case <-found:
		return int64(peerIdx), true
	case <-time.After(fd.Config.Timeout):
		return int64(peerIdx), false
	}
}

func (fd *FailureDetector) handlePing(message Message) {
	log.Println("[PingHandler] Responding to message from %d", message.From)
	fd.Router.Send(message.From, ACK, nil)
}

func (fd *FailureDetector) handlePingReq(message Message) {
	log.Println("[PingRequestHandler] Forwarding ping from %d to %d", message.From, message.Forward)
	if fd.pingPeer(message.Forward, PING) {
		fd.Router.Send(message.From, ACK, nil)
	}
}

func (fd *FailureDetector) Listen() {
	// 1. Set up handlers
	fd.Router.AddHandler(&Handler{
		applies: func(m Message) bool { return m.Type == PING },
		handle:  func(m Message) { fd.handlePing(m) },
	})
	//
	fd.Router.AddHandler(&Handler{
		applies: func(m Message) bool { return m.Type == PING_REQ },
		handle:  func(m Message) { fd.handlePingReq(m) },
	})
	go fd.Router.Listen()
}
