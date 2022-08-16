package swim

import (
	"log"
	"net"
	"time"
)

type Config struct {
	Host                      string
	Port                      int
	FailureDetectorConfig     FailureDetectorConfig
	DisseminationBufferConfig DisseminationBufferConfig
	BootstrapNodes            []net.UDPAddr
}

type SwimNode struct {
	Node                NodeInfo
	Tracker             *Tracker
	FailureDetector     *FailureDetector
	DisseminationBuffer *DisseminationBuffer
	Router              *Router
	Config              Config
}

func NewSWIMNode(config Config) (*SwimNode, error) {
	member, err := NodeInfoFromAdress(config.Host, config.Port)
	if err != nil {
		return nil, err
	}
	// Initialize tracker
	tracker := NewTracker()
	for _, member := range config.BootstrapNodes {
		tracker.AddMember(memberIdFromAddress(string(member.IP), member.Port), member)
	}
	// Initialize router
	router := NewRouter(member, tracker)
	// Initialize failure detector
	failureDetector := NewFailureDetector(&config.FailureDetectorConfig, router, tracker)
	// Initialize buffer
	buffer := NewDisseminationBuffer(config.DisseminationBufferConfig)

	node := &SwimNode{
		Tracker:             tracker,
		FailureDetector:     failureDetector,
		DisseminationBuffer: buffer,
		Router:              router,
		Config:              config,
	}
	return node, nil
}

func (node *SwimNode) Start() {
	log.Println("Started..")
	node.FailureDetector.Listen()
}

func (node *SwimNode) Probe() {
	for {
		// 1. Check for failure in a node from the network.
		log.Println("Group nodes: ", node.Tracker.GetNodes())
		if peerId, alive := node.FailureDetector.Ping(node.Tracker.GetNodes()); !alive {
			node.Tracker.Update(peerId, STATE_DEAD)
			// 2. Add the failure event to the di	ssemination buffer.
			node.DisseminationBuffer.Add(Update{Event: FAILURE, Member: int(peerId), Count: 0})
		} else {
			node.Tracker.Update(peerId, STATE_ALIVE)
		}
		time.Sleep(node.Config.FailureDetectorConfig.Period)
	}
}
