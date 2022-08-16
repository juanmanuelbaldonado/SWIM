package swim

import (
	"log"
	"net"
	"time"
)

const MAX_UPDATES_PER_MESSAGE = 5
const READ_BUFFER_SIZE = 1024

type Handler struct {
	applies func(Message) bool
	handle  func(Message)
}

type MessageInfo struct {
	MessageId int64
	Timestamp time.Time
	Callback  (chan Message)
}

type Router struct {
	PeerId              int64
	Address             net.UDPAddr
	tracker             Tracker
	handlers            []Handler
	outgoing            map[int64]MessageInfo
	nextMessageId       AtomicCounter
	disseminationBuffer DisseminationBuffer
}

func NewRouter(node *NodeInfo, tracker *Tracker) *Router {
	return &Router{
		PeerId:        node.Id,
		Address:       node.Address,
		tracker:       *tracker,
		outgoing:      make(map[int64]MessageInfo),
		handlers:      make([]Handler, 0),
		nextMessageId: *NewAtomicCounter(),
	}
}

func (router *Router) AddHandler(handler *Handler) {
	router.handlers = append(router.handlers, *handler)
}

func (router *Router) buildPacket(to int64, messageType int64, responseCallback chan Message) *Packet {
	message := Message{
		From: int64(router.PeerId),
		Type: messageType,
	}
	updates := router.disseminationBuffer.Get(MAX_UPDATES_PER_MESSAGE)
	header := Header{
		MessageId:   router.nextMessageId.GetAndAdd(),
		From:        int64(router.PeerId),
		UpdateCount: int32(len(updates)),
	}
	return &Packet{
		Header:  header,
		Message: message,
		Updates: updates,
	}
}

func (router *Router) Send(to int64, messageType int64, responseCallback chan Message) error {
	peer, _ := router.tracker.Get(to)
	log.Println("Sending message to node")
	conn, err := net.DialUDP("udp", nil, &peer.Address)
	if err != nil {
		log.Panic(err)
	}
	defer conn.Close()
	packet := router.buildPacket(to, messageType, responseCallback)
	encoded, err := encodePacket(*packet)
	if err != nil {
		log.Panic(err)
		return err
	}
	_, err = conn.Write(encoded)
	if err != nil {
		log.Panic(err)
		return err
	}
	// Increse update counts

	if responseCallback != nil {
		// TODO: expire callbacks
		router.outgoing[packet.Header.MessageId] = MessageInfo{
			MessageId: packet.Header.MessageId,
			Timestamp: time.Now(),
			Callback:  responseCallback,
		}
	}
	return nil
}

func (router *Router) Listen() {
	log.Println("[Router] Listening on ", router.Address.String())
	conn, err := net.ListenUDP("udp", &router.Address)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	buffer := make([]byte, READ_BUFFER_SIZE)
	for {
		packet, from, err := readPacket(buffer, conn)
		log.Println("[Router] Got message from ", packet.Header)
		if err != nil {
			log.Println("[Router] Failed to decode message. Skiping..")
		}
		// 1. Check if there was a callback set for this message
		if msg, ok := router.outgoing[packet.Header.MessageId]; ok {
			msg.Callback <- packet.Message
			delete(router.outgoing, packet.Header.MessageId)
		}
		// 2. Check handlers
		for _, handler := range router.handlers {
			if handler.applies(*&packet.Message) {
				handler.handle(*&packet.Message)
			}
		}
		// 3. Handle updates
		for _, update := range packet.Updates {
			router.disseminationBuffer.Add(update)
			router.tracker.processUpdate(&update, from)
		}
	}
}

func readPacket(buffer []byte, conn *net.UDPConn) (*Packet, *net.UDPAddr, error) {
	_, addr, err := conn.ReadFromUDP(buffer[:])
	if err != nil {
		return nil, nil, err
	}
	packet, err := decodePacket(buffer[:])
	if err != nil {
		return nil, addr, err
	}
	return packet, addr, nil
}
