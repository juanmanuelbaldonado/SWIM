package swim

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"net"
)

const MESSAGE_SIZE = 8 * 4
const HEADER_SIZE = 8*2 + 4
const EVENT_SIZE = 4 + 8

const (
	PING     = iota
	PING_REQ = iota
	ACK      = iota
)

type Update struct {
	Event  int
	Member int
	Count  int
}

type Message struct {
	From    int64
	Type    int64
	Forward int64
}

type Header struct {
	MessageId   int64
	From        int64
	UpdateCount int32
	Address     net.UDPAddr
}

type Packet struct {
	Header  Header
	Message Message
	Updates []Update
}

func encodePacket(packet Packet) ([]byte, error) {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	if err := enc.Encode(packet); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodePacket(encoded []byte) (*Packet, error) {
	var packet Packet
	decoder := gob.NewDecoder(bytes.NewBuffer(encoded))
	if err := decoder.Decode(&packet); err != nil {
		return nil, err
	}
	return &packet, nil
}

func encode(message Message) ([]byte, error) {
	buffer := &bytes.Buffer{}
	err := binary.Write(buffer, binary.BigEndian, message)
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func decode(encoded []byte) (*Message, error) {
	message := Message{}
	err := binary.Read(bytes.NewBuffer(encoded), binary.BigEndian, &message)
	if err != nil {
		return nil, err
	}
	return &message, nil
}
