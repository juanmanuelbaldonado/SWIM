package message

type Message interface {
	encode()
	decode()
	size()
}
