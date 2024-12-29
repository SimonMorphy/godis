package reply

type PongReply struct {
}

var pongBytes = []byte("+PONG\r\n")

func (p *PongReply) ToBytes() []byte {
	return pongBytes
}

func MakePongReply() *PongReply {
	return &PongReply{}
}

type OKReply struct {
}

var okBytes = []byte("+OK\r\n")

func (O *OKReply) ToBytes() []byte {
	return okBytes
}

var theOKReply = new(OKReply)

func MakeOKReply() *OKReply {
	return theOKReply
}

type NullBulkReply struct {
}

var nullBulkBytes = []byte("$-1\r\n")

func (n *NullBulkReply) ToBytes() []byte {
	return nullBulkBytes
}

func MakeNullBulkReply() *NullBulkReply {
	return &NullBulkReply{}
}

type EmptyMultiBulkReply struct {
}

var emptyMultiBulkBytes = []byte("*0\r\n")

func (e *EmptyMultiBulkReply) ToBytes() []byte {
	return emptyMultiBulkBytes
}

func MakeEmptyMultiBulkReply() *EmptyMultiBulkReply {
	return &EmptyMultiBulkReply{}
}

var NoBytes = []byte("")

type NoReply struct {
}

func (n *NoReply) ToBytes() []byte {
	return NoBytes
}

var theNoReply = new(NoReply)

func MakeNoReply() *NoReply {
	return theNoReply
}
