package cluster

import (
	"github.com/SimonMorphy/godis/interface/resp"
	"github.com/SimonMorphy/godis/resp/reply"
)

func rename(clusterDatabase *ClusterDatabase, connection resp.Connection, args [][]byte) resp.Reply {
	if len(args) != 3 {
		return reply.MakeErrReply("Err Wrong num args")
	}
	src := string(args[1])
	dest := string(args[1])
	srcPeer := clusterDatabase.peerPicker.PickNode(src)
	destPeer := clusterDatabase.peerPicker.PickNode(dest)
	if srcPeer != destPeer {
		return reply.MakeErrReply("Err rename must within on peer")
	}
	return clusterDatabase.relay(srcPeer, connection, args)
}
