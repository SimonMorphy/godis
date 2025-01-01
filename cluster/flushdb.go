package cluster

import (
	"github.com/SimonMorphy/godis/interface/resp"
	"github.com/SimonMorphy/godis/resp/reply"
)

func flushDB(clusterDatabase *ClusterDatabase, connection resp.Connection, args [][]byte) resp.Reply {
	replies := clusterDatabase.broadcast(connection, args)
	var errReply reply.ErrorReply
	for _, r := range replies {
		if reply.IsErrReply(r) {
			errReply = r.(reply.ErrorReply)
			break
		}
	}
	if errReply == nil {
		return reply.MakeOKReply()
	}
	return reply.MakeErrReply("ERR " + errReply.Error())
}
