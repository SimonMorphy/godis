package cluster

import (
	"github.com/SimonMorphy/godis/interface/resp"
	"github.com/SimonMorphy/godis/resp/reply"
)

func Del(clusterDatabase *ClusterDatabase, connection resp.Connection, args [][]byte) resp.Reply {
	replies := clusterDatabase.broadcast(connection, args)
	var errReply reply.ErrorReply
	var deleted int64 = 0
	for _, r := range replies {
		if reply.IsErrReply(r) {
			errReply = r.(reply.ErrorReply)
			break
		}
		intReply, ok := r.(*reply.IntReply)
		if !ok {
			errReply = reply.MakeErrReply("error")
		}
		deleted += intReply.Code
	}
	if errReply == nil {
		return reply.MakeIntReply(deleted)
	}
	return reply.MakeErrReply("Err" + errReply.Error())
}
