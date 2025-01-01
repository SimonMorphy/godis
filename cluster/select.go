package cluster

import "github.com/SimonMorphy/godis/interface/resp"

func execSelect(clusterDatabase *ClusterDatabase, connection resp.Connection, args [][]byte) resp.Reply {
	return clusterDatabase.db.Exec(connection, args)
}
