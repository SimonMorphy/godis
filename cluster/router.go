package cluster

import "github.com/SimonMorphy/godis/interface/resp"

func DefaultFunc(clusterDatabase *ClusterDatabase, connection resp.Connection, args [][]byte) resp.Reply {
	key := string(args[1])
	peer := clusterDatabase.peerPicker.PickNode(key)
	return clusterDatabase.relay(peer, connection, args)
}

func MakeRouter() map[string]CmdFunc {
	routerMap := make(map[string]CmdFunc)
	routerMap["exists"] = DefaultFunc
	routerMap["type"] = DefaultFunc
	routerMap["set"] = DefaultFunc
	routerMap["setnx"] = DefaultFunc
	routerMap["get"] = DefaultFunc
	routerMap["getset"] = DefaultFunc
	routerMap["ping"] = ping
	routerMap["rename"] = rename
	routerMap["renamenx"] = rename
	routerMap["flushdb"] = flushDB
	routerMap["del"] = Del
	routerMap["select"] = execSelect
	return routerMap
}
