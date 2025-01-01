package cluster

import (
	"context"
	"github.com/SimonMorphy/godis/config"
	database2 "github.com/SimonMorphy/godis/database"
	"github.com/SimonMorphy/godis/interface/database"
	"github.com/SimonMorphy/godis/interface/resp"
	"github.com/SimonMorphy/godis/lib/consistentHash"
	"github.com/SimonMorphy/godis/lib/logger"
	"github.com/SimonMorphy/godis/resp/reply"
	pool "github.com/jolestar/go-commons-pool/v2"
	"strings"
)

type ClusterDatabase struct {
	self           string
	nodes          []string
	peerPicker     *consistentHash.NodeMap
	peerConnection map[string]*pool.ObjectPool
	db             database.Database
}

func MakeClusterDatabase() *ClusterDatabase {
	c := &ClusterDatabase{
		self:           config.Properties.Self,
		db:             database2.NewStandAloneDatabase(),
		peerPicker:     consistentHash.NewNodeMap(nil),
		peerConnection: make(map[string]*pool.ObjectPool),
	}
	nodes := make([]string, 0, len(config.Properties.Peers)+1)
	for _, peer := range config.Properties.Peers {
		nodes = append(nodes, peer)
	}
	nodes = append(nodes, config.Properties.Self)
	c.peerPicker.AddNodes(nodes...)
	ctx := context.Background()
	for _, peer := range config.Properties.Peers {
		c.peerConnection[peer] = pool.NewObjectPoolWithDefaultConfig(ctx, &connectFactory{Peer: peer})
	}
	c.nodes = nodes
	return c
}

type CmdFunc func(clusterDatabase *ClusterDatabase, connection resp.Connection, args [][]byte) resp.Reply

var router = MakeRouter()

func (c *ClusterDatabase) Exec(client resp.Connection, args [][]byte) (result resp.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err)
			result = &reply.UnKnownErrorReply{}
		}
	}()
	cmdName := strings.ToLower(string(args[0]))
	cmdFunc, ok := router[cmdName]
	if !ok {
		return reply.MakeErrReply("NOT SUPPORT CMD")
	}
	result = cmdFunc(c, client, args)
	return
}

func (c *ClusterDatabase) Close() {
}

func (c *ClusterDatabase) AfterClientClose(con resp.Connection) {
}
