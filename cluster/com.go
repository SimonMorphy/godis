package cluster

import (
	"context"
	"errors"
	"github.com/SimonMorphy/godis/interface/resp"
	"github.com/SimonMorphy/godis/lib/utils"
	"github.com/SimonMorphy/godis/resp/client"
	"github.com/SimonMorphy/godis/resp/reply"
	"strconv"
)

func (c *ClusterDatabase) relay(peer string, connection resp.Connection, args [][]byte) resp.Reply {
	if peer == c.self {
		return c.db.Exec(connection, args)
	}
	peerClient, err := c.getPeerClient(peer)
	if err != nil {
		return reply.MakeErrReply(err.Error())
	}
	defer func() {
		_ = c.returnPeerClient(peer, peerClient)
	}()
	peerClient.Send(utils.ToCmdLine("SELECT", strconv.Itoa(connection.GetDBIndex())))
	return peerClient.Send(args)
}

func (c *ClusterDatabase) broadcast(con resp.Connection, args [][]byte) map[string]resp.Reply {
	res := make(map[string]resp.Reply)
	for _, node := range c.nodes {
		result := c.relay(node, con, args)
		res[node] = result
	}
	return res
}
func (c *ClusterDatabase) getPeerClient(peer string) (*client.Client, error) {
	pool, ok := c.peerConnection[peer]
	if !ok {
		return nil, errors.New("无此连接")
	}
	object, err := pool.BorrowObject(context.Background())
	if err != nil {
		return nil, err
	}
	cli, ok := object.(*client.Client)
	if !ok {
		return nil, errors.New("类型不匹配")
	}
	return cli, err
}
func (c *ClusterDatabase) returnPeerClient(peer string, cli *client.Client) error {
	pool, ok := c.peerConnection[peer]
	if !ok {
		return errors.New("连接不存在")
	}
	err := pool.ReturnObject(context.Background(), cli)
	if err != nil {
		return err
	}
	return nil
}
