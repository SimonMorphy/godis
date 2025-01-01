package cluster

import (
	"context"
	"errors"
	"github.com/SimonMorphy/godis/resp/client"
	pool "github.com/jolestar/go-commons-pool/v2"
)

type connectFactory struct {
	Peer string
}

func (c *connectFactory) MakeObject(ctx context.Context) (*pool.PooledObject, error) {
	cli, err := client.MakeClient(c.Peer)
	if err != nil {
		return nil, err
	}
	cli.Start()
	return pool.NewPooledObject(cli), nil
}

func (c *connectFactory) DestroyObject(ctx context.Context, object *pool.PooledObject) error {
	cli, ok := object.Object.(*client.Client)
	if !ok {
		return errors.New("类型不匹配")
	}
	cli.Close()
	return nil
}

func (c *connectFactory) ValidateObject(ctx context.Context, object *pool.PooledObject) bool {
	return true
}

func (c *connectFactory) ActivateObject(ctx context.Context, object *pool.PooledObject) error {
	return nil
}

func (c *connectFactory) PassivateObject(ctx context.Context, object *pool.PooledObject) error {
	return nil
}
