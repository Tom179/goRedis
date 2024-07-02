package cluster

import (
	"context"
	"errors"
	pool "github.com/jolestar/go-commons-pool"
	"goRedis/resp/client"
)

type connectionFactory struct {
	Peer string
}

func (f connectionFactory) MakeObject(ctx context.Context) (*pool.PooledObject, error) {
	Client, err := client.MakeClient(f.Peer)
	if err != nil {
		return nil, err
	}
	Client.Start()
	return pool.NewPooledObject(Client), nil
}

func (f connectionFactory) DestroyObject(ctx context.Context, object *pool.PooledObject) error {
	c, ok := object.Object.(*client.Client)
	if !ok {
		return errors.New("类型转换出错")
	}
	c.Close()
	return nil
}

func (f connectionFactory) ValidateObject(ctx context.Context, object *pool.PooledObject) bool {
	return true
}

func (f connectionFactory) ActivateObject(ctx context.Context, object *pool.PooledObject) error {
	return nil
}

func (f connectionFactory) PassivateObject(ctx context.Context, object *pool.PooledObject) error {
	return nil
}
