package cluster

import (
	"context"
	"errors"
	"fmt"
	"goRedis/interface/resp"
	"goRedis/lib/utils"
	"goRedis/resp/client"
	"goRedis/resp/reply"
	"log"
	"strconv"
)

func (cluster *ClusterDatabase) getPeerClient(peer string) (*client.Client, error) { //获取连接

	pool, ok := cluster.peerConnection[peer]
	if !ok {
		return nil, errors.New("ObjectPool not found") //连接池为空
	}
	object, err := pool.BorrowObject(context.Background()) //从连接池获取连接新建对象。会自动调用MakeObject函数。其中封装了Client.Start函数
	if err != nil {
		log.Println(err)
		return nil, err
	}
	c, ok := object.(*client.Client)
	if !ok {
		return nil, errors.New("FactoryCreated TypeErr")
	}

	return c, err
}

func (cluster *ClusterDatabase) returnPeerClient(peer string, peerClient *client.Client) error {
	pool, ok := cluster.peerConnection[peer]
	if !ok {
		return errors.New("ObjectPool not found")
	}
	return pool.ReturnObject(context.Background(), peerClient)

}

func (cluster *ClusterDatabase) relay(peerIp string, c resp.Connection, args [][]byte) resp.Reply { //【转发】从连接池中根据peerIp获取客户端连接，将指令转发到该连接
	fmt.Println("进入relay函数，peerIp为", peerIp)
	if peerIp == cluster.self {
		return cluster.db.Exec(c, args)
	}
	cli, err := cluster.getPeerClient(peerIp)
	if err != nil {
		fmt.Println("获取自定义集群连接失败")
		return reply.NewStandardErrReply(err.Error())
	}
	defer func() {
		_ = cluster.returnPeerClient(peerIp, cli)
	}()

	cli.Send(utils.ToCmdLine("select", strconv.Itoa(c.GetDBIndex()))) //当前库号是否是预期的？保证切换数据库后库号一致。
	return cli.Send(args)
}

func (cluster *ClusterDatabase) broadcast(c resp.Connection, args [][]byte) map[string]resp.Reply { //[][]byte是指令
	results := make(map[string]resp.Reply)
	for _, node := range cluster.nodes {
		result := cluster.relay(node, c, args) //调用转发函数
		results[node] = result
	}
	return results
}
