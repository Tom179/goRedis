package cluster

import (
	"context"
	"errors"
	"goRedis/resp/client"
	"log"
)

func (cluster *ClusterDatabase) getPeerClient(peer string) (any, error) { //获取连接

	ctx := context.Background()
	pool, ok := cluster.peerConnection[peer]
	if !ok {
		return nil, errors.New("未找到连接池") //连接池为空
	}
	object, err := pool.BorrowObject(ctx)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	c, ok := object.(*client.Client)
	if !ok {
		return nil, errors.New("连接工厂类型错误")
	}

	return c, err
}

func (cluster *ClusterDatabase) returnPeerClient(peer string, peerClient *client.Client) error { //todo 传入redis连接
	pool, ok := cluster.peerConnection[peer]
	if !ok {
		return errors.New("未找到连接池")
	}
	return pool.ReturnObject(context.Background(), peerClient)

}

//func (cluster *ClusterDatabase) relay(peerIp string, c resp.Connection, args [][]byte) resp.Reply { //转发。connection是resp里面记录用户信息的conn
//	if peerIp == cluster.self {
//		return cluster.db.Exec(c, args)
//	}
//	client, err := cluster.getPeerClient(peerIp)
//	if err != nil {
//		return reply.NewStandardErrReply(err.Error())
//	}
//	defer func() {
//		_ = cluster.returnPeerClient(peerIp, client.)
//	}()
//	//todo peerClient.Send(utils.ToCmdLine("select",strconv.Itoa(c.getDBIndex())))
//	return nil //todo 返回转发的响应return client.Send(args)
//}

//func (cluster *ClusterDatabase) broadcast(c resp.Connection, args [][]byte) map[string]resp.Reply { //[][]byte是指令
//	results := make(map[string]resp.Reply)
//	for _, node := range cluster.nodes {
//		result := cluster.relay(node, c, args) //调用转发函数
//		results[node] = result
//	}
//	return results
//}
