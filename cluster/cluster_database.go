package cluster //集群数据库：在这层做请求转发。底层的单机数据库为standAlone_database

import (
	"context"
	"fmt"
	pool "github.com/jolestar/go-commons-pool"
	"github.com/spaolacci/murmur3"
	"goRedis/config"
	standAloneDatabase "goRedis/database"
	"goRedis/interface/database"
	"goRedis/interface/resp"
	"goRedis/lib/consistentHash"
	"goRedis/lib/logger"
	"goRedis/resp/reply"
	"strings"
)

type ClusterDatabase struct { //Cluster节点:A要维护一组对B、一组对C节点的客户端。并发获取多个连接而不是一个连接。
	self           string
	nodes          []string                    //记录集群中所有的节点
	peerPicker     *consistentHash.NodeMap     //一致性哈希管理器，可以判空、添加节点、选择节点
	peerConnection map[string]*pool.ObjectPool //连接池                         //ConnectionPool//每个cluster节点都需要多个链接，用连接池维护
	db             database.Database           //底层的单机数据库
}

func NewClusterDatabase() *ClusterDatabase {
	cluster := &ClusterDatabase{
		self:           config.Properties.Self,
		db:             standAloneDatabase.NewDataBase(),
		peerPicker:     consistentHash.NewNodeMap(murmur3.Sum32), //默认哈希函数
		peerConnection: make(map[string]*pool.ObjectPool),
	}
	nodes := make([]string, 0, len(config.Properties.Peers)+1)
	for _, peer := range config.Properties.Peers { //遍历配置中兄弟结点的ip
		nodes = append(nodes, peer)
	}
	nodes = append(nodes, config.Properties.Self)

	for _, node := range nodes {
		cluster.peerPicker.AddNode(node) //todo 结点ip的哈希值得很紧怎么办？。而Key的hash值太散，会插不进去。
		cluster.peerConnection[node] = pool.NewObjectPoolWithDefaultConfig(context.Background(), &connectionFactory{
			Peer: node,
		})
	}
	//Dugug
	cluster.peerPicker.ShowNodeHashMap()

	cluster.nodes = nodes
	return cluster
}

// cluster集群执行命令的函数
type CmdFunc func(cluster *ClusterDatabase, c resp.Connection, cmdArgs [][]byte) resp.Reply

func (cluster *ClusterDatabase) Exec(client resp.Connection, args [][]byte) (result resp.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err)
			result = reply.NewUnknownErrReply()
		}
	}()
	cmdName := strings.ToLower(string(args[0])) //识别指令类型
	cmdFunc, ok := CommandRouter[cmdName]
	if !ok {
		fmt.Println("集群层指令未注册")
	}
	result = cmdFunc(cluster, client, args)
	return
}

func (cluster *ClusterDatabase) Close() error {
	return cluster.db.Close()
}

func (cluster *ClusterDatabase) AfterClientClose(client resp.Connection) error {
	return cluster.db.AfterClientClose(client)
}
