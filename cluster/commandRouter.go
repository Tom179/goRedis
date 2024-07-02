package cluster

import (
	"goRedis/interface/resp"
	"goRedis/resp/reply"
)

var router = makeRouter()

func makeRouter() map[string]CmdFunc { //string是指令
	routerMap := make(map[string]CmdFunc)
	routerMap["select"] = execSelect
	routerMap["exists"] = defultFunc
	routerMap["type"] = defultFunc
	routerMap["set"] = defultFunc
	routerMap["setnx"] = defultFunc
	routerMap["get"] = defultFunc
	routerMap["getset"] = defultFunc
	routerMap["ping"] = ping
	routerMap["rename"] = Rename
	routerMap["renamenx"] = Rename
	routerMap["flushdb"] = flushDB
	routerMap["del"] = Del

	return routerMap
}

// get key//set key v1//对一个键操作：可以直接转发
func defultFunc(cluster *ClusterDatabase, c resp.Connection, cmdArgs [][]byte) resp.Reply {
	key := string(cmdArgs[1])
	peerIp := cluster.peerPicker.PickNode(key) //找到结点地址
	return cluster.relay(peerIp, c, cmdArgs)   //转发
}

func ping(cluster *ClusterDatabase, c resp.Connection, cmdArgs [][]byte) resp.Reply {
	return cluster.db.Exec(c, cmdArgs)
}

func execSelect(cluster *ClusterDatabase, c resp.Connection, cmdArgs [][]byte) resp.Reply { //和ping一样
	return cluster.db.Exec(c, cmdArgs)
}

func Rename(cluster *ClusterDatabase, c resp.Connection, cmdArgs [][]byte) resp.Reply {
	if len(cmdArgs) != 3 {
		return reply.NewStandardErrReply("err wrong number args")
	}
	src := string(cmdArgs[1])
	dst := string(cmdArgs[2])
	srcPeer := cluster.peerPicker.PickNode(src) //源地址ip
	dstPeer := cluster.peerPicker.PickNode(dst) //目标地址ip
	if srcPeer != dstPeer {
		return reply.NewStandardErrReply("rename 应当在一个结点中")
	}
	return cluster.relay(srcPeer, c, cmdArgs)
}

// 广播
func flushDB(cluster *ClusterDatabase, c resp.Connection, cmdArgs [][]byte) resp.Reply {
	replyMap := cluster.broadcast(c, cmdArgs)
	var errReply resp.ErrorReply
	for _, r := range replyMap {
		if reply.IsErrReply(r) {
			errReply = r.(resp.ErrorReply)
			break
		}

	}
	if errReply == nil {
		return reply.NewOkReply()
	}
	return reply.NewStandardErrReply("error:" + errReply.Error())
}

// 删除del k1 k2 k3：3，回复成功删除的个数。这些key可能散布在不同结点，所以广播全部删除。
func Del(cluster *ClusterDatabase, c resp.Connection, cmdArgs [][]byte) resp.Reply {
	replyMap := cluster.broadcast(c, cmdArgs)
	var errReply resp.ErrorReply
	var deleted int64 = 0 //要转为intReply
	for _, r := range replyMap {
		if reply.IsErrReply(r) {
			errReply = r.(resp.ErrorReply)
			break
		}
		intReply, ok := r.(*reply.IntReply)
		if !ok {
			errReply = reply.NewStandardErrReply("intReply transform Error")
			break
		}
		deleted += intReply.Code
	}
	if errReply == nil {
		return reply.NewIntReply(deleted)
	}
	return reply.NewStandardErrReply("error:" + errReply.Error())
}
