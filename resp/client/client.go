package client

import (
	"errors"
	"fmt"
	"goRedis/interface/resp"
	"goRedis/lib/logger"
	"goRedis/lib/sync/wait"
	"goRedis/resp/parser"
	"goRedis/resp/reply"
	"net"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	created = iota
	running
	closed
)

// 此Client是管道模式的 redis 客户端
type Client struct {
	conn        net.Conn
	pendingReqs chan *request // wait to send        等待发送
	watingResp  chan *request // waiting response    等待响应
	ticker      *time.Ticker
	addr        string

	status  int32
	working *sync.WaitGroup // its counter presents unfinished requests(pending and waiting)
}

type request struct {
	id        uint64
	args      [][]byte
	reply     resp.Reply
	heartbeat bool
	waiting   *wait.Wait
	err       error
}

const (
	chanSize = 256
	maxWait  = 3 * time.Second
)

// MakeClient creates a new client
func MakeClient(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Client{
		addr:        addr,
		conn:        conn,
		pendingReqs: make(chan *request, chanSize),
		watingResp:  make(chan *request, chanSize),
		working:     &sync.WaitGroup{},
	}, nil
}

// Start starts asynchronous goroutines
func (client *Client) Start() {
	client.ticker = time.NewTicker(10 * time.Second) //心跳通道

	go client.handleWrite() //监听pendingReq通道的请求并，将请求写入到client.conn和watingResp中，暂未响应。
	go client.handleRead()  //从client.conn中读取数据并将其填入到从WatingResp获取的request中。request从等待响watingResp中移出，代表已成功响应
	go client.heartbeat()   //每10秒发送ping命令到pendingReq

	atomic.StoreInt32(&client.status, running)
}

// Close stops asynchronous goroutines and close connection
func (client *Client) Close() {
	atomic.StoreInt32(&client.status, closed)
	client.ticker.Stop()

	close(client.pendingReqs) // stop new request

	client.working.Wait() //等待所有woking归零，等待所有任务完成

	_ = client.conn.Close() //关闭
	close(client.watingResp)
}

func (client *Client) reconnect() {
	logger.Info("reconnect with: " + client.addr)
	_ = client.conn.Close() // ignore possible errors from repeated closes

	var conn net.Conn
	for i := 0; i < 3; i++ {
		var err error
		conn, err = net.Dial("tcp", client.addr)
		if err != nil {
			logger.Error("reconnect error: " + err.Error())
			time.Sleep(time.Second)
			continue
		} else {
			break
		}
	}
	if conn == nil { // reach max retry, abort
		client.Close()
		return
	}
	client.conn = conn

	close(client.watingResp)
	for req := range client.watingResp {
		req.err = errors.New("connection closed")
		req.waiting.Done()
	}
	client.watingResp = make(chan *request, chanSize)
	// restart handle read
	go client.handleRead()
}

func (client *Client) heartbeat() {
	for range client.ticker.C {
		client.doHeartbeat()
	}
}

func (client *Client) handleWrite() {
	for req := range client.pendingReqs { //遍历待发送的管道
		client.doRequest(req)
	}
}

// 发送请求,发送到pendingReqs通道而已
func (client *Client) Send(args [][]byte) resp.Reply {
	if atomic.LoadInt32(&client.status) != running { //获取运行状态
		return reply.NewStandardErrReply("client closed")
	}
	req := &request{
		args:      args,
		heartbeat: false,
		waiting:   &wait.Wait{},
	}
	req.waiting.Add(1)
	client.working.Add(1)       //wg+1
	defer client.working.Done() //wg-1，表示一个发送pendingReqs任务完成，清零时表示client任务完成
	client.pendingReqs <- req
	timeout := req.waiting.WaitWithTimeout(maxWait)
	if timeout {
		return reply.NewStandardErrReply("server time out")
	}
	if req.err != nil {
		return reply.NewStandardErrReply("request faild" + req.err.Error())
	}
	return req.reply //等待handleRead函数调用finishReq将reply填入，若超时则返回Err，不会到达这段代码
}

func (client *Client) doHeartbeat() {
	request := &request{
		args:      [][]byte{[]byte("PING")},
		heartbeat: true,
		waiting:   &wait.Wait{},
	}
	request.waiting.Add(1)
	client.working.Add(1)
	defer client.working.Done()
	client.pendingReqs <- request            //阻塞到request.wating通过后就关闭：不管request是否被发送到pendingReqs，等待指定时间
	request.waiting.WaitWithTimeout(maxWait) //在此等待时间结束
}

func (client *Client) doRequest(req *request) {
	if req == nil || len(req.args) == 0 {
		return
	}
	re := reply.NewMultiBulkReply(req.args) //封装一个数据库Reply
	bytes := re.ToBytes()
	var err error
	for i := 0; i < 3; i++ { // only retry, waiting for handleRead
		_, err = client.conn.Write(bytes) //写入到Conn
		if err == nil ||
			(!strings.Contains(err.Error(), "timeout") && // only retry timeout
				!strings.Contains(err.Error(), "deadline exceeded")) {
			break
		}
	}
	if err == nil { //写成功
		fmt.Println("写入到连接成功！！！,将req发送到WatingResp通道")
		client.watingResp <- req
	} else {
		req.err = err
		req.waiting.Done() //请求成功处理
	}
}

func (client *Client) finishRequest(reply resp.Reply) { //client.从watingResp管道中获取Request结构体，并把reply参数填入到Request中，request标记为Done()减一
	defer func() {
		if err := recover(); err != nil {
			debug.PrintStack()
			logger.Error(err)
		}
	}()
	request := <-client.watingResp
	if request == nil {
		return
	}
	request.reply = reply
	if request.waiting != nil {
		request.waiting.Done()
	}
}

func (client *Client) handleRead() {
	ch := parser.ParseStream(client.conn) //从conn中读指令
	for payload := range ch {
		if payload.Err != nil {
			status := atomic.LoadInt32(&client.status) //原子读取conn的状态
			if status == closed {
				return
			}
			client.reconnect()
			return
		}
		client.finishRequest(payload.Data)
	}
}
