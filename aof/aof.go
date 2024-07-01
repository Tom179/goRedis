package aof

import (
	"fmt"
	"goRedis/config"
	"goRedis/interface/database"
	"goRedis/lib/logger"
	"goRedis/lib/utils"
	"goRedis/resp/connection"
	"goRedis/resp/parser"
	"goRedis/resp/reply"
	"io"
	"os"
	"strconv"
)

type CmdLine = [][]byte

const aofBufferSize = 1 << 16

type payload struct {
	cmdLine CmdLine
	dbIndex int
}

type AofHandler struct {
	database    database.Database
	aofChan     chan *payload //存储引擎写操作时，传递消息
	aofFile     *os.File
	aofFileName string
	currentDB   int //维护当前库的id
	Loading     utils.SafeBool
}

func NewAofHandler(database database.Database) (*AofHandler, error) {
	handler := &AofHandler{}
	handler.aofFileName = config.Properties.AppendFilename
	handler.database = database
	//handler.LoadAof() //loadAof。当调用NewAofHandler时，是启动操作。先把写在硬盘上的aof文件恢复到内存中来。

	aofFile, err := os.OpenFile(handler.aofFileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600) //
	if err != nil {
		return nil, err
	}
	handler.aofFile = aofFile
	handler.aofChan = make(chan *payload, aofBufferSize)
	go func() {
		handler.handleAof()
	}()
	return handler, nil
}

// ↓异步落盘\持久化
func (handler *AofHandler) AddAof(dbIndex int, cmd CmdLine) { //传入：几号DB数据库
	logger.Debugf("调用AddAof，命令为%s,发送到chan的预期dbIndex为%d\n", utils.BytesToStrings(cmd), dbIndex)
	fmt.Println("调用AddAof，命令为%s,发送到chan的预期dbIndex为%d")
	if config.Properties.AppendOnly && handler.aofChan != nil {
		//新建pyload
		handler.aofChan <- &payload{ //将传入参数组装为payload并传到channel
			cmdLine: cmd,
			dbIndex: dbIndex,
		}
	}
}

// 接收aofChan中的payload
func (handler *AofHandler) handleAof() { //将aofChan中的命令持久化。、修改vDB编号等
	fmt.Println("进入handleAof函数")
	handler.currentDB = 0

	for p := range handler.aofChan {
		//fmt.Println("aofChan中传递的预取id为", p.dbIndex)
		if p.dbIndex != handler.currentDB {
			if !handler.Loading.Get() { //不是恢复模式就写
				args := utils.ToCmdLine("select", strconv.Itoa(p.dbIndex))
				data := reply.NewMultiBulkReply(args).ToBytes() //得到写入文件的字节
				_, err := handler.aofFile.Write(data)
				if err != nil {
					logger.Error(err)
					continue
				}
			}
			handler.currentDB = p.dbIndex
			fmt.Println("db编号修改为:", handler.currentDB)
		}
		if !handler.Loading.Get() { //不是恢复模式就写
			data := reply.NewMultiBulkReply(p.cmdLine).ToBytes()
			_, err := handler.aofFile.Write(data)
			if err != nil {
				logger.Error(err)
			}
		}
	}
	handler.Loading.Set(false)
}

// loadAof
func (handler *AofHandler) LoadAof() {
	handler.Loading.Set(true)
	fmt.Println("修改loding为true")
	file, err := os.Open(handler.aofFileName) //封装了openFile()函数，以只读的方式打开文件
	if err != nil {
		logger.Error(err)
		return
	}
	defer file.Close() //恢复加载后就关闭文件
	ch := parser.ParseStream(file)
	for p := range ch {
		if p.Err != nil {
			if p.Err == io.EOF {
				break
			}
			logger.Error(p.Err)
			continue
		}
		if p.Data == nil {
			logger.Error("empty payload")
			continue
		}
		r, ok := p.Data.(*reply.MultiBulkReply) //类型断言和强转的区别在于，只是尝试转换，会告诉是否转换失败
		if !ok {
			logger.Error("need Mutibulk type")
			continue
		}
		tempConn := &connection.RESPConn{}

		logger.Debugf("从ch中获取的payload.arg为一条指令:%s currentDB:%d", utils.BytesToStrings(r.Args), handler.currentDB)

		rep := handler.database.Exec(tempConn, r.Args)
		if reply.IsErrReply(rep) {
			logger.Error(rep)
		}
	}
	fmt.Println("修改loding为false")
}
