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
	"strings"
)

type CmdLine = [][]byte

const aofBufferSize = 1 << 16

type payload struct {
	cmdLine CmdLine
	dbIndex int
	Loading bool //添加一个标志位记录是否恢复模式
}

type AofHandler struct {
	database    database.Database
	aofChan     chan *payload //存储引擎写操作时，传递消息
	AofFile     *os.File
	aofFileName string
	currentDB   int //维护当前库的id
	Loading     bool
	AofCmd      map[string]bool
}

func NewAofHandler(database database.Database) (*AofHandler, error) {
	handler := &AofHandler{}
	handler.AofCmd = make(map[string]bool)
	handler.aofFileName = config.Properties.AppendFilename
	handler.database = database

	aofFile, err := os.OpenFile(handler.aofFileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600) //
	if err != nil {
		return nil, err
	}
	handler.AofFile = aofFile
	handler.aofChan = make(chan *payload, aofBufferSize)
	go func() {
		handler.handleAof()
	}()
	return handler, nil
}

// ↓异步落盘\持久化
func (handler *AofHandler) AddAof(dbIndex int, cmd CmdLine, isLoading bool) { //传入：几号DB数据库
	//fmt.Printf("调用AddAof，命令为%s,发送到chan的预期dbIndex为%d,是否为恢复模式:%t\n", utils.BytesToStrings(cmd), dbIndex, isLoading)
	if config.Properties.AppendOnly && handler.aofChan != nil {
		//新建pyload
		handler.aofChan <- &payload{ //将传入参数组装为payload并传到channel
			cmdLine: cmd,
			dbIndex: dbIndex,
			Loading: isLoading,
		}
	}
}

// 接收aofChan中的payload
func (handler *AofHandler) handleAof() { //将aofChan中的命令持久化。、修改vDB编号等
	fmt.Println("启动handleAof线程，监听handler.AofChan")
	handler.currentDB = 0

	for p := range handler.aofChan { //传入当前指令和上一个id
		//fmt.Printf("handle线程在AofChan中获取一条：命令为%s,恢复模式为%t\n", utils.BytesToStrings(p.cmdLine), p.Loading)
		if !p.Loading { //若不是恢复模式就写
			if p.dbIndex != handler.currentDB { //handler.currentDB：旧DB,p.dbIndex,通道传过来的最新DB?
				args := utils.ToCmdLine("select", strconv.Itoa(p.dbIndex))
				data := reply.NewMultiBulkReply(args).ToBytes()
				_, err := handler.AofFile.Write(data) //写Select命令到Aof中
				if err != nil {
					logger.Error(err)
					continue
				}
				handler.currentDB = p.dbIndex
				//fmt.Println("handler.currentDB编号修改为:", handler.currentDB)
			}
			data := reply.NewMultiBulkReply(p.cmdLine).ToBytes()
			_, err := handler.AofFile.Write(data)
			if err != nil {
				logger.Error(err)
			}
		} else {
			continue
		}

	}
	//handler.Loading.Set(false)调不到
}

// loadAof
func (handler *AofHandler) LoadAof() {
	handler.Loading = true
	//fmt.Println("修改loding为true,loding:", handler.Loading)
	file, err := os.Open(handler.aofFileName) //封装了openFile()函数，以只读的方式打开文件
	if err != nil {
		logger.Error(err)
		return
	}
	defer file.Close()             //恢复加载后就关闭文件
	ch := parser.ParseStream(file) //把流中的命令读取结束就退出
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
		//fmt.Printf("恢复文件时从ch中获取的payload.arg为一条指令:%s currentDB:%d\n", utils.BytesToStrings(r.Args), handler.currentDB)

		//fmt.Println("从文件中读取的一条指令", utils.BytesToStrings(r.Args), "准备调用Exec函数")
		tempConn := &connection.RESPConn{}
		tempConn.SelectDB(handler.currentDB) ///////////////////////////////////////////////////////////////

		rep := handler.database.Exec(tempConn, r.Args)
		if reply.IsErrReply(rep) {
			logger.Error(rep)
		}

		cmdName := strings.ToLower(string(r.Args[0])) ///////////////////////////////////////////////////
		if cmdName == "select" {
			if len(r.Args) != 2 {
				continue
			}
			handler.currentDB, _ = strconv.Atoi(string(r.Args[1]))
		}
	}
	handler.Loading = false
	//fmt.Println("修改loding为false,loding:", handler.Loading)
}
