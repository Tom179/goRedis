package database

import (
	"fmt"
	"goRedis/aof"
	"goRedis/config"
	database2 "goRedis/interface/database"
	"goRedis/interface/resp"
	"goRedis/lib/logger"
	"goRedis/lib/utils"
	"goRedis/resp/reply"
	"strconv"
	"strings"
)

type Database struct {
	dbSet      []*RedisDb
	aofHandler *aof.AofHandler //持久化对象
}

func NewDataBase() *Database {
	database := &Database{}
	if config.Properties.Databases <= 0 { // 默认16个数据库
		config.Properties.Databases = 16
	}
	database.dbSet = make([]*RedisDb, config.Properties.Databases)
	for i := 0; i < config.Properties.Databases; i++ {
		db := NewRedisDb()
		db.SetId(i)
		database.dbSet[i] = db
	}

	if config.Properties.AppendOnly { //
		aofHandler, err := aof.NewAofHandler(database) //打开文件，新建一个goroutine从AofChan中取指令来写文件，（调用AddAof才会写入AofChan），所以会阻塞到AofChan传值为止
		if err != nil {
			panic(err) //严重错误直接抛出panic
		}
		database.aofHandler = aofHandler
		/*for _, db := range database.dbSet {
			sdb := *db
			fmt.Println(db.Id)
			sdb.AddAof = func(line database2.CmdLine) { //仅声明，未调用
				fmt.Println("初始化该addAof函数时，预期db值为：", sdb.Id)
				database.aofHandler.AddAof(sdb.Id, line)
			}
		}*/
		for _, db := range database.dbSet {
			sdb := db                                  // 创建一个局部变量 sdb
			db.AddAof = func(line database2.CmdLine) { //仅声明，未调用
				//fmt.Println("调用该DB号时，实际的DB号为：", sdb.Id, aofHandler.Loading)
				database.aofHandler.AddAof(sdb.Id, line, aofHandler.Loading)
			}
		}
		aofHandler.LoadAof()
	}
	return database
}

func (db *Database) Exec(client resp.Connection, args [][]byte) resp.Reply {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(fmt.Sprintf("panic: %v", err))
		}
	}()

	cmdName := string(args[0])
	cmdName = strings.ToLower(cmdName)
	if cmdName == "select" {
		if len(args) != 2 {
			return reply.NewArgNumErrReply("select")
		}
		return Select(client, db, args[1:]) //更改Conn的selectedDB
	}
	dbIndex := client.GetDBIndex() //获取Conn的selectedDB
	fmt.Printf("上层：Conn层的selectedDB为%d，所以执行dbSet[%d].Exec方法,args为%s\n", dbIndex, dbIndex, utils.BytesToStrings(args))

	if dbIndex < 0 || dbIndex >= len(db.dbSet) {
		return reply.NewStandardErrReply("ERR DB index out of range")
	}

	db.dbSet[dbIndex].AddAof(args)
	return db.dbSet[dbIndex].Exec(client, args)
}

func (db *Database) Close() error {
	return nil
}

func (db *Database) AfterClientClose(client resp.Connection) error {
	return nil
}

// Select 选择数据库
func Select(conn resp.Connection, db *Database, args [][]byte) resp.Reply {
	dbIndex, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return reply.NewStandardErrReply("ERR invalid DB index")
	}
	if dbIndex < 0 || dbIndex >= len(db.dbSet) {
		return reply.NewStandardErrReply("ERR DB index out of range")
	}
	conn.SelectDB(dbIndex)
	return reply.NewOkReply()
}
