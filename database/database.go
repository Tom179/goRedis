package database

import (
	"fmt"
	"goRedis/aof"
	"goRedis/config"
	database2 "goRedis/interface/database"
	"goRedis/interface/resp"
	"goRedis/lib/logger"
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
		for _, db := range database.dbSet {
			db.AddAof = func(line database2.CmdLine) { //仅声明，未调用，现在的id、Loaing是固定不变的,记得在调用之前手动改一下
				database.aofHandler.AddAof(db.Id, line, aofHandler.Loading)
			}
		}
		database.RegistAofCmd() //指定Aof指令
		aofHandler.LoadAof()
	}
	return database
}

func (db *Database) RegistAofCmd() { //
	db.aofHandler.AofCmd["set"] = true
	db.aofHandler.AofCmd["del"] = true
	db.aofHandler.AofCmd["setnx"] = true
	db.aofHandler.AofCmd["getset"] = true
	db.aofHandler.AofCmd["flushdb"] = true
	db.aofHandler.AofCmd["rename"] = true
	db.aofHandler.AofCmd["renamenx"] = true

	//hash
	db.aofHandler.AofCmd["hget"] = true
	db.aofHandler.AofCmd["hdel"] = true

	//list
	db.aofHandler.AofCmd["lpush"] = true
	db.aofHandler.AofCmd["rpush"] = true
	db.aofHandler.AofCmd["lpop"] = true
	db.aofHandler.AofCmd["rpop"] = true

	//set
	db.aofHandler.AofCmd["sadd"] = true
	db.aofHandler.AofCmd["srem"] = true

	//db.aofHandler.AofCmd["del"] = true

}
func (db *Database) Exec(client resp.Connection, args [][]byte) resp.Reply {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(fmt.Sprintf("panic: %v", err))
		}
	}()

	cmdName := strings.ToLower(string(args[0]))
	if cmdName == "select" {
		//fmt.Println("Exec读取到select串，修改Conn的DB到", utils.BytesToStrings(args[1:]))
		if len(args) != 2 {
			return reply.NewArgNumErrReply("select")
		}
		return Select(client, db, args[1:]) //直接从Conn读取str，手动修改Conn的selectedDB
	}

	ConnIndex := client.GetDBIndex()   //获取Conn的DB
	if db.aofHandler.AofCmd[cmdName] { //如果cmd是写指令
		db.dbSet[ConnIndex].AddAof(args) //会写入到aofChan中。当前dbIndex一定是正确的，因为Conn连接中的id是强更新强一致的。
		//所以发送到AofChan中的id是最新的，【但是要在非select命令的时候才会发送最新id】
	}
	return db.dbSet[ConnIndex].Exec(client, args)
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
