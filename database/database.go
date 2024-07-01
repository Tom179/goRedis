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
		database.dbSet[i].AddAof = func(line database2.CmdLine) { //仅声明，未调用
			database.aofHandler.AddAof(i, line)
		}
		if config.Properties.AppendOnly { //
			aofHandler, err := aof.NewAofHandler(database)
			if err != nil {
				panic(err) //严重错误直接抛出panic
			}
			database.aofHandler = aofHandler

		}
	}
	/*	fmt.Println("测试")
		database.dbSet[1].AddAof(utils.ToCmdLine("haha", "yes"))////这样就可以》》》》？？
		fmt.Println("测试")*/
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
		return Select(client, db, args[1:]) //读取到select指令,更改最上层连接层的selectedDB：数据库编号
	}
	dbIndex := client.GetDBIndex()
	logger.Debug("Conn层的selectedDB为", dbIndex)

	if dbIndex < 0 || dbIndex >= len(db.dbSet) {
		return reply.NewStandardErrReply("ERR DB index out of range")
	}
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
