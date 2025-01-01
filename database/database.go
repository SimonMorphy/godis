package database

import (
	"github.com/SimonMorphy/godis/aof"
	"github.com/SimonMorphy/godis/config"
	database2 "github.com/SimonMorphy/godis/interface/database"
	"github.com/SimonMorphy/godis/interface/resp"
	"github.com/SimonMorphy/godis/lib/logger"
	"github.com/SimonMorphy/godis/resp/reply"
	"strconv"
	"strings"
)

type Database struct {
	dbSet      []*DB
	aofHandler *aof.AofHandler
}

func NewDatabase() *Database {
	database := &Database{}
	if config.Properties.Databases == 0 {
		config.Properties.Databases = 16
	}
	database.dbSet = make([]*DB, config.Properties.Databases)
	for i := range database.dbSet {
		db := MakeDB()
		db.index = i
		database.dbSet[i] = db
	}
	if config.Properties.AppendOnly {
		handler, err := aof.NewAofHandler(database)
		if err != nil {
			panic(err)
		}
		database.aofHandler = handler
		for _, db := range database.dbSet {
			sdb := db
			sdb.addAof = func(line database2.CmdLine) {
				database.aofHandler.AddAof(sdb.index, line)
			}
		}
	}
	return database
}

func (d *Database) Exec(client resp.Connection, args [][]byte) resp.Reply {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err)
		}
	}()
	command := strings.ToLower(string(args[0]))
	if command == "select" {
		if len(args) != 2 {
			return reply.MakeArgNumErrReply("select")
		}
		return execSelect(client, d, args)
	}
	index := client.GetDBIndex()
	return d.dbSet[index].Exec(client, args)
}

func (d *Database) Close() {
}

func (d *Database) AfterClientClose(c resp.Connection) {
}

func execSelect(connection resp.Connection, database *Database, args [][]byte) resp.Reply {
	dbNum, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return reply.MakeErrReply("ERR invalid DB index")
	}
	if dbNum >= len(database.dbSet) || dbNum < 0 {
		return reply.MakeErrReply("ERR DB index is out of range")
	}
	connection.SelectDB(dbNum)
	return reply.MakeOKReply()
}
