package database

import (
	"github.com/SimonMorphy/godis/interface/database"
	"github.com/SimonMorphy/godis/interface/resp"
	"github.com/SimonMorphy/godis/lib/utils"
	"github.com/SimonMorphy/godis/resp/reply"
)

// GET / SET / SETNX / GETSET

func Get(db *DB, args CommandLine) resp.Reply {
	key := string(args[0])
	entity, exists := db.GetEntity(key)
	if !exists {
		return reply.MakeNullBulkReply()
	}
	if entity.Data == nil {
		return reply.MakeNullBulkReply()
	}
	bytes := entity.Data.(string)
	return reply.MakeBulkReply([]byte(bytes))
}
func Set(db *DB, args CommandLine) resp.Reply {
	key := string(args[0])
	value := string(args[1])
	entity := &database.DataEntity{
		Data: value,
	}
	db.PutEntity(key, entity)
	db.addAof(utils.ToCmdLine3("set", args...))
	return reply.MakeOKReply()
}
func SetNX(db *DB, args CommandLine) resp.Reply {
	key := string(args[0])
	value := string(args[1])
	entity := &database.DataEntity{
		Data: value,
	}
	absent := db.PutIfAbsent(key, entity)
	db.addAof(utils.ToCmdLine3("setnx", args...))
	return reply.MakeIntReply(int64(absent))
}
func GetSet(db *DB, args CommandLine) resp.Reply {
	key := string(args[0])
	value := string(args[1])
	entity, e := db.GetEntity(key)
	if !e {
		return reply.MakeNullBulkReply()
	}
	db.PutEntity(key, &database.DataEntity{
		Data: value,
	})
	db.addAof(utils.ToCmdLine3("getset", args...))
	return reply.MakeBulkReply([]byte(entity.Data.(string)))
}
func StrLen(db *DB, args CommandLine) resp.Reply {
	key := string(args[0])
	entity, e := db.GetEntity(key)
	if !e {
		return reply.MakeNullBulkReply()
	}
	bytes := entity.Data.([]byte)
	return reply.MakeIntReply(int64(len(bytes)))
}

func init() {
	RegisterCommand("SET", Set, 3)
	RegisterCommand("GET", Get, 2)
	RegisterCommand("SETNX", SetNX, 3)
	RegisterCommand("GETSET", GetSet, 3)
	RegisterCommand("STRLEN", StrLen, 2)

}
