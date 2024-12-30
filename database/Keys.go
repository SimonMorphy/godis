package database

import (
	"github.com/SimonMorphy/godis/datastruct/dict"
	"github.com/SimonMorphy/godis/interface/resp"
	"github.com/SimonMorphy/godis/lib/wildcard"
	"github.com/SimonMorphy/godis/resp/reply"
)

// Del
func Del(db *DB, args CommandLine) resp.Reply {
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}
	deleted := db.Removes(keys...)
	return reply.MakeIntReply(int64(deleted))
}

// Exist
func Exists(db *DB, args CommandLine) resp.Reply {
	result := int64(0)
	for _, arg := range args {
		key := string(arg)
		_, e := db.GetEntity(key)
		if e {
			result++
		}
	}
	return reply.MakeIntReply(result)
}

// FlushDB
func FlushDB(db *DB, args CommandLine) resp.Reply {
	db.data.Clear()
	return reply.MakeOKReply()
}

// TYPE
func Type(db *DB, args CommandLine) resp.Reply {
	key := string(args[0])
	entity, e := db.GetEntity(key)
	if !e {
		return reply.MakeStatusReply("none")
	}
	switch entity.Data.(type) {
	case []byte:
		return reply.MakeStatusReply("string")
	case dict.Dict:
		return reply.MakeStatusReply("map")
	}
	return &reply.UnKnownErrorReply{}
}

// Rename
func Rename(db *DB, args CommandLine) resp.Reply {
	src := string(args[0])
	dest := string(args[1])
	entity, e := db.GetEntity(src)
	if !e {
		return reply.MakeErrReply("该数据不存在")
	}
	db.PutEntity(dest, entity)
	db.Remove(src)
	return reply.MakeOKReply()
}

func Renamenx(db *DB, args CommandLine) resp.Reply {
	src := string(args[0])
	dest := string(args[1])
	_, ok := db.GetEntity(dest)
	if ok {
		return reply.MakeIntReply(0)
	}
	entity, e := db.GetEntity(src)
	if !e {
		return reply.MakeErrReply("该数据不存在")
	}
	db.PutEntity(dest, entity)
	db.Remove(src)
	return reply.MakeOKReply()
}

// Keys
func Keys(db *DB, args CommandLine) resp.Reply {
	pattern, _ := wildcard.CompilePattern(string(args[0]))
	result := make([][]byte, 0)
	db.data.ForEach(func(key string, val interface{}) bool {
		if pattern.IsMatch(key) {
			result = append(result, []byte(key))
		}
		return true
	})
	return reply.MakeMultiBulkReply(result)
}

func init() {
	RegisterCommand("DEL", Del, -2)
	RegisterCommand("EXISTS", Exists, -2)
	RegisterCommand("FLUSHDB", FlushDB, -1)
	RegisterCommand("TYPE", Type, 2)
	RegisterCommand("RENAME", Rename, 3)
	RegisterCommand("RENAMENX", Renamenx, 3)
	RegisterCommand("KEYS", Keys, 2)
}
