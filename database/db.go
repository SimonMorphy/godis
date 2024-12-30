package database

import (
	"strings"

	"github.com/SimonMorphy/godis/datastruct/dict"
	"github.com/SimonMorphy/godis/interface/database"
	"github.com/SimonMorphy/godis/interface/resp"
	"github.com/SimonMorphy/godis/resp/reply"
)

type DB struct {
	index int
	data  dict.Dict
}

type CommandLine = [][]byte

func MakeDB() *DB {
	return &DB{
		data: dict.MakeSyncDic(),
	}
}

type ExecFunc func(db *DB, args CommandLine) resp.Reply

func (d *DB) Exec(c resp.Connection, line database.CmdLine) resp.Reply {
	cmdName := strings.ToLower(string(line[0]))
	cmd, ok := commandTable[cmdName]
	if !ok {
		return reply.MakeErrReply("Err unknown command " + cmdName)
	}
	if !validateArity(cmd.arity, line) {
		return reply.MakeArgNumErrReply(cmdName)
	}
	fun := cmd.executor
	return fun(d, line[1:])
}

func validateArity(arity int, cmdArgs [][]byte) bool {
	argNum := len(cmdArgs)
	if arity >= 0 {
		return argNum == arity
	}
	return argNum >= -arity
}

func (d *DB) GetEntity(key string) (*database.DataEntity, bool) {
	val, exists := d.data.Get(key)
	if !exists {
		return nil, false
	}
	if entity, ok := val.(*database.DataEntity); ok {
		return entity, true
	}
	return nil, false
}

func (d *DB) PutEntity(key string, val *database.DataEntity) int {
	return d.data.Put(key, val)
}
func (d *DB) PutIfExist(key string, val *database.DataEntity) int {
	return d.data.PutIfExist(key, val)
}
func (d *DB) PutIfAbsent(key string, val *database.DataEntity) int {
	return d.data.PutIfAbsent(key, val)
}
func (d *DB) Remove(key string) {
	d.data.Remove(key)
}
func (d *DB) Removes(keys ...string) int {
	deleted := 0
	for _, k := range keys {
		_, exists := d.data.Get(k)
		if exists {
			d.Remove(k)
			deleted++
		}
	}
	return deleted
}

func (d *DB) Flush() {
	d.data.Clear()
}
