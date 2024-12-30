package database

import "strings"

var commandTable = make(map[string]*Command)

type Command struct {
	executor ExecFunc
	arity    int
}

func RegisterCommand(name string, executor ExecFunc, arity int) {
	name = strings.ToLower(name)
	commandTable[name] = &Command{
		executor: executor,
		arity:    arity,
	}
}
