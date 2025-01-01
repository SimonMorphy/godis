package aof

import (
	"github.com/SimonMorphy/godis/config"
	"github.com/SimonMorphy/godis/interface/database"
	"github.com/SimonMorphy/godis/lib/logger"
	"github.com/SimonMorphy/godis/lib/utils"
	"github.com/SimonMorphy/godis/resp/connection"
	"github.com/SimonMorphy/godis/resp/parser"
	"github.com/SimonMorphy/godis/resp/reply"
	"io"
	"os"
	"strconv"
)

type payload struct {
	cmdLine database.CmdLine
	DbIndex int
}

const aofBufferSize = 1 << 16

type AofHandler struct {
	database    database.Database
	aofChan     chan *payload
	aofFile     *os.File
	aofFileName string
	currentDB   int
}

func NewAofHandler(d database.Database) (*AofHandler, error) {
	file, err := os.OpenFile(config.Properties.AppendFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		logger.Error("无法开启AOF功能，文件权限受限")
		return nil, err
	}
	handler := AofHandler{
		database:    d,
		aofFileName: config.Properties.AppendFilename,
		aofFile:     file,
		aofChan:     make(chan *payload, aofBufferSize),
	}
	handler.LoadAof()
	go func() {
		handler.handleAof()
	}()
	return &handler, nil
}

func (h *AofHandler) AddAof(dbIndex int, line database.CmdLine) {
	if config.Properties.AppendOnly && h.aofChan != nil {
		h.aofChan <- &payload{
			cmdLine: line,
			DbIndex: dbIndex,
		}
	}
}

func (h *AofHandler) handleAof() {
	h.currentDB = 0
	for p := range h.aofChan {
		if p.DbIndex != h.currentDB {
			data := reply.MakeMultiBulkReply(utils.ToCmdLine("select", strconv.Itoa(h.currentDB))).ToBytes()
			_, err := h.aofFile.Write(data)
			if err != nil {
				logger.Error("AOF落盘失败")
				continue
			}
			h.currentDB = p.DbIndex
		}
		data := reply.MakeMultiBulkReply(p.cmdLine).ToBytes()
		_, err := h.aofFile.Write(data)
		if err != nil {
			logger.Error("AOF落盘失败")
			continue
		}
	}
}

func (h *AofHandler) LoadAof() {
	f, err := os.Open(h.aofFileName)
	c := &connection.Connection{}
	if err != nil {
		logger.Error(err)
		return
	}
	defer f.Close()
	ch := parser.ParseStream(f)
	for p := range ch {
		if p.Err != nil {
			if p.Err == io.EOF {
				break
			}
			logger.Error(p.Err)
			continue
		}
		if p.Data == nil {
			logger.Error("空指令")
			continue
		}
		r, ok := p.Data.(*reply.MultiBulkReply)
		if !ok {
			logger.Error("非多行协议")
			continue
		}
		rep := h.database.Exec(c, r.Args)
		if reply.IsErrReply(rep) {
			logger.Error(rep)
		}
	}
}
