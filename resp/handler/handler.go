package handler

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"

	database2 "github.com/SimonMorphy/godis/database"
	"github.com/SimonMorphy/godis/interface/database"
	"github.com/SimonMorphy/godis/lib/logger"
	"github.com/SimonMorphy/godis/lib/sync/atomic"
	"github.com/SimonMorphy/godis/resp/connection"
	"github.com/SimonMorphy/godis/resp/parser"
	"github.com/SimonMorphy/godis/resp/reply"
)

var (
	unknownErr = []byte("-ERR unknown\r\n")
)

type RespHandler struct {
	activeConn sync.Map
	db         database.Database
	closing    atomic.Boolean
}

func MakeRespHandler() *RespHandler {
	var db database.Database
	db = database2.NewDatabase()
	return &RespHandler{
		db: db,
	}
}

func (r *RespHandler) closeOne(client *connection.Connection) {
	_ = client.Close()
	r.db.Close()
	r.activeConn.Delete(client)
}

func (r *RespHandler) Handle(ctx context.Context, conn net.Conn) {
	logger.Info("")
	if r.closing.Get() {
		_ = conn.Close()
	}
	client := connection.MakeConn(conn)
	r.activeConn.Store(client, struct{}{})
	ch := parser.ParseStream(conn)
	for payload := range ch {
		if payload.Err != nil {
			if payload.Err == io.EOF ||
				errors.Is(payload.Err, io.ErrUnexpectedEOF) ||
				strings.Contains(payload.Err.Error(), "use of closed network connection") {
				r.closeOne(client)
				logger.Info("连接关闭" + client.RemoteAddr().String())
				return
			}
			errReply := reply.MakeErrReply(payload.Err.Error())
			err := client.Write(errReply.ToBytes())
			if err != nil {
				r.closeOne(client)
				logger.Info("连接中断" + client.RemoteAddr().String())
				return
			}
			continue
		}
		if payload.Data == nil {
			continue
		}
		switch reply := payload.Data.(type) {
		case *reply.MultiBulkReply:
			result := r.db.Exec(client, reply.Args)
			if result != nil {
				_ = client.Write(result.ToBytes())
			} else {
				_ = client.Write(unknownErr)
			}
		default:
			logger.Debug("收到非MultiBulkReply类型的数据：", reply)
		}
	}
}

func (r *RespHandler) Close() error {
	logger.Info("处理器关闭中......")
	r.activeConn.Range(func(key, value interface{}) bool {
		client := key.(*connection.Connection)
		_ = client.Close()
		return true
	})
	r.db.Close()
	return nil
}
