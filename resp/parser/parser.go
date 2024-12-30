package parser

import (
	"bufio"
	"errors"
	"io"
	"runtime/debug"
	"strconv"
	"strings"

	"github.com/SimonMorphy/godis/interface/resp"
	"github.com/SimonMorphy/godis/lib/logger"
	"github.com/SimonMorphy/godis/resp/reply"
)

type Payload struct {
	Data resp.Reply
	Err  error
}

type readState struct {
	readingMultiLine  bool
	expectedArgsCount int
	msgType           byte
	args              [][]byte
	bulkLine          int64
}

func (s *readState) finished() bool {
	return s.expectedArgsCount > 0 && len(s.args) == s.expectedArgsCount
}

func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	go parse0(reader, ch)
	return ch
}

func parse0(reader io.Reader, ch chan<- *Payload) {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(string(debug.Stack()))
		}
	}()
	bufReader := bufio.NewReader(reader)
	var state readState

	for {
		var ioErr bool
		msg, ioErr, err := readLine(bufReader, &state)
		if err != nil {
			if ioErr {
				ch <- &Payload{Err: err}
				close(ch)
				return
			}

			ch <- &Payload{Err: err}
			state = readState{}
			continue
		}

		if !state.readingMultiLine {
			if msg[0] == '*' {
				err = parseMultiBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{Err: errors.New("Protocol Error: " + string(msg))}
					state = readState{}
				}
				continue
			}
			// 处理单行回复
			if state.expectedArgsCount == 0 {
				result, err := parseSingleLineReply(msg)
				ch <- &Payload{
					Data: result,
					Err:  err,
				}
				state = readState{}
				continue
			}
		} else {
			// 处理多行命令
			err = readBody(msg, &state)
			if err != nil {
				ch <- &Payload{Err: err}
				state = readState{}
				continue
			}

			if state.finished() {
				result := reply.MakeMultiBulkReply(state.args)
				ch <- &Payload{
					Data: result,
					Err:  nil,
				}
				state = readState{}
			}
		}
	}
}

func readLine(reader *bufio.Reader, state *readState) ([]byte, bool, error) {
	//情况一、没有$表示预设字符长度，直接按照\r\n切分
	var msg []byte
	var err error
	if state.bulkLine == 0 {
		msg, err = reader.ReadBytes('\n')
		if err != nil {
			return nil, true, err
		}
		if len(msg) == 0 || msg[len(msg)-2] != '\r' {
			return nil, false, errors.New("Protocol Error: " + string(msg))
		}
		return msg, false, nil
	} else { //情况二、有$,先按照预设长度切分，再用\r\n切分
		msg = make([]byte, state.bulkLine+2)
		_, err := io.ReadFull(reader, msg)
		if err != nil {
			return nil, true, err
		}
		if len(msg) == 0 || msg[len(msg)-2] != '\r' || msg[len(msg)-1] != '\n' {
			return nil, false, errors.New("Protocol Error: " + string(msg))
		}
		state.bulkLine = 0
	}
	return msg, false, err
}

func parseBulkHeader(msg []byte, state *readState) (err error) {
	state.bulkLine, err = strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)
	if err != nil {
		return errors.New("Protocol Error: " + string(msg))
	}
	if state.bulkLine == -1 {
		return nil
	} else if state.bulkLine > 0 {
		state.msgType = msg[0]
		state.args = make([][]byte, 0, 1)
		state.readingMultiLine = true
		state.expectedArgsCount = 1
	} else {
		return errors.New("Protocol Error: " + string(msg))
	}
	return nil
}

func parseMultiBulkHeader(msg []byte, state *readState) error {
	var err error
	var expectedLine uint64
	expectedLine, err = strconv.ParseUint(string(msg[1:len(msg)-2]), 10, 32)
	if err != nil {
		return errors.New("Protocol Error: " + string(msg))
	}
	if expectedLine == 0 {
		state.expectedArgsCount = 0
		return nil
	} else if expectedLine > 1 {
		state.msgType = msg[0]
		state.readingMultiLine = true
		state.expectedArgsCount = int(expectedLine)
		state.args = make([][]byte, 0, expectedLine)
		return nil
	} else {
		return errors.New("Protocol Error: " + string(msg))
	}
}

func parseSingleLineReply(msg []byte) (resp.Reply, error) {
	str := strings.TrimSuffix(string(msg), "\r\n")
	var result resp.Reply
	switch msg[0] {
	case '+':
		result = reply.MakeStatusReply(str[1:])
	case '-':
		result = reply.MakeErrReply(str[1:])
	case ':':
		i, err := strconv.ParseInt(str[1:], 10, 32)
		if err != nil {
			return nil, errors.New("Protocol Error: " + string(msg))
		}
		result = reply.MakeIntReply(i)
	}
	return result, nil
}

func readBody(msg []byte, state *readState) (err error) {
	line := msg[0 : len(msg)-2]
	if line[0] == '$' {
		state.bulkLine, err = strconv.ParseInt(string(line[1:]), 10, 32)
		if err != nil {
			return errors.New("Protocol Error: " + string(msg))
		}

		if state.bulkLine <= 0 {
			state.args = append(state.args, []byte{})
		}
	} else {
		state.args = append(state.args, line)
	}
	return nil
}
