package tcp

import (
	"context"
	"github.com/SimonMorphy/godis/interface/tcp"
	"github.com/SimonMorphy/godis/lib/logger"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

type Config struct {
	Address string
}

func ListenAndServeWithSignal(
	cfg *Config,
	handler tcp.Handler) error {
	closeChannel := make(chan struct{})
	signalChannel := make(chan os.Signal)
	signal.Notify(signalChannel,
		syscall.SIGQUIT, syscall.SIGHUP,
		syscall.SIGTERM, syscall.SIGINT,
	)

	go func() {
		sig := <-signalChannel
		switch sig {
		case syscall.SIGQUIT, syscall.SIGHUP,
			syscall.SIGTERM, syscall.SIGINT:
			closeChannel <- struct{}{}
		}
	}()

	listen, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return err
	}
	logger.Info("开始监听......")
	ListenAndServe(listen, handler, closeChannel)
	return nil
}

func ListenAndServe(
	listener net.Listener,
	handler tcp.Handler,
	closeChan <-chan struct{}) {

	go func() {
		<-closeChan
		_ = listener.Close()
		_ = handler.Close()
	}()

	defer func() {
		_ = listener.Close()
		_ = handler.Close()
	}()
	ctx := context.Background()
	group := sync.WaitGroup{}
	for {
		conn, err := listener.Accept()
		if err != nil {
			break
		}
		logger.Info("接收到新连接......")
		group.Add(1)
		go func() {
			defer func() {
				group.Done()
			}()
			handler.Handle(ctx, conn)
		}()
	}
	group.Wait()
}
