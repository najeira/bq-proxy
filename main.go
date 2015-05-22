package main

import (
	"flag"
	"fmt"
	"github.com/najeira/goutils/nlog"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"
)

var logger nlog.Logger = nil

var Options struct {
	FD      uint
	Port    int
	Email   string
	Pem     []byte
	Logging string
}

func initOptions() {
	var pemFile string

	flag.UintVar(&Options.FD, "fd", 0, "file descriptor")
	flag.IntVar(&Options.Port, "port", 0, "port")
	flag.StringVar(&Options.Email, "email", "", "bigquery account email")
	flag.StringVar(&pemFile, "pem", "", "bigquery PEM file")
	flag.StringVar(&Options.Logging, "logging", "warn", "log level")
	flag.Parse()

	if err := checkOptions(pemFile); err != nil {
		flag.Usage()
		fatal(err)
	}
}

func checkOptions(pemFile string) error {
	if Options.Email == "" {
		return fmt.Errorf("account required.")
	} else if pemFile == "" {
		return fmt.Errorf("pem required.")
	} else if Options.FD == 0 && Options.Port == 0 {
		return fmt.Errorf("fd or port required.")
	}

	f, err := os.Open(pemFile)
	if err != nil {
		return err
	}

	defer f.Close()

	pem, err := ioutil.ReadAll(f)
	if err != nil {
		return err
	}

	Options.Pem = pem
	return nil
}

func fatal(err error) {
	logger.Errorf("%v", err)
	os.Exit(1)
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	// logger
	logger = nlog.NewLogger(nil)
	logger.SetLevel(nlog.Error)

	// parse flags
	initOptions()

	// update logging level
	logger.SetLevelName(Options.Logging)

	// listen
	ln, err := listen()
	if err != nil {
		fatal(err)
		return
	}

	// handler
	handler := newHttpHandler()

	// signal handler
	done := runSignalHandler(ln, handler)

	// start server
	timeoutHandler := http.TimeoutHandler(handler, time.Second*60, "")
	if err := http.Serve(ln, timeoutHandler); err != nil {
		// signalなどで閉じられるとerrが返ってくる
		logger.Noticef("%v", err)
	} else {
		logger.Noticef("server closed")
	}

	// サーバとワーカの終了まで待つ
	<-done
}

func runSignalHandler(ln net.Listener, handler *httpHandler) chan struct{} {
	done := make(chan struct{}, 1)
	sigCh := make(chan os.Signal, 10)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		signal.Stop(sigCh)
		close(sigCh)

		logger.Noticef("signal %v", sig)

		// 先にサーバ側を終了し、新規のリクエストを止める
		ln.Close()

		// ワーカーを停止する
		handler.Close()

		// 完了
		close(done)
	}()

	logger.Infof("runSignalHandler")
	return done
}

func listen() (net.Listener, error) {
	if Options.FD != 0 {
		return listenFileDescriptor(Options.FD)
	} else if Options.Port != 0 {
		return listenTCP(Options.Port)
	}
	return nil, fmt.Errorf("no listener")
}

func listenTCP(port int) (net.Listener, error) {
	addr := fmt.Sprintf(":%d", port)
	logger.Infof("listenTCP %d", port)
	return net.Listen("tcp", addr)
}

func listenFileDescriptor(fd uint) (net.Listener, error) {
	file := os.NewFile(uintptr(fd), "listen socket")
	defer file.Close()
	logger.Infof("listenFileDescriptor %v", file)
	return net.FileListener(file)
}
