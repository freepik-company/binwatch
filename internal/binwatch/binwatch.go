package binwatch

import (
	"binwatch/api/v1alpha1"
	"binwatch/internal/binwatch/binlogwork"
	"binwatch/internal/binwatch/hashringwork"
	"binwatch/internal/binwatch/serverapi"
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"

	"gopkg.in/yaml.v3"
)

type BinWatchT struct {
	cfg   *v1alpha1.ConfigSpec
	ready atomic.Bool

	bwa *serverapi.ServerAPIT
	hrw *hashringwork.HashRingWorkT
	blw *binlogwork.BinlogWorkT
}

func NewBinWatch(configPath string) (bw *BinWatchT, err error) {
	bw = &BinWatchT{
		cfg: &v1alpha1.ConfigSpec{},
	}
	bw.ready.Store(false)

	var fileBytes []byte
	fileBytes, err = os.ReadFile(configPath)
	if err != nil {
		err = fmt.Errorf("error in config file read: %w", err)
		return bw, err
	}
	fileBytes = []byte(os.ExpandEnv(string(fileBytes)))

	err = yaml.Unmarshal(fileBytes, bw.cfg)
	if err != nil {
		err = fmt.Errorf("error in config file parsing: %w", err)
		return bw, err
	}

	bw.hrw, err = hashringwork.NewHashRing(bw.cfg, &bw.ready)
	if err != nil {
		err = fmt.Errorf("error in hashring worker creation: %w", err)
		return bw, err
	}

	bw.bwa, err = serverapi.NewBinWatchApi(bw.cfg, &bw.ready)
	if err != nil {
		err = fmt.Errorf("error in server API creation: %w", err)
		return bw, err
	}

	bw.blw, err = binlogwork.NewBinLogWork(bw.cfg, &bw.ready)
	if err != nil {
		err = fmt.Errorf("error in binlog worker creation: %w", err)
		return bw, err
	}

	return bw, err
}

func (bw *BinWatchT) Run() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(3)
	go bw.hrw.Run(&wg, ctx)
	go bw.bwa.Run(&wg, ctx)
	go bw.blw.Run(&wg, ctx)

	sig := <-sigs
	cancel()

	wg.Wait()

	log.Printf("INFO close service with '%s' signal", sig.String())
}
