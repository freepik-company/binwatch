package binwatch

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"

	"gopkg.in/yaml.v3"

	"binwatch/api/v1alpha1"
	"binwatch/internal/binwatch/blreaderwork"
	"binwatch/internal/binwatch/blsenderwork"
	"binwatch/internal/binwatch/hashringwork"
	"binwatch/internal/binwatch/serverapi"
	"binwatch/internal/hashring"
	"binwatch/internal/logger"
	"binwatch/internal/managers"
	"binwatch/internal/pools"
)

type BinWatchT struct {
	cfg     *v1alpha1.ConfigSpec
	hr      *hashring.HashRing
	hrReady atomic.Bool
	cqpool  *pools.ConnectorsQueuePoolT

	// managers
	rm *managers.RedisManT

	// services
	bwa *serverapi.ServerAPIT
	hrw *hashringwork.HashRingWorkT
	blr *blreaderwork.BLReaderWorkT
	bls *blsenderwork.BLSenderWorkT
}

func NewBinWatch(configPath string) (bw *BinWatchT, err error) {
	bw = &BinWatchT{
		cfg: &v1alpha1.ConfigSpec{},
	}

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

	// configuration checks

	if bw.cfg.Server.ID == "" {
		err = fmt.Errorf("empty server id")
		return bw, err
	}
	if ip := net.ParseIP(bw.cfg.Server.Host); ip == nil {
		err = fmt.Errorf("malformed server host, invalid ip form")
		return bw, err
	}
	if bw.cfg.Server.Port <= 1023 {
		err = fmt.Errorf("invalid '%d' port number in server", bw.cfg.Server.Port)
		return bw, err
	}

	// Init common values

	bw.cqpool = pools.NewConnectorsQueuePool()
	bw.hrReady.Store(false)
	bw.hr = hashring.NewHashRing(1)

	// Init managers

	if bw.cfg.Redis.Enabled {
		bw.rm, err = managers.NewRedisMan(bw.cfg)
		if err != nil {
			err = fmt.Errorf("error in redis manager creation: %w", err)
			return bw, err
		}
	}

	// Init paralel services

	bw.hrw, err = hashringwork.NewHashRingWork(bw.cfg, bw.hr, &bw.hrReady)
	if err != nil {
		err = fmt.Errorf("error in hashring worker creation: %w", err)
		return bw, err
	}

	bw.bwa, err = serverapi.NewBinWatchApi(bw.cfg, bw.hr, &bw.hrReady)
	if err != nil {
		err = fmt.Errorf("error in server API creation: %w", err)
		return bw, err
	}

	bw.blr, err = blreaderwork.NewBinlogReaderWork(bw.cfg, bw.rm, bw.cqpool)
	if err != nil {
		err = fmt.Errorf("error in binlog reader worker creation: %w", err)
		return bw, err
	}

	bw.bls, err = blsenderwork.NewBinlogSenderWork(bw.cfg, bw.cqpool, bw.hr, &bw.hrReady)
	if err != nil {
		err = fmt.Errorf("error in binlog reader worker creation: %w", err)
		return bw, err
	}

	return bw, err
}

func (bw *BinWatchT) Run() {
	jlog := logger.NewLogger(logger.GetLevel(bw.cfg.Logger.Level))
	extra := logger.ExtraFieldsT{"service": "binwatch"}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(4)
	go bw.hrw.Run(&wg, ctx)
	go bw.bwa.Run(&wg, ctx)
	go bw.blr.Run(&wg, ctx)
	go bw.bls.Run(&wg, ctx)

	sig := <-sigs
	cancel()

	wg.Wait()

	extra.Set("signal", sig.String())
	jlog.Info("close service", extra)
}
