package binwatch

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"binwatch/api/v1alpha2"
	"binwatch/internal/binwatch/blreaderwork"
	"binwatch/internal/binwatch/blsenderwork"
	"binwatch/internal/binwatch/serverapi"
	"binwatch/internal/cache"
	"binwatch/internal/logger"
	"binwatch/internal/pools"
	"binwatch/internal/utils"

	"gopkg.in/yaml.v3"
)

const (
	componentName = "BinWatch"
)

type BinWatchT struct {
	cfg *v1alpha2.ConfigT

	// pools
	rePool *pools.RowEventPoolT

	// managers
	cach cache.CacheI

	// services
	bwa *serverapi.ServerAPIT
	blr *blreaderwork.BLReaderWorkT
	bls *blsenderwork.BLSenderWorkT
}

func NewBinWatch(configPath string) (bw *BinWatchT, err error) {
	bw = &BinWatchT{
		cfg: &v1alpha2.ConfigT{},
	}

	var fileBytes []byte
	fileBytes, err = os.ReadFile(configPath)
	if err != nil {
		err = fmt.Errorf("error in config file read: %w", err)
		return bw, err
	}
	fileBytes = utils.ExpandEnv(fileBytes)

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
	// Usable ports: 1024 - 49151
	if !utils.IsRegisteredPort(bw.cfg.Server.Port) {
		err = fmt.Errorf("invalid '%d' port number in server", bw.cfg.Server.Port)
		return bw, err
	}

	// Init common values

	bw.rePool = pools.NewRowEventPool(int(bw.cfg.Server.Pool.Size))

	// Init managers

	if bw.cfg.Server.Cache.Enabled {
		bw.cach, err = cache.NewCache(bw.cfg.Server)
		if err != nil {
			err = fmt.Errorf("error in cache manager creation: %w", err)
			return bw, err
		}
	}

	// Init paralel services

	bw.bwa, err = serverapi.NewBinWatchApi(bw.cfg, bw.rePool)
	if err != nil {
		err = fmt.Errorf("error in server API creation: %w", err)
		return bw, err
	}

	bw.blr, err = blreaderwork.NewBinlogReaderWork(bw.cfg, bw.rePool, bw.cach)
	if err != nil {
		err = fmt.Errorf("error in binlog reader worker creation: %w", err)
		return bw, err
	}

	bw.bls, err = blsenderwork.NewBinlogSenderWork(bw.cfg, bw.rePool, bw.cach)
	if err != nil {
		err = fmt.Errorf("error in binlog reader worker creation: %w", err)
		return bw, err
	}

	return bw, err
}

func (bw *BinWatchT) Run() {
	jlog := logger.NewLogger(logger.GetLevel(bw.cfg.Logger.Level))
	extra := utils.GetBasicLogExtraFields(componentName)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(3)
	go bw.bwa.Run(&wg, ctx)
	go bw.blr.Run(&wg, ctx)
	go bw.bls.Run(&wg, ctx)

	sig := <-sigs
	cancel()

	wg.Wait()

	extra.Set("signal", sig.String())
	jlog.Info("close service", extra)
}
