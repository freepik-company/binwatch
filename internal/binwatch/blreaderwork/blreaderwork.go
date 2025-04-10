package blreaderwork

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"binwatch/api/v1alpha1"
	"binwatch/internal/logger"
	"binwatch/internal/managers"
	"binwatch/internal/pools"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
)

type BLReaderWorkT struct {
	log    logger.LoggerT
	cfg    *v1alpha1.ConfigSpec
	rm     *managers.RedisManT
	cqpool *pools.ConnectorsQueuePoolT

	rv runValuesT
}

type runValuesT struct {
	blPos mysql.Position

	readTimeout     time.Duration
	heathbeatPeriod time.Duration
	mysqlConfig     *canal.Config
}

func NewBinlogReaderWork(cfg *v1alpha1.ConfigSpec, rm *managers.RedisManT, cqpool *pools.ConnectorsQueuePoolT) (w *BLReaderWorkT, err error) {
	w = &BLReaderWorkT{
		log:    logger.NewLogger(logger.GetLevel(cfg.Logger.Level)),
		cfg:    cfg,
		rm:     rm,
		cqpool: cqpool,
	}

	if w.cfg.Redis.Enabled {
		w.rv.blPos.Name, w.rv.blPos.Pos, err = w.rm.GetBinlogFilePos()
		if err != nil {
			return w, err
		}
	}

	w.rv.readTimeout, err = time.ParseDuration(w.cfg.Sources.MySQL.ReadTimeout)
	if err != nil {
		return w, err
	}

	w.rv.heathbeatPeriod, err = time.ParseDuration(w.cfg.Sources.MySQL.HeartbeatPeriod)
	if err != nil {
		return w, err
	}

	serverId, err := strconv.ParseUint(w.cfg.Sources.MySQL.ServerID, 10, 32)
	if err != nil {
		return w, err
	}

	w.rv.mysqlConfig = &canal.Config{
		ServerID:        uint32(serverId),
		Flavor:          w.cfg.Sources.MySQL.Flavor,
		Addr:            fmt.Sprintf("%s:%s", w.cfg.Sources.MySQL.Host, w.cfg.Sources.MySQL.Port),
		User:            w.cfg.Sources.MySQL.User,
		Password:        w.cfg.Sources.MySQL.Password,
		ReadTimeout:     w.rv.readTimeout,
		HeartbeatPeriod: w.rv.heathbeatPeriod,
	}

	return w, err
}

func (w *BLReaderWorkT) Run(wg *sync.WaitGroup, ctx context.Context) {
	defer wg.Done()
	var extra = logger.ExtraFieldsT{"component": "BinlogReaderWorker"}
	var err error

	runWorker := true
	for runWorker {
		select {
		case <-ctx.Done():
			{
				runWorker = false
				w.log.Info("execution cancelled", extra)
			}
		default:
			{
				w.log.Debug("waiting to next sync", extra)
				time.Sleep(2 * time.Second)
				_ = err

				// mysql.Sync(&app, hr)
			}
		}
	}
}
