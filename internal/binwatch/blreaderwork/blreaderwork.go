package blreaderwork

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"binwatch/api/v1alpha2"
	"binwatch/internal/logger"
	"binwatch/internal/managers"
	"binwatch/internal/pools"
	"binwatch/internal/utils"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

const (
	componentName = "BinlogReaderWorker"
)

type BLReaderWorkT struct {
	cfg *v1alpha2.ConfigT
	log logger.LoggerT

	rm     *managers.RedisManT
	rePool *pools.RowEventPoolT

	mysql mysqlT
}

type mysqlT struct {
	blSyncer *replication.BinlogSyncer
	blStream *replication.BinlogStreamer
	blPos    mysql.Position
	database string
	tables   []string
}

func NewBinlogReaderWork(cfg *v1alpha2.ConfigT, rm *managers.RedisManT, rePool *pools.RowEventPoolT) (w *BLReaderWorkT, err error) {
	w = &BLReaderWorkT{
		log: logger.NewLogger(logger.GetLevel(cfg.Logger.Level)),
		cfg: cfg,

		rm:     rm,
		rePool: rePool,
	}
	w.mysql.database = cfg.Source.Database
	w.mysql.tables = append(w.mysql.tables, cfg.Source.Tables...)

	w.mysql.blSyncer = replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
		Flavor:          w.cfg.Source.Flavor,
		ServerID:        w.cfg.Source.ServerID,
		Host:            w.cfg.Source.Host,
		Port:            uint16(w.cfg.Source.Port),
		User:            w.cfg.Source.User,
		Password:        w.cfg.Source.Password,
		ReadTimeout:     w.cfg.Source.ReadTimeout,
		HeartbeatPeriod: w.cfg.Source.HeartbeatPeriod,
		// Logger:          logger.DummyLogger{},
	})

	// Get BINLOG position

	if !reflect.ValueOf(w.cfg.Source.StartPosition).IsZero() {
		w.mysql.blPos = mysql.Position{
			Name: w.cfg.Source.StartPosition.File,
			Pos:  w.cfg.Source.StartPosition.Position,
		}
	} else if w.cfg.Server.Cache.Enabled {
		w.mysql.blPos.Name, w.mysql.blPos.Pos, err = w.rm.GetBinlogFilePos()
		if err != nil {
			return w, err
		}
	} else {
		var ctmp *canal.Canal
		ctmp, err = canal.NewCanal(&canal.Config{
			ServerID: w.cfg.Source.ServerID,
			Flavor:   w.cfg.Source.Flavor,
			Addr:     fmt.Sprintf("%s:%d", w.cfg.Source.Host, w.cfg.Source.Port),
			User:     w.cfg.Source.User,
			Password: w.cfg.Source.Password,
		})
		if err != nil {
			return w, err
		}
		defer ctmp.Close()

		w.mysql.blPos, err = ctmp.GetMasterPos()
		if err != nil {
			return w, err
		}
	}

	return w, err
}

func (w *BLReaderWorkT) Run(wg *sync.WaitGroup, ctx context.Context) {
	defer wg.Done()
	var extra = utils.GetBasicLogExtraFields(componentName)
	var err error

	w.mysql.blStream, err = w.mysql.blSyncer.StartSync(w.mysql.blPos)
	if err != nil {
		w.log.Fatal("unable to start syncer", extra, err)
	}

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
				var event *replication.BinlogEvent
				event, err = w.mysql.blStream.GetEvent(ctx)
				if err != nil {
					if err != context.Canceled {
						w.log.Error("error in get binlog event", extra, err)
					}
					continue
				}

				err = w.processEvent(ctx, event)
				if err != nil {
					w.log.Error("error in process binlog event", extra, err)
					continue
				}
			}
		}
	}
}
