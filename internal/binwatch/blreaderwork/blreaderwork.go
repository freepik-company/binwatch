package blreaderwork

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"binwatch/api/v1alpha2"
	"binwatch/internal/cache"
	"binwatch/internal/logger"
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

	rePool *pools.RowEventPoolT
	cach   cache.CacheI

	mysql mysqlT
}

type mysqlT struct {
	blSyncer *replication.BinlogSyncer
	blStream *replication.BinlogStreamer
	blLoc    mysql.Position
	colNames map[string][]string
}

func NewBinlogReaderWork(cfg *v1alpha2.ConfigT, rePool *pools.RowEventPoolT, cach cache.CacheI) (w *BLReaderWorkT, err error) {
	w = &BLReaderWorkT{
		log: logger.NewLogger(logger.GetLevel(cfg.Logger.Level)),
		cfg: cfg,

		rePool: rePool,
		cach:   cach,
	}

	w.mysql.blSyncer = replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
		Flavor:          w.cfg.Source.Flavor,
		ServerID:        w.cfg.Source.ServerID,
		Host:            w.cfg.Source.Host,
		Port:            uint16(w.cfg.Source.Port),
		User:            w.cfg.Source.User,
		Password:        w.cfg.Source.Password,
		ReadTimeout:     w.cfg.Source.ReadTimeout,
		HeartbeatPeriod: w.cfg.Source.HeartbeatPeriod,
		Logger:          logger.DummyLogger{},
	})

	// Get Columns

	w.mysql.colNames, err = utils.GetTableColumns(utils.DBOptions{
		Flavor: w.cfg.Source.Flavor,
		User:   w.cfg.Source.User,
		Pass:   w.cfg.Source.Password,
		Host:   w.cfg.Source.Host,
		Port:   w.cfg.Source.Port,
	}, w.cfg.Source.DBTables)
	if err != nil {
		return w, err
	}

	// Get BINLOG position

	if w.cfg.Server.Cache.Enabled {
		var blLoc cache.BinlogLocation
		blLoc, err = w.cach.Load()
		if err != nil {
			return w, err
		}
		w.mysql.blLoc = mysql.Position{
			Name: blLoc.File,
			Pos:  blLoc.Position,
		}
	}

	if !reflect.ValueOf(w.cfg.Source.StartLocation).IsZero() {
		w.mysql.blLoc = mysql.Position{
			Name: w.cfg.Source.StartLocation.File,
			Pos:  w.cfg.Source.StartLocation.Position,
		}
	}

	if w.mysql.blLoc.Name == "" {
		w.mysql.blLoc, err = utils.GetCurrentBinlogLocation(&canal.Config{
			ServerID: w.cfg.Source.ServerID,
			Flavor:   w.cfg.Source.Flavor,
			Addr:     fmt.Sprintf("%s:%d", w.cfg.Source.Host, w.cfg.Source.Port),
			User:     w.cfg.Source.User,
			Password: w.cfg.Source.Password,
			Logger:   logger.DummyLogger{},
		})
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

	w.mysql.blStream, err = w.mysql.blSyncer.StartSync(w.mysql.blLoc)
	if err != nil {
		w.log.Error("unable to start syncer", extra, err, true)
	}

	extra.Set("location", w.mysql.blLoc)
	w.log.Info("start sync process", extra)
	extra.Del("location")
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
				var e *replication.BinlogEvent
				extra.Del("event")

				e, err = w.mysql.blStream.GetEvent(ctx)
				if err != nil {
					if err != context.Canceled {
						w.log.Error("error in get binlog event", extra, err, w.cfg.Server.StopInError)

						w.log.Info("restarting syncer", extra)
						w.mysql.blSyncer.Close()
						time.Sleep(5 * time.Second)

						if w.cfg.Server.Cache.Enabled {
							var blLoc cache.BinlogLocation
							blLoc, err = w.cach.Load()
							if err != nil {
								w.log.Error("error in get cache binlog location", extra, err, true)
							}
							w.mysql.blLoc = mysql.Position{
								Name: blLoc.File,
								Pos:  blLoc.Position,
							}
						}

						w.mysql.blSyncer = replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
							Flavor:          w.cfg.Source.Flavor,
							ServerID:        w.cfg.Source.ServerID,
							Host:            w.cfg.Source.Host,
							Port:            uint16(w.cfg.Source.Port),
							User:            w.cfg.Source.User,
							Password:        w.cfg.Source.Password,
							ReadTimeout:     w.cfg.Source.ReadTimeout,
							HeartbeatPeriod: w.cfg.Source.HeartbeatPeriod,
							Logger:          logger.DummyLogger{},
						})

						w.mysql.blStream, err = w.mysql.blSyncer.StartSync(w.mysql.blLoc)
						if err != nil {
							w.log.Error("unable to restart syncer", extra, err, true)
						}
					}
					continue
				}

				switch e.Event.(type) {
				case *replication.RotateEvent:
					{
						re := e.Event.(*replication.RotateEvent)
						w.mysql.blLoc.Name = string(re.NextLogName)
						w.mysql.blLoc.Pos = uint32(re.Position)

						item := pools.RowEventItemT{
							ItemID: 0,
							Log: pools.RowEventItemLogT{
								EventType:      e.Header.EventType.String(),
								BinlogFile:     string(re.NextLogName),
								BinlogPosition: uint64(re.Position),
							},
						}

						if w.cfg.Server.Cache.Enabled {
							err = w.cach.Store(cache.BinlogLocation{
								File:     item.Log.BinlogFile,
								Position: uint32(item.Log.BinlogPosition),
							})
							if err != nil {
								w.log.Error("error rotating binlog file location", extra, err, w.cfg.Server.StopInError)
								continue
							}
						}

						extra.Set("event", item)
						w.log.Debug("rotate binlog file in location", extra)
					}
				case *replication.RowsEvent:
					{
						re := e.Event.(*replication.RowsEvent)

						// Set items basics

						item := &pools.RowEventItemT{
							Log: pools.RowEventItemLogT{
								EventType:      e.Header.EventType.String(),
								BinlogFile:     w.mysql.blLoc.Name,
								BinlogPosition: uint64(e.Header.LogPos),
							},
							Data: pools.RowEventItemDataT{
								Database:  string(re.Table.Schema),
								Table:     string(re.Table.Table),
								Operation: utils.GetDMLOperationFromRowsEventType(e.Header.EventType),
							},
						}

						// Get item rows

						colNamesKey := strings.Join([]string{item.Data.Database, item.Data.Table}, ".")
						if _, ok := w.mysql.colNames[colNamesKey]; !ok {
							continue
						}

						rowi := 0
						itemRows := []map[string]any{}
						for ri := range re.Rows {
							if len(w.mysql.colNames[colNamesKey]) != len(re.Rows[ri]) {
								err = fmt.Errorf("the table %s has %d columns but binlog have %d", colNamesKey, len(w.mysql.colNames[colNamesKey]), len(re.Rows[ri]))
								w.log.Error("number of columns mismatch in row", extra, err, w.cfg.Server.StopInError)
								break
							}

							if item.Data.Operation == utils.DMLOperationUpdate && ri%2 == 0 {
								continue
							}
							itemRows = append(itemRows, map[string]any{})

							for ci, cv := range w.mysql.colNames[colNamesKey] {
								itemRows[rowi][cv] = re.Rows[ri][ci]
							}
							rowi++
						}
						if err != nil {
							continue
						}

						if w.cfg.Server.Pool.ItemByRow {
							for _, rowv := range itemRows {
								partialItem := &pools.RowEventItemT{
									Log:  item.Log,
									Data: item.Data,
								}
								partialItem.Data.Rows = []map[string]any{rowv}

								w.rePool.Prepare(partialItem)
								err = w.rePool.Add(ctx, partialItem)
								if err != nil {
									w.log.Error("error adding partial item in pool", extra, err, w.cfg.Server.StopInError)
									break
								}

								extra.Set("event", partialItem)
								w.log.Info("success adding event in pool", extra)
								extra.Del("event")
							}
							continue
						}

						item.Data.Rows = itemRows
						w.rePool.Prepare(item)
						err = w.rePool.Add(ctx, item)
						if err != nil {
							w.log.Error("error adding item in pool", extra, err, w.cfg.Server.StopInError)
							continue
						}

						extra.Set("event", item)
						w.log.Info("success adding event in pool", extra)
					}
				default:
					{
						continue
					}
				}
			}
		}
	}
}
