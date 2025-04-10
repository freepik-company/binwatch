/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package mysql

import (
	//
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"time"

	//
	"cloud.google.com/go/pubsub"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"go.uber.org/zap"

	//
	"binwatch/api/v1alpha1"
	"binwatch/internal/hashring"
)

const (
	DumpStep             = "mysqldump"
	DefaultMysqlDumpPath = "/usr/bin/mysqldump"
)

var (
	DumpFinished    = errors.New("dump finished")
	binlogPosition  = mysql.Position{}
	dumpRollbackPos = uint32(0)
	err             error
	binLogPos       uint32
	binLogFile      string
	jsonData        []byte
	// Initialize connectorsQueue and run workers for execute Connectors
	connectorsQueue = &ConnectorsQueue{
		queue: []QueueItems{},
	}
)

type ConnectorsQueue struct {
	mutex sync.RWMutex
	queue []QueueItems
}

type QueueItems struct {
	eventType     string
	eventTable    string
	eventDatabase string
	data          []byte
}

// CanalEventHandler is a custom event handler for canal
type CanalEventHandler struct {
	canal.DummyEventHandler
	app             *v1alpha1.Application
	ring            *hashring.HashRing
	canal           *canal.Canal
	connectorsQueue *ConnectorsQueue
}

// Sync function to sync MySQL with the hashring support
func Sync(app *v1alpha1.Application, ring *hashring.HashRing) {

	// Parse durations
	readTimeout, err := time.ParseDuration(app.Config.Sources.MySQL.ReadTimeout)
	if err != nil {
		app.Logger.Fatal("Error parsing read timeout", zap.Error(err))
	}
	heathbeatPeriod, err := time.ParseDuration(app.Config.Sources.MySQL.HeartbeatPeriod)
	if err != nil {
		app.Logger.Fatal("Error parsing heartbeat period", zap.Error(err))
	}
	serverId, err := strconv.ParseUint(app.Config.Sources.MySQL.ServerID, 10, 32)
	if err != nil {
		app.Logger.Fatal("Error parsing server ID", zap.Error(err))
	}

	cfg := &canal.Config{
		ServerID:        uint32(serverId),
		Flavor:          app.Config.Sources.MySQL.Flavor,
		Addr:            fmt.Sprintf("%s:%s", app.Config.Sources.MySQL.Host, app.Config.Sources.MySQL.Port),
		User:            app.Config.Sources.MySQL.User,
		Password:        app.Config.Sources.MySQL.Password,
		ReadTimeout:     readTimeout,
		HeartbeatPeriod: heathbeatPeriod,
	}

	// If hashring is configured, get the current position from the memory cache
	if !reflect.ValueOf(app.Config.Hashring).IsZero() {
		// binlogPosition.Pos, binlogPosition.Name, err = hashring.GetRedisLogPos(app)
		// if err != nil {
		// 	app.Logger.Fatal("Error getting minimal binlog position", zap.Error(err))
		// }

		app.Logger.Info(fmt.Sprintf("Binlog position detected from memory store %s/%d", binlogPosition.Name, binlogPosition.Pos))
		if binlogPosition.Name == DumpStep {
			app.Logger.Info(fmt.Sprintf("Dump step detected that it was in progress, let's start from the last position knowledge %d", binlogPosition.Pos))
			dumpRollbackPos = binlogPosition.Pos
			binlogPosition.Pos = 0
		}
	}

	// If set, get start_position from the config file. Use it to start canal
	if !reflect.ValueOf(app.Config.Sources.MySQL.StartPosition).IsZero() {
		binlogPosition.Pos = app.Config.Sources.MySQL.StartPosition.Position
		binlogPosition.Name = app.Config.Sources.MySQL.StartPosition.File
		app.Logger.Info(fmt.Sprintf("Start position configured in %s/%v", binLogFile, binLogPos))
	}

	// First get the dump configuration. If it is present, use it to dump the database and tables defined.
	// If user defines just one database, it will check for tables to dump. If user defines more than one database,
	//it will dump all tables from the databases defined.
	if !reflect.ValueOf(app.Config.Sources.MySQL.DumpConfig).IsZero() && (binlogPosition.Name == "" || binlogPosition.Name == DumpStep) {
		if len(app.Config.Sources.MySQL.DumpConfig.Databases) > 1 && len(app.Config.Sources.MySQL.DumpConfig.Tables) > 0 {
			cfg.Dump.Databases = app.Config.Sources.MySQL.DumpConfig.Databases
			app.Logger.Info(fmt.Sprintf("database to dump: %s  with all tables", cfg.Dump.TableDB))
		} else if len(app.Config.Sources.MySQL.DumpConfig.Databases) == 1 {
			cfg.Dump.TableDB = app.Config.Sources.MySQL.DumpConfig.Databases[0]
			if len(app.Config.Sources.MySQL.DumpConfig.Tables) > 0 {
				cfg.Dump.Tables = app.Config.Sources.MySQL.DumpConfig.Tables
			}
			app.Logger.Info(fmt.Sprintf("database to dump: %s and tables: %v", cfg.Dump.TableDB, cfg.Dump.Tables))
		} else {
			app.Logger.Fatal("no database configured")
		}

		// Define mysqldump path. If it is running in Docker container, the default path is /bin/mysqldump
		cfg.Dump.ExecutionPath = app.Config.Sources.MySQL.DumpConfig.MySQLDumpBinPath
		if app.Config.Sources.MySQL.DumpConfig.MySQLDumpBinPath == "" {
			cfg.Dump.ExecutionPath = DefaultMysqlDumpPath
		}

		// Set BinLogFile with the dump step.
		app.BinLogFile = DumpStep
		binlogPosition.Name = DumpStep
		cfg.Dump.SkipMasterData = true
		if len(app.Config.Sources.MySQL.DumpConfig.MySQLDumpExtraOptions) > 0 {
			app.Logger.Debug(fmt.Sprintf("Extra options for mysqldump: %v", app.Config.Sources.MySQL.DumpConfig.MySQLDumpExtraOptions))
			cfg.Dump.ExtraOptions = app.Config.Sources.MySQL.DumpConfig.MySQLDumpExtraOptions
		}
	}

	// Create pubsub clients if the configuration is present, to avoid creating a client for each event
	if !reflect.ValueOf(app.Config.Connectors.PubSub).IsZero() {
		app.PubsubClient = make(map[string]*pubsub.Client)
		for _, pubsubConfig := range app.Config.Connectors.PubSub {
			app.PubsubClient[pubsubConfig.Name], err = pubsub.NewClient(app.Context, pubsubConfig.ProjectID)
			if err != nil {
				app.Logger.Fatal(fmt.Sprintf("Error creating pubsub client for %s project ID.", pubsubConfig.ProjectID), zap.Error(err))
			}
		}
	}

	// Start connector workers to run in background listening to the queue
	maxWorkers := 1
	if app.Config.MaxWorkers != "" {
		maxWorkers, err = strconv.Atoi(app.Config.MaxWorkers)
		if err != nil {
			app.Logger.Fatal("Error parsing max workers", zap.Error(err))
		}
	}
	for i := 0; i < maxWorkers; i++ {
		app.Logger.Debug(fmt.Sprintf("Starting worker with id: %d", i))
		go executeConnectors(app, connectorsQueue)
	}

	// goroutine for calculate sleep time between events
	go calculateSleepTime(app, connectorsQueue)

	// Create a new canal instance
	c, err := canal.NewCanal(cfg)
	if err != nil {
		app.Logger.Fatal("Error creating canal", zap.Error(err))
	}

	// Register a handler to handle canal events
	c.SetEventHandler(&CanalEventHandler{
		app:             app,
		ring:            ring,
		canal:           c,
		connectorsQueue: connectorsQueue,
	})

	// Start canal loop to sync MySQL with rollback support
	for {

		if binlogPosition.Name == DumpStep {
			app.Logger.Info("Starting mysqldump process")
			err = c.Run()
		} else {
			if binlogPosition.Pos == 0 {
				binlogPosition, errM := c.GetMasterPos()
				if errM != nil || binlogPosition == (mysql.Position{}) {
					app.Logger.Fatal("Error getting master position", zap.Error(err))
				}
			}
			app.BinLogPosition = binlogPosition.Pos
			app.BinLogFile = binlogPosition.Name
			err = c.RunFrom(binlogPosition)
		}

		if err != nil {
			app.Logger.Fatal("Error running canal", zap.Error(err))
			return
		}

		break
	}

}

// OnRow is called when a row event is received, for Dump and Normal process.
func (h *CanalEventHandler) OnRow(e *canal.RowsEvent) error {

	// If we are recovering the dump process
	if dumpRollbackPos != 0 {
		if h.app.BinLogPosition < dumpRollbackPos {
			h.app.BinLogPosition++
			h.app.Logger.Debug(fmt.Sprintf("Binlog position for Dump process already synced, skipping: %d - %d", h.app.BinLogPosition, dumpRollbackPos))
			return nil
		}
	}

	// If the event is a dump event, increment the position by one
	if h.app.BinLogFile == DumpStep {
		h.app.BinLogPosition++
	} else {
		// For normal process, update the position and file from the event header
		h.app.BinLogPosition = e.Header.LogPos
		syncedPos := h.canal.SyncedPosition()
		h.app.BinLogFile = syncedPos.Name

	}

	h.app.Logger.Debug(fmt.Sprintf("Syncing position %d in file: %s", h.app.BinLogPosition, h.app.BinLogFile))

	// Filter database and table included in the configuration
	if !watchEvent(h.app, e) {
		return nil
	}

	// Check if the server assigned to the event is the current server in the hashring
	if !reflect.ValueOf(h.app.Config.Hashring).IsZero() {
		severAssigned := h.ring.GetServer(fmt.Sprintf("%d", h.app.BinLogPosition))
		h.app.Logger.Debug(fmt.Sprintf("Server assigned: %s", severAssigned))
		if h.app.Config.Server.ID != severAssigned {
			return nil
		}
	}

	// Process the row event
	row := e.Rows[0]
	if e.Action == canal.UpdateAction {
		row = e.Rows[1]
	}
	jsonData, err = processRow(h.app, row, e.Table.Columns)
	if err != nil {
		return fmt.Errorf("error processing row: %v", err)
	}

	// Add data to the queue
	h.connectorsQueue.mutex.Lock()
	defer h.connectorsQueue.mutex.Unlock()
	h.connectorsQueue.queue = append(h.connectorsQueue.queue, QueueItems{
		eventType:     e.Action,
		eventTable:    e.Table.Name,
		eventDatabase: e.Table.Schema,
		data:          jsonData,
	})

	// Calculate conenctorsQueue size and sleep time if needed
	queueSize := len(h.connectorsQueue.queue)
	h.app.Logger.Debug(fmt.Sprintf("Queue size: %d", queueSize))
	time.Sleep(h.app.SleepTime)

	return nil

}
