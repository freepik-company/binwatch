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
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"regexp"
	"time"

	//
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	_ "github.com/go-sql-driver/mysql"
	"go.uber.org/zap"

	//
	"binwatch/api/v1alpha1"
	"binwatch/internal/hashring"
)

var (
	tableMetadata   = make(map[string][]string)
	alterTableRegex = regexp.MustCompile(`(?i)ALTER\s+TABLE\s+(\S+)`)
	db              *sql.DB
	err             error
	binLogPos       uint32
	binLogFile      string
)

// Watcher function to watch the MySQL binlog
func Watcher(app *v1alpha1.Application, ring *hashring.HashRing) {

	// configure the MySQL connection
	cfg := replication.BinlogSyncerConfig{
		ServerID:        app.Config.Sources.MySQL.ServerID,
		Flavor:          app.Config.Sources.MySQL.Flavor,
		Host:            app.Config.Sources.MySQL.Host,
		Port:            app.Config.Sources.MySQL.Port,
		User:            app.Config.Sources.MySQL.User,
		Password:        app.Config.Sources.MySQL.Password,
		ReadTimeout:     time.Duration(app.Config.Sources.MySQL.ReadTimeout) * time.Second,
		HeartbeatPeriod: time.Duration(app.Config.Sources.MySQL.HeartbeatPeriod) * time.Second,
	}

	// If hashring is configured, check if there are servers running with the position of the binlog
	if !reflect.ValueOf(app.Config.Hashring).IsZero() {
		// Get minimal binlog position from all servers
		binLogPos, binLogFile, err = getMinimalBinlogPosition(app, ring)
	}

	// If no position is found, let's read binlog from the latest position reading it from database directly
	if binLogPos == 0 && binLogFile == "" {
		binLogPos, binLogFile, err = getMasterStatus(app)
		if err != nil {
			app.Logger.Error("Error getting actual position of binlog", zap.Error(err))
		}
	}

	app.Logger.Info("Starting binlog capture", zap.String("binlog_file", binLogFile),
		zap.Uint32("binlog_pos", binLogPos))

	// Start the binlog syncer from the actual position
	syncer := replication.NewBinlogSyncer(cfg)
	pos := mysql.Position{Name: binLogFile, Pos: binLogPos}
	streamer, err := syncer.StartSync(pos)
	if err != nil {
		app.Logger.Fatal("Error starting sync for binlogs", zap.Error(err))
		return
	}

	// Process the events
	for {

		// Set binlog position if rollback is needed when a node lefts the hashring
		if app.RollBackPosition != 0 && app.RollBackFile != "" {

			app.Logger.Info("Rolling back to last knowledge position and file",
				zap.Uint32("position", app.RollBackPosition), zap.String("file", app.RollBackFile))

			// New position is the rollback position
			pos.Pos = app.RollBackPosition
			pos.Name = app.RollBackFile

			// Close the current syncer and start a new one
			syncer.Close()
			syncer = replication.NewBinlogSyncer(cfg)
			streamer, err = syncer.StartSync(pos)
			if err != nil {
				app.Logger.Fatal("Error starting sync for binlogs again, bye.", zap.Error(err))
				return
			}

			// Clean up the rollback position after using it
			app.RollBackPosition = 0
			app.RollBackFile = ""
		}

		// Update the binlog current position in the application
		app.BinLogPosition = syncer.GetNextPosition().Pos
		app.BinLogFile = syncer.GetNextPosition().Name

		// Set timeout for processing events
		sqlapp, cancel := context.WithTimeout(context.Background(),
			time.Duration(app.Config.Sources.MySQL.SyncTimeoutMs)*time.Millisecond)

		// Get the next event
		ev, err := streamer.GetEvent(sqlapp)
		cancel()

		// Handle errors
		if err != nil {

			// Handle context timeout
			if err == context.DeadlineExceeded {
				continue
			}

			// Handle other errors
			app.Logger.Info("Error getting the event", zap.Error(err))
			continue
		}

		// Filter database and table included in the configuration
		// By default watch all tables
		if len(app.Config.Sources.MySQL.FilterTables) > 0 {
			ok, err := filterEvent(app, ev)
			if err != nil {
				app.Logger.Info("Error filtering event", zap.Error(err))
			}
			if !ok {
				continue
			}
		}

		// Check if the server assigned to the event is the current server
		if !reflect.ValueOf(app.Config.Hashring).IsZero() {
			t := time.Now()
			severAssigned := ring.GetServer(t.Format("20060102150405"))
			app.Logger.Debug(fmt.Sprintf("Server assigned: %s", severAssigned))
			if app.Config.ServerId != severAssigned {
				continue
			}
		}

		err = processEventBinLog(app, ev)
		if err != nil {
			app.Logger.Fatal("Error processing event", zap.Error(err))
		}
	}
}
