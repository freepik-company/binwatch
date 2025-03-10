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
	"binwatch/api/v1alpha1"
	"binwatch/internal/hashring"
	"errors"
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"go.uber.org/zap"
	"reflect"
	"time"
)

const (
	DumpStep             = "mysqldump"
	DefaultMysqlDumpPath = "/usr/bin/mysqldump"
)

var (
	ErrRollback = errors.New("rollback requested")
	rollbackPos = mysql.Position{}
	err         error
	binLogPos   uint32
	binLogFile  string
	jsonData    []byte
)

// CanalEventHandler is a custom event handler for canal
type CanalEventHandler struct {
	canal.DummyEventHandler
	app   *v1alpha1.Application
	ring  *hashring.HashRing
	canal *canal.Canal
}

func Sync(app *v1alpha1.Application, ring *hashring.HashRing) {
	cfg := &canal.Config{
		ServerID:        app.Config.Sources.MySQL.ServerID,
		Flavor:          app.Config.Sources.MySQL.Flavor,
		Addr:            fmt.Sprintf("%s:%d", app.Config.Sources.MySQL.Host, app.Config.Sources.MySQL.Port),
		User:            app.Config.Sources.MySQL.User,
		Password:        app.Config.Sources.MySQL.Password,
		ReadTimeout:     time.Duration(app.Config.Sources.MySQL.ReadTimeout) * time.Second,
		HeartbeatPeriod: time.Duration(app.Config.Sources.MySQL.HeartbeatPeriod) * time.Second,
	}

	// If hashring is configured, check if there are servers running with the position of the binlog
	if !reflect.ValueOf(app.Config.Hashring).IsZero() {
		// Get minimal binlog position from all servers
		binLogPos, binLogFile, err = getMinimalBinlogPosition(app, ring)

		// If sync process is still during the DumpStep, use RollbackFile and RollbackPosition variables
		if binLogFile == DumpStep {
			app.Logger.Info(fmt.Sprintf("Sync already being restored from dump position %s/%v", binLogFile, binLogPos))
			app.RollBackFile = binLogFile
			app.RollBackPosition = binLogPos
		} else if binLogPos != 0 && binLogFile != "" {
			// If sync process is not during the DumpStep, use the rollbackPos variable to start canal in this position
			app.Logger.Info(fmt.Sprintf("Syncing from minimal position knowledge %s/%v", binLogFile, binLogPos))
			rollbackPos.Pos = binLogPos
			rollbackPos.Name = binLogFile
		}
	}

	// First get the dump configuration. If it is present, use it to dump the database and tables defined.
	// If user defines just one database, it will check for tables to dump. If user defines more than one database,
	//it will dump all tables from the databases defined.
	if !reflect.ValueOf(app.Config.Sources.MySQL.DumpConfig).IsZero() {
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
		cfg.Dump.SkipMasterData = true
		if len(app.Config.Sources.MySQL.DumpConfig.MySQLDumpExtraOptions) > 0 {
			cfg.Dump.ExtraOptions = app.Config.Sources.MySQL.DumpConfig.MySQLDumpExtraOptions
		}
	}

	// Create a new canal instance
	c, err := canal.NewCanal(cfg)
	if err != nil {
		app.Logger.Fatal("Error creating canal", zap.Error(err))
	}

	// Register a handler to handle canal events
	c.SetEventHandler(&CanalEventHandler{
		app:   app,
		ring:  ring,
		canal: c,
	})

	// Start canal loop to sync MySQL with rollback support
	for {

		// If rollback mysql position is defined, then start canal From this position
		// In other cases or during the DumpStep process, start canal normally. We will
		// check if rollback is needed during the canal loop.
		if rollbackPos != (mysql.Position{}) {
			err = c.RunFrom(rollbackPos)
		} else {
			err = c.Run()
		}

		// Listen for specific signal to rollback
		if errors.Is(err, ErrRollback) {

			// Log rollback reached
			app.Logger.Info("A rollback is requested. Stopping and recreating canal from last knowledge position",
				zap.Uint32("position", app.RollBackPosition), zap.String("file", app.RollBackFile))

			// Close existing canal
			c.Close()

			// Recreate new canal with the same configuration
			c, err = canal.NewCanal(cfg)
			if err != nil {
				app.Logger.Fatal("Error recreating canal", zap.Error(err))
				return
			}

			// Set rollback needed to false
			app.RollbackNeeded = false

			// Register a handler to handle canal events to the new canal
			c.SetEventHandler(&CanalEventHandler{
				app:   app,
				ring:  ring,
				canal: c,
			})

			// If rollback is requested during the DumpStep process, set the rollback position and file
			// to the begining of the dump process. We will check in OnRow event for events already processed.
			if app.RollBackFile == DumpStep {
				app.BinLogPosition = 0
				app.BinLogFile = DumpStep
			} else {
				// If rollback is requested during the normal process, set the rollback position and file
				// to the last knowledge position and clean up the rollback variables.
				rollbackPos.Pos = app.RollBackPosition
				rollbackPos.Name = app.RollBackFile
				app.RollBackPosition = 0
				app.RollBackFile = ""
			}

			continue
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

	// If the event is a dump event, increment the position by one
	if h.app.BinLogFile == DumpStep {
		h.app.BinLogPosition++
	} else {
		// For normal process, update the position and file from the event header
		h.app.BinLogPosition = e.Header.LogPos
		syncedPos := h.canal.SyncedPosition()
		h.app.BinLogFile = syncedPos.Name

	}

	h.app.Logger.Debug(fmt.Sprintf("Syncing position %d for file: %s", h.app.BinLogPosition, h.app.BinLogFile))

	// If a rollback is needed, skip the event and return the specific error
	if h.app.RollbackNeeded {
		return ErrRollback
	}

	// If rollback position is defined, skip the event if the position is lower than the rollback position
	if h.app.RollBackPosition != 0 && h.app.RollBackFile != "" {
		if h.app.BinLogPosition < h.app.RollBackPosition {
			h.app.Logger.Debug(fmt.Sprintf("Skipping event %d for file: %s. Already synced",
				h.app.BinLogPosition, h.app.BinLogFile))
			return nil
		}
		// Clean up the rollback position after reach it
		h.app.RollBackPosition = 0
		h.app.RollBackFile = ""
	}

	// Filter database and table included in the configuration
	if !watchEvent(h.app, e) {
		return nil
	}

	// Check if the server assigned to the event is the current server in the hashring
	if !reflect.ValueOf(h.app.Config.Hashring).IsZero() {
		severAssigned := h.ring.GetServer(fmt.Sprintf("%d", h.app.BinLogPosition))
		h.app.Logger.Debug(fmt.Sprintf("Server assigned: %s", severAssigned))
		if h.app.Config.ServerId != severAssigned {
			return nil
		}
	}

	// Get the column names from the event to process the row
	columnNames := make([]string, 0, len(e.Table.Columns))
	for _, column := range e.Table.Columns {
		columnNames = append(columnNames, column.Name)
	}

	// Process the row event
	jsonData, err = processRow(h.app, columnNames, e.Rows[0])
	if err != nil {
		return fmt.Errorf("error processing row: %v", err)
	}

	// Send the JSON data to connectors
	err = executeConnectors(h.app, e.Action, jsonData)
	if err != nil {
		h.app.Logger.Warn("Error executing connectors", zap.Error(err))
	}

	return nil

}

// OnPosSynced is called when the position is synced (for example when mysqldump ends)
func (h *CanalEventHandler) OnPosSynced(header *replication.EventHeader, pos mysql.Position, set mysql.GTIDSet, force bool) error {

	// If the event is in the DumpStep, it means that the dump process is finished. So, update the binlog position
	// and file with the last position and file from the dump process.
	if h.app.BinLogFile == DumpStep && pos.Name != "" {
		h.app.BinLogFile = pos.Name
		h.app.BinLogPosition = pos.Pos
		h.app.Logger.Info("Dump finished")
	}
	return nil
}
