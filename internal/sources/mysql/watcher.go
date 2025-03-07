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
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strings"
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
		db.Close()
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
			if app.Config.ServerName != severAssigned {
				continue
			}
		}

		// Handle the event
		switch e := ev.Event.(type) {

		// For query events (main case for STATEMENT bin-log configuration but also used in ROW) (DDL)
		case *replication.QueryEvent:

			// Get the query and print it
			query := string(e.Query)

			app.Logger.Info("Executed query for Schema", zap.String("schema", string(e.Schema)),
				zap.String("query", query), zap.Uint32("position", app.BinLogPosition),
				zap.String("file", app.BinLogFile))

			// If the query is an ALTER TABLE, clean up the memory used for the table metadata, so when an insert
			// is executed, the tableID is increased by one.
			matches := alterTableRegex.FindStringSubmatch(query)
			if len(matches) > 1 {

				// Get the table name
				tableName := strings.Trim(matches[1], "`")

				// Get the column names
				// Print the message to get to know the user that the columns are being retrieved from MySQL so it's a query
				app.Logger.Info("Getting columns for table", zap.String("table", tableName))
				columnNames, err := getColumnNames(string(e.Schema), tableName)
				if err != nil {
					app.Logger.Info("Error getting columns after ALTER TABLE", zap.Error(err))
				}

				// Replace table columns in memory
				tableMetadata[tableName] = columnNames
				app.Logger.Info("Updated metadata for table", zap.String("table", tableName),
					zap.Strings("columns", columnNames))

			}

		// Capture TableMapEvent to get the column names. Before a RowsEvent normally (with bin-log format ROW) (DML)
		// there are a TableMapEvent which the table and its metadata (columns).
		case *replication.TableMapEvent:

			// Get the table ID, schema and table name
			tableID := e.TableID
			schemaName := string(e.Schema)
			tableName := string(e.Table)

			app.Logger.Info("TableMapEvent detected", zap.String("schema", schemaName),
				zap.String("table", tableName), zap.Uint64("table_id", tableID),
				zap.Uint32("position", app.BinLogPosition), zap.String("file", app.BinLogFile))

			// Check if table metadata is already stored in memory
			_, exists := tableMetadata[tableName]
			if !exists {

				// If not exists, get the column names and store them in memory
				columnNames, err := getColumnNames(schemaName, tableName)
				if err != nil {
					app.Logger.Info("Error getting columns for table", zap.String("table", tableName),
						zap.Error(err))
				}

				tableMetadata[tableName] = columnNames
				app.Logger.Info("Found columns for table", zap.String("table", tableName),
					zap.Strings("columns", columnNames))

			}

		// For RowsEvent (DML) when bin-log format is ROW
		case *replication.RowsEvent:

			// Get the table ID and name
			tableID := e.TableID
			schemaName := string(e.Table.Schema)
			tableName := string(e.Table.Table)

			// Get the column names from memory, if not exists, skip the event (it must exist so always before a
			// RowsEvent there is a TableMapEvent)
			columnNames, ok := tableMetadata[tableName]
			if !ok {
				app.Logger.Info("Not found table metadata in memory", zap.String("table", tableName))
				continue
			}

			// Map the event type to a string for printing and verbosity
			eventType := ev.Header.EventType
			eventStr := ""
			switch eventType {
			case replication.WRITE_ROWS_EVENTv2:
				eventStr = "INSERT"
			case replication.UPDATE_ROWS_EVENTv2:
				eventStr = "UPDATE"
			case replication.DELETE_ROWS_EVENTv2:
				eventStr = "DELETE"
			}

			app.Logger.Info("Found event", zap.String("event", eventStr),
				zap.String("schema", schemaName), zap.String("table", tableName),
				zap.Uint64("table_id", tableID), zap.Uint32("position", app.BinLogPosition),
				zap.String("file", app.BinLogFile))

			// For UPDATE, the event receives the values in pairs (OLD, NEW), so we only take the NEW values
			// For INSERT and DELETE, the event receives the values directly
			init := 0
			hop := 1
			if eventType == replication.UPDATE_ROWS_EVENTv2 {
				init = 1
				hop = 2
			}

			// Iterate over the rows
			for i := init; i < len(e.Rows); i += hop {

				// Get the new row
				row := e.Rows[i]

				// Map the row values to the column names
				rowMap := make(map[string]interface{})
				for idx, value := range row {
					if idx < len(columnNames) {
						rowMap[columnNames[idx]] = value
					} else {
						rowMap[fmt.Sprintf("unknown_col_%d", idx)] = value
					}
				}

				// Convert the row to JSON
				jsonData, err := json.Marshal(rowMap)
				if err != nil {
					app.Logger.Info("Error marshaling json data", zap.Error(err))
					continue
				}

				// Print the JSON data
				app.Logger.Debug("JSON data", zap.String("data", string(jsonData)))

				// Send the JSON data to connectors
				executeConnectors(app, eventStr, jsonData)
			}
		}
	}
}
