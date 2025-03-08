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
	"database/sql"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	//
	"github.com/go-mysql-org/go-mysql/replication"
	"go.uber.org/zap"

	//
	"binwatch/api/v1alpha1"
	"binwatch/internal/connectors/pubsub"
	"binwatch/internal/connectors/webhook"
	"binwatch/internal/hashring"
)

// getColumnNames function to get the column names of a table
func getColumnNames(app *v1alpha1.Application, schema, table string) (columnNames []string, err error) {

	// Open the connection to the MySQL server.
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/", app.Config.Sources.MySQL.User, app.Config.Sources.MySQL.Password,
		app.Config.Sources.MySQL.Host, app.Config.Sources.MySQL.Port)
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	// Query to get the columns
	query := fmt.Sprintf("SHOW COLUMNS FROM `%s`.`%s`", schema, table)
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Iterate over the rows and get the column names
	for rows.Next() {
		var colName string
		var colType, colNull, colKey, colDefault, colExtra sql.NullString
		if err = rows.Scan(&colName, &colType, &colNull, &colKey, &colDefault, &colExtra); err != nil {
			return nil, err
		}
		columnNames = append(columnNames, colName)
	}

	return columnNames, nil
}

// getMasterStatus function to get the master status of the MySQL server and get the actual binlog position
func getMasterStatus(app *v1alpha1.Application) (binLogPos uint32, binLogFile string, err error) {

	app.Logger.Debug("Getting last binlog position from MySQL server")
	// Open the connection to the MySQL server. It is closed at the end of the Handler function
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/", app.Config.Sources.MySQL.User, app.Config.Sources.MySQL.Password,
		app.Config.Sources.MySQL.Host, app.Config.Sources.MySQL.Port)
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		return binLogPos, binLogFile, err
	}
	defer db.Close()

	// Query to get the master status
	err = db.QueryRow("SHOW MASTER STATUS").Scan(&binLogFile, &binLogPos, new(interface{}), new(interface{}), new(interface{}))
	if err != nil {
		return binLogPos, binLogFile, err
	}

	return binLogPos, binLogFile, err
}

// filterEvent function to filter the events based on the configuration
func filterEvent(app *v1alpha1.Application, ev *replication.BinlogEvent) (bool, error) {

	// Get the event schema and table
	schema := ""
	table := ""
	switch e := ev.Event.(type) {
	case *replication.QueryEvent:
		schema = string(e.Schema)
	case *replication.TableMapEvent:
		schema = string(e.Schema)
		table = string(e.Table)
	case *replication.RowsEvent:
		schema = string(e.Table.Schema)
		table = string(e.Table.Table)
	}

	// Iterate over the filter tables
	for _, filter := range app.Config.Sources.MySQL.FilterTables {

		// Check if the event schema and table matches the filter
		if filter.Database == schema && filter.Table == table {
			return true, nil
		}
	}

	return false, nil
}

// executeConnectors function to execute the connectors based on the configuration
func executeConnectors(app *v1alpha1.Application, eventStr string, jsonData []byte) {

	event := strings.ToLower(eventStr)

	for _, connector := range app.Config.Connectors.Routes {
		if slices.Contains(connector.Events, event) && connector.Connector == "webhook" {
			app.Logger.Debug("Sending data to webhook connector", zap.String("connector", connector.Connector),
				zap.String("data", string(jsonData)), zap.String("event", event))
			webhook.Send(app, connector.Data, jsonData)
		}
		if slices.Contains(connector.Events, event) && connector.Connector == "pubsub" {
			app.Logger.Debug("Sending data to pubsub connector", zap.String("connector", connector.Connector),
				zap.String("data", string(jsonData)), zap.String("event", event))
			pubsub.Send(app, connector.Data, jsonData)
		}
	}

}

func getMinimalBinlogPosition(app *v1alpha1.Application, ring *hashring.HashRing) (uint32, string, error) {
	// Get current servers in the hashring
	servers := ring.GetServerList()
	var minPosition uint32
	var minFile string
	initialized := false
	serverInitialized := ""
	for _, server := range servers {

		// Skip the current server
		if server == app.Config.ServerId {
			continue
		}

		//
		p, f, err := ring.GetServerBinlogPositionMem(server)
		if err != nil {
			app.Logger.Error(fmt.Sprintf("Error getting binlog position for server %s", server), zap.Error(err))
			continue
		}

		// Initialize with the first valid position
		if !initialized {
			minPosition = p
			minFile = f
			serverInitialized = server
			initialized = true
			continue
		}

		// Compare files first if they're different
		if f != minFile {
			// You may need a more sophisticated comparison if files have a numeric sequence
			// For example: binlog.000001, binlog.000002, etc.
			if f < minFile {
				minPosition = p
				minFile = f
				serverInitialized = server
			}
		} else if p < minPosition {
			// If same file, compare positions
			minPosition = p
			minFile = f
			serverInitialized = server
		}
	}

	if initialized {
		app.Logger.Info(fmt.Sprintf("Most behind binlog position found in file %s at position %d from server %s",
			minFile, minPosition, serverInitialized),
			zap.String("file", minFile),
			zap.Uint32("position", minPosition))
		return minPosition, minFile, nil
	}

	app.Logger.Warn("Could not determine binlog position from any server")
	return minPosition, minFile, nil
}

func processEventBinLog(app *v1alpha1.Application, ev *replication.BinlogEvent) (err error) {
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
			columnNames, err := getColumnNames(app, string(e.Schema), tableName)
			if err != nil {
				return fmt.Errorf("error getting columns after ALTER TABLE: %v", err)
			}

			// Replace table columns in memory
			tableMetadata[tableName] = columnNames
			app.Logger.Info("Updated metadata for table", zap.String("table", tableName),
				zap.Strings("columns", columnNames))

		}

		return nil

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
			columnNames, err := getColumnNames(app, schemaName, tableName)
			if err != nil {
				return fmt.Errorf("error getting columns for table %s : %v", tableName, err)
			}

			tableMetadata[tableName] = columnNames
			app.Logger.Info("Found columns for table", zap.String("table", tableName),
				zap.Strings("columns", columnNames))

		}

		return nil

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
			return fmt.Errorf("not found table %s metadata in memory", tableName)
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

			err = processRow(app, eventStr, columnNames, row)
			if err != nil {
				return fmt.Errorf("error processing row: %v", err)
			}

		}
		return nil
	}
	return nil
}

func processRow(app *v1alpha1.Application, eventStr string, columnNames []string, row []interface{}) error {

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
		return fmt.Errorf("error marshaling json data: %v", err)
	}

	// Print the JSON data
	app.Logger.Debug("JSON data", zap.String("data", string(jsonData)))

	// Send the JSON data to connectors
	executeConnectors(app, eventStr, jsonData)

	return nil
}
