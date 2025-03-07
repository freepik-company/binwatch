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
			go webhook.Send(app, connector.Data, jsonData)
		}
		if slices.Contains(connector.Events, event) && connector.Connector == "pubsub" {
			app.Logger.Debug("Sending data to pubsub connector", zap.String("connector", connector.Connector),
				zap.String("data", string(jsonData)), zap.String("event", event))
			go pubsub.Send(app, connector.Data, jsonData)
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
