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
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"slices"
	"strings"

	"go.uber.org/zap"

	//
	"binwatch/api/v1alpha1"
	"binwatch/internal/connectors/pubsub"
	"binwatch/internal/connectors/webhook"
	"binwatch/internal/hashring"
)

// executeConnectors function to execute the connectors based on the configuration
func executeConnectors(app *v1alpha1.Application, eventStr string, jsonData []byte) (err error) {
	// Use errors.Join to combine multiple errors
	var errs []error

	// Get the event
	event := strings.ToLower(eventStr)

	// Execute the connectors based on the configuration routes
	for _, connector := range app.Config.Connectors.Routes {
		// Check webhook connector
		if slices.Contains(connector.Events, event) && connector.Connector == "webhook" {
			app.Logger.Debug("Sending data to webhook connector", zap.String("connector", connector.Connector),
				zap.String("data", string(jsonData)), zap.String("event", event))
			err = webhook.Send(app, connector.Data, jsonData)
			if err != nil {
				errs = append(errs, fmt.Errorf("error sending data to webhook connector: %v", err))
			}
		}

		// Check pubsub connector
		if slices.Contains(connector.Events, event) && connector.Connector == "pubsub" {
			app.Logger.Debug("Sending data to pubsub connector", zap.String("connector", connector.Connector),
				zap.String("data", string(jsonData)), zap.String("event", event))
			err = pubsub.Send(app, connector.Data, jsonData)
			if err != nil {
				errs = append(errs, fmt.Errorf("error sending data to pubsub connector: %v", err))
			}
		}
	}

	// If no errors occurred, return nil
	if len(errs) == 0 {
		return nil
	}

	// Use errors.Join to combine all errors (Go 1.20+)
	return errors.Join(errs...)
}

// getMinimalBinlogPosition function to get the minimal binlog position from the hashring
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
			// If files are different, always the lowest position will be the worst case so
			// its probably the position of the new binlog. So we restore the greater one
			if p > minPosition {
				minPosition = p
				minFile = f
				serverInitialized = server
				break
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

// processRow function to process the row event
func processRow(app *v1alpha1.Application, columnNames []string, row []interface{}) (jsonData []byte, err error) {

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
	jsonData, err = json.Marshal(rowMap)
	if err != nil {
		return jsonData, fmt.Errorf("error marshaling json data: %v", err)
	}

	// Print the JSON data
	app.Logger.Debug("JSON data", zap.String("data", string(jsonData)))

	return jsonData, nil
}

// watchEvent function to filter the events based on the configuration
func watchEvent(app *v1alpha1.Application, ev *canal.RowsEvent) bool {
	// Filter database and table included in the configuration
	// If no filters are defined, watch all tables
	if len(app.Config.Sources.MySQL.FilterTables) == 0 {
		return true
	}

	// If filters are defined, check if the table is in the list
	for _, pair := range app.Config.Sources.MySQL.FilterTables {
		if pair.Database == ev.Table.Schema && pair.Table == ev.Table.Name {
			return true
		}
	}

	return false
}
