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
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"slices"
	"strings"
	"time"

	"go.uber.org/zap"

	//
	"binwatch/api/v1alpha1"
	"binwatch/internal/connectors/pubsub"
	"binwatch/internal/connectors/webhook"
	"binwatch/internal/hashring"
)

// executeConnectors function to execute the connectors based on the configuration
// This function is intended to be run as a goroutine
func executeConnectors(app *v1alpha1.Application, connectorsQueue *ConnectorsQueue) {
	for {
		var eventStr string
		var eventTable string
		var eventDatabase string
		var data []byte

		// Safely extract an item from the queue
		connectorsQueue.mutex.Lock()
		if len(connectorsQueue.queue) > 0 {
			app.Logger.Debug(fmt.Sprintf("Connectors Queue size: %d", len(connectorsQueue.queue)))
			eventStr = connectorsQueue.queue[0].eventType
			data = connectorsQueue.queue[0].data
			eventTable = connectorsQueue.queue[0].eventTable
			eventDatabase = connectorsQueue.queue[0].eventDatabase
			connectorsQueue.queue = connectorsQueue.queue[1:]
			connectorsQueue.mutex.Unlock()

			// Convert event to lowercase
			event := strings.ToLower(eventStr)

			// Execute connectors outside the lock
			for _, connector := range app.Config.Connectors.Routes {
				// Check if connector handles this event
				if slices.Contains(connector.Events, event) {
					// Process webhooks
					for _, wh := range app.Config.Connectors.WebHook {
						if wh.Name == connector.Connector && connector.Table == eventTable && connector.Database == eventDatabase {
							app.Logger.Debug("Sending data to webhook connector",
								zap.String("connector", connector.Connector),
								zap.String("data", string(data)),
								zap.String("event", event))
							err := webhook.Send(app, connector.Data, wh, data)
							if err != nil {
								app.Logger.Error(fmt.Sprintf("error sending data to webhook connector %s", connector.Connector),
									zap.Error(err))
							}
						}
					}

					// Process pubsub
					for _, pb := range app.Config.Connectors.PubSub {
						if pb.Name == connector.Connector && connector.Table == eventTable && connector.Database == eventDatabase {
							app.Logger.Debug("Sending data to pubsub connector",
								zap.String("connector", connector.Connector),
								zap.String("data", string(data)),
								zap.String("event", event))
							err := pubsub.Send(app, connector.Data, pb, data)
							if err != nil {
								app.Logger.Error(fmt.Sprintf("error sending data to pubsub connector %s", connector.Connector),
									zap.Error(err))
							}
						}
					}
				}
			}
		} else {
			connectorsQueue.mutex.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// getMinimalBinlogPosition function to get the minimal binlog position from the hashring
func getMinimalBinlogPosition(app *v1alpha1.Application, ring *hashring.HashRing) (uint32, string, error) {
	// Get current servers in the hashring
	servers := ring.GetServerList()
	var minPosition uint32
	var minFile string
	var initialized bool
	for _, server := range servers {

		// Skip the current server
		if server == app.Config.ServerId {
			continue
		}

		//
		pos, fil, err := ring.GetServerBinlogPositionMem(server)
		if err != nil {
			app.Logger.Error(fmt.Sprintf("Error getting binlog position for server %s", server), zap.Error(err))
			continue
		}

		//
		if !initialized {
			minPosition = pos
			minFile = fil
			initialized = true
			continue
		}

		//
		if fil == minFile && fil == DumpStep {
			if pos < minPosition {
				minPosition = pos
			}
		}

		if fil != minFile && fil == DumpStep {
			minFile = fil
			minPosition = pos
		}

		if fil < minFile && fil != DumpStep {
			minFile = fil
			minPosition = pos
		}
	}

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

// calculateSleepTime function to calculate the sleep time between events in base of the queue size
func calculateSleepTime(app *v1alpha1.Application, connectorsQueue *ConnectorsQueue) {
	if len(app.Config.FlowControl.Thresholds) > 0 {
		for {
			sleepTime := time.Duration(0)
			queueSize := len(connectorsQueue.queue)
			for _, flowControl := range app.Config.FlowControl.Thresholds {
				if queueSize >= flowControl.QueueSize {
					sleepTime, err = time.ParseDuration(flowControl.SleepTime)
					if err != nil {
						app.Logger.Error("Error parsing sleep time", zap.Error(err))
					}
				}
			}
			if sleepTime != 0 {
				app.Logger.Warn(fmt.Sprintf("Too many elements in queue %d, sleeping %v seconds between events", queueSize, sleepTime))
			}
			app.SleepTime = sleepTime
			checkInterval, err := time.ParseDuration(app.Config.FlowControl.CheckInterval)
			if err != nil {
				app.Logger.Error("Error parsing check interval", zap.Error(err))
			}
			time.Sleep(checkInterval)
		}
	}
}
