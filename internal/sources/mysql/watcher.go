package mysql

import (
	"binwatch/internal/connectors/pubsub"
	"binwatch/internal/connectors/webhook"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"go.uber.org/zap"
	"reflect"
	"regexp"
	"strings"
	"time"

	"binwatch/api/v1alpha1"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	_ "github.com/go-sql-driver/mysql"
)

var (
	tableMetadata   = make(map[string][]string)
	alterTableRegex = regexp.MustCompile(`(?i)ALTER\s+TABLE\s+(\S+)`)
	db              *sql.DB
	binLogFile      *string
	binLogPos       *uint32
)

// Función principal para capturar el binlog
func Watcher(ctx v1alpha1.Context) {

	// Get configuration from the context
	host := ctx.Config.Sources.MySQL.Host
	port := ctx.Config.Sources.MySQL.Port
	user := ctx.Config.Sources.MySQL.User
	password := ctx.Config.Sources.MySQL.Password
	serverID := ctx.Config.Sources.MySQL.ServerID
	flavor := ctx.Config.Sources.MySQL.Flavor
	readTimeout := ctx.Config.Sources.MySQL.ReadTimeout
	heartbeatPeriod := ctx.Config.Sources.MySQL.HeartbeatPeriod

	logger := ctx.Logger

	syncTimeoutMs := ctx.Config.Sources.MySQL.SyncTimeoutMs

	// Get the current binlog position
	err := getMasterStatus(host, port, user, password)
	if err != nil {
		logger.Error("Error getting actual position of binlog", zap.Error(err))
	}
	defer db.Close()
	logger.Info("Starting binlog capture", zap.String("binlog_file", *binLogFile), zap.Uint32("binlog_pos", *binLogPos))

	// configure the MySQL connection
	cfg := replication.BinlogSyncerConfig{
		ServerID:        serverID,
		Flavor:          flavor,
		Host:            host,
		Port:            port,
		User:            user,
		Password:        password,
		ReadTimeout:     time.Duration(readTimeout) * time.Second,
		HeartbeatPeriod: time.Duration(heartbeatPeriod) * time.Second,
	}

	// Start the binlog syncer from the actual position
	syncer := replication.NewBinlogSyncer(cfg)
	pos := mysql.Position{Name: *binLogFile, Pos: *binLogPos}
	streamer, err := syncer.StartSync(pos)
	if err != nil {
		logger.Error("Error starting sync for binlogs", zap.Error(err))
	}

	// Process the events
	for {

		// Set timeout for processing events
		sqlctx, cancel := context.WithTimeout(context.Background(), time.Duration(syncTimeoutMs)*time.Millisecond)

		// Get the next event
		ev, err := streamer.GetEvent(sqlctx)
		cancel()

		// Handle errors
		if err != nil {

			// Handle context timeout
			if err == context.DeadlineExceeded {
				continue
			}

			// Handle other errors
			logger.Info("Error getting the event", zap.Error(err))
			continue
		}

		// Filter database and table included in the configuration
		// By default watch all tables
		if len(ctx.Config.Sources.MySQL.FilterTables) > 0 {
			ok, err := filterEvent(ctx, ev)
			if err != nil {
				logger.Info("Error filtering event", zap.Error(err))
			}
			if !ok {
				continue
			}
		}

		// Handle the event
		switch e := ev.Event.(type) {

		// For query events (main case for STATEMENT bin-log configuration but also used in ROW) (DDL)
		case *replication.QueryEvent:

			// Get the query and print it
			query := string(e.Query)
			logger.Info("Executed query for Schema", zap.String("schema", string(e.Schema)), zap.String("query", query))

			// If the query is an ALTER TABLE, clean up the memory used for the table metadata, so when an insert
			// is executed, the tableID is increased by one.
			matches := alterTableRegex.FindStringSubmatch(query)
			if len(matches) > 1 {

				// Get the table name
				tableName := strings.Trim(matches[1], "`")

				// Get the column names
				// Print the message to get to know the user that the columns are being retrieved from MySQL so it's a query
				logger.Info("Getting columns for table", zap.String("table", tableName))
				columnNames, err := getColumnNames(string(e.Schema), tableName)
				if err != nil {
					logger.Info("Error getting columns after ALTER TABLE", zap.Error(err))
				}

				// Replace table columns in memory
				tableMetadata[tableName] = columnNames
				logger.Info("Updated metadata for table", zap.String("table", tableName), zap.Strings("columns", columnNames))

			}

		// Capture TableMapEvent to get the column names. Before a RowsEvent normally (with bin-log format ROW) (DML)
		// there are a TableMapEvent which the table and its metadata (columns).
		case *replication.TableMapEvent:

			// Get the table ID, schema and table name
			tableID := e.TableID
			schemaName := string(e.Schema)
			tableName := string(e.Table)

			logger.Info("TableMapEvent detected", zap.String("schema", schemaName), zap.String("table", tableName), zap.Uint64("table_id", tableID))

			// Check if table metadata is already stored in memory
			_, exists := tableMetadata[tableName]
			if !exists {

				// If not exists, get the column names and store them in memory
				columnNames, err := getColumnNames(schemaName, tableName)
				if err != nil {
					logger.Info("Error getting columns for table", zap.String("table", tableName), zap.Error(err))
				}

				tableMetadata[tableName] = columnNames
				logger.Info("Found columns for table", zap.String("table", tableName), zap.Strings("columns", columnNames))

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
				logger.Info("Not found table metadata in memory", zap.String("table", tableName))
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

			logger.Info("Found event", zap.String("event", eventStr), zap.String("schema", schemaName), zap.String("table", tableName), zap.Uint64("table_id", tableID))

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
					logger.Info("Error marshaling json data", zap.Error(err))
					continue
				}

				// Print the JSON data
				logger.Debug("JSON data", zap.String("data", string(jsonData)))

				// Send the JSON data to connectors
				executeConnectors(ctx, jsonData)
			}
		}
	}
}

// Function to get the column names of a table
func getColumnNames(schema, table string) ([]string, error) {

	// Query to get the columns
	query := fmt.Sprintf("SHOW COLUMNS FROM `%s`.`%s`", schema, table)
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Iterate over the rows and get the column names
	var columnNames []string
	for rows.Next() {
		var colName string
		var colType, colNull, colKey, colDefault, colExtra sql.NullString
		if err := rows.Scan(&colName, &colType, &colNull, &colKey, &colDefault, &colExtra); err != nil {
			return nil, err
		}
		columnNames = append(columnNames, colName)
	}

	return columnNames, nil
}

// Function to get the master status of the MySQL server and get the actual binlog position
func getMasterStatus(host string, port uint16, user, password string) (err error) {

	// Open the connection to the MySQL server. It is closed at the end of the Handler function
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/", user, password, host, port)
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		return err
	}

	// Query to get the master status
	err = db.QueryRow("SHOW MASTER STATUS").Scan(&binLogFile, &binLogPos, new(interface{}), new(interface{}), new(interface{}))
	if err != nil {
		return err
	}

	return err
}

// Function to filter the events based on the configuration
func filterEvent(ctx v1alpha1.Context, ev *replication.BinlogEvent) (bool, error) {

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
	for _, filter := range ctx.Config.Sources.MySQL.FilterTables {

		// Check if the event schema and table matches the filter
		if filter.Database == schema && filter.Table == table {
			return true, nil
		}
	}

	return false, nil
}

func executeConnectors(ctx v1alpha1.Context, jsonData []byte) {

	// Send the JSON data to connectors
	if !reflect.DeepEqual(ctx.Config.Connectors.WebHook, v1alpha1.WebHookConfig{}) {
		go webhook.Send(ctx, jsonData)
	}

	if !reflect.DeepEqual(ctx.Config.Connectors.PubSub, v1alpha1.PubSubConfig{}) {
		go pubsub.Send(ctx, jsonData)
	}

}
