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
)

// getColumnNames function to get the column names of a table
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

// getMasterStatus function to get the master status of the MySQL server and get the actual binlog position
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

// filterEvent function to filter the events based on the configuration
func filterEvent(app v1alpha1.Application, ev *replication.BinlogEvent) (bool, error) {

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
func executeConnectors(app v1alpha1.Application, eventStr string, jsonData []byte) {

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
