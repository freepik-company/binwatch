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
	"bufio"
	"fmt"
	"github.com/go-mysql-org/go-mysql/dump"
	"go.uber.org/zap"
	"io"
	"regexp"
	"strings"
	"sync"

	"binwatch/api/v1alpha1"
)

// Regular expression to extract the table name from INSERT INTO
var insertTableRegex = regexp.MustCompile(`INSERT INTO\s+` + "`?([^` ]+)`?" + `\s+`)

// Dumper function to dump the MySQL tables with concurrent processing
func Dumper(app *v1alpha1.Application) {

	// Iterate over the tables and databases to dump defined in the configuration file
	for _, pair := range app.Config.Sources.MySQL.FilterTables {

		// Configure the dumper
		dumper, err := dump.NewDumper("mysqldump", fmt.Sprintf("%s:%d", app.Config.Sources.MySQL.Host,
			app.Config.Sources.MySQL.Port), app.Config.Sources.MySQL.User, app.Config.Sources.MySQL.Password)
		if err != nil {
			app.Logger.Fatal("Error creating dumper", zap.Error(err))
		}

		// Set the database and table to dump
		dumper.TableDB = pair.Database
		dumper.Tables = []string{pair.Table}

		// Skip master data and set extra options for complete insert
		dumper.SkipMasterData(true)
		dumper.SetExtraOptions([]string{"--complete-insert"})

		// Process the dump as binlog events concurrently
		if err := processDumpAsBinlog(app, dumper, pair.Database, []string{pair.Table}); err != nil {
			app.Logger.Fatal("Error processing dump", zap.Error(err))
		}
	}
}

// processDumpAsBinlog process the dump as binlog events, concurrenlty processing the INSERT INTO statements
func processDumpAsBinlog(app *v1alpha1.Application, dumper *dump.Dumper, database string, tables []string) error {

	// Create a pipe to read the dump output
	r, w := io.Pipe()

	// Channel to send lines to workers
	linesChan := make(chan string, 100)

	// Wait group to wait for workers to finish
	var wg sync.WaitGroup

	// Execute workers to process the INSERT INTO statements. Workers are defined in configuration file
	// with a default of 1 worker if not defined
	if app.Config.Sources.MySQL.DumpWorkers == 0 {
		app.Config.Sources.MySQL.DumpWorkers = 1
	}
	wg.Add(app.Config.Sources.MySQL.DumpWorkers)
	for i := 0; i < app.Config.Sources.MySQL.DumpWorkers; i++ {
		// Each worker execute the processInsertAsBinlogEvent function for each line
		// received from the channel
		go func(id int) {
			defer wg.Done()
			for line := range linesChan {
				processInsertAsBinlogEvent(app, database, line, id)
			}
		}(i)
	}

	// Execute the dump and send the output to the pipe in other goroutine
	go func() {
		defer w.Close()
		err = dumper.Dump(w)
		if err != nil {
			app.Logger.Error("Error executing dump", zap.Error(err))
		}
	}()

	// First load the table metadata to get the column names
	if err := preloadTableMetadata(app, database, tables); err != nil {
		return fmt.Errorf("error preloading table metadata: %w", err)
	}

	// Read the dump output line by line and send the INSERT INTO statements to the workers
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "INSERT INTO") {
			linesChan <- line
		}
	}

	// Close the channel and wait for workers to finish
	close(linesChan)
	wg.Wait()

	// Check for errors in the scanner
	if err = scanner.Err(); err != nil {
		return fmt.Errorf("error reading dump output: %w", err)
	}

	app.Logger.Info("Dump processing completed",
		zap.String("database", database),
		zap.Strings("tables", tables))

	return nil
}

// processInsertAsBinlogEvent process the INSERT INTO statements from the dump as binlog events
func processInsertAsBinlogEvent(app *v1alpha1.Application, database string, line string, workerID int) {

	// Extract the table name from the INSERT INTO statement
	tableMatches := insertTableRegex.FindStringSubmatch(line)
	// If the table name is not found, log an error and return
	if len(tableMatches) < 2 {
		app.Logger.Error("Could not extract table name from INSERT", zap.String("line", truncateString(line, 100)))
		return
	}
	tableName := strings.Trim(tableMatches[1], "`")

	// Check if the table metadata is available
	columnNames, exists := tableMetadata[tableName]
	if !exists {
		app.Logger.Error("Table metadata not found", zap.String("table", tableName))
		return
	}

	// Extract the values from the INSERT INTO statement
	values := extractValuesFromInsert(line)
	if len(values) == 0 {
		app.Logger.Error("Could not extract values from INSERT", zap.String("line", truncateString(line, 100)))
		return
	}

	app.Logger.Info("Processing INSERT",
		zap.String("worker", fmt.Sprintf("%d", workerID)),
		zap.String("schema", database),
		zap.String("table", tableName),
		zap.Int("rows", len(values)))

	// Process each row of values as an INSERT event
	for _, rowStr := range values {

		// Parse the row values
		row := parseRowValues(rowStr)

		// Process the row
		err = processRow(app, "INSERT", database, tableName, columnNames, row)
		if err != nil {
			app.Logger.Error("error processing row", zap.Error(err))
			return
		}
	}
}
