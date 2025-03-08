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
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"go.uber.org/zap"
	"time"
)

func Sync(app *v1alpha1.Application) {
	cfg := &canal.Config{
		ServerID:        app.Config.Sources.MySQL.ServerID,
		Flavor:          app.Config.Sources.MySQL.Flavor,
		Addr:            fmt.Sprintf("%s:%d", app.Config.Sources.MySQL.Host, app.Config.Sources.MySQL.Port),
		User:            app.Config.Sources.MySQL.User,
		Password:        app.Config.Sources.MySQL.Password,
		ReadTimeout:     time.Duration(app.Config.Sources.MySQL.ReadTimeout) * time.Second,
		HeartbeatPeriod: time.Duration(app.Config.Sources.MySQL.HeartbeatPeriod) * time.Second,
	}

	// Get databases and tables to dump
	if len(app.Config.Sources.MySQL.DumpConfig.Databases) > 1 {
		cfg.Dump.Databases = app.Config.Sources.MySQL.DumpConfig.Databases
		app.Logger.Info(fmt.Sprintf("database to dump: %s  with all tables", cfg.Dump.TableDB))
	} else if len(app.Config.Sources.MySQL.DumpConfig.Databases) == 1 {
		cfg.Dump.TableDB = app.Config.Sources.MySQL.DumpConfig.Databases[0]
		cfg.Dump.Tables = app.Config.Sources.MySQL.DumpConfig.Tables
		app.Logger.Info(fmt.Sprintf("database to dump: %s and tables: %v", cfg.Dump.TableDB, cfg.Dump.Tables))
	} else {
		app.Logger.Fatal("no database configured")
	}

	cfg.Dump.ExecutionPath = app.Config.Sources.MySQL.DumpConfig.MySQLDumpBinPath
	if app.Config.Sources.MySQL.DumpConfig.MySQLDumpBinPath == "" {
		cfg.Dump.ExecutionPath = "/bin/mysqldump"
	}

	cfg.Dump.SkipMasterData = true
	c, err := canal.NewCanal(cfg)
	if err != nil {
		app.Logger.Fatal("Error creating canal", zap.Error(err))
	}

	// Register a handler to handle RowsEvent
	c.SetEventHandler(&MyEventHandler{
		app: app,
	})

	// Start canal
	err = c.Run()
	if err != nil {
		app.Logger.Fatal("Error running canal", zap.Error(err))
	}

}

type MyEventHandler struct {
	canal.DummyEventHandler
	app *v1alpha1.Application
}

func (h *MyEventHandler) OnRow(e *canal.RowsEvent) error {
	// Filter database and table included in the configuration
	// By default watch all tables
	found := false
	if len(h.app.Config.Sources.MySQL.FilterTables) > 0 {
		for _, pair := range h.app.Config.Sources.MySQL.FilterTables {
			if pair.Database == e.Table.Schema && pair.Table == e.Table.Name {
				found = true
			}
		}
	}

	if !found {
		return nil
	}

	columnNames := make([]string, 0, len(e.Table.Columns))
	for _, column := range e.Table.Columns {
		columnNames = append(columnNames, column.Name)
	}

	err = processRow(h.app, "INSERT", columnNames, e.Rows[0])
	if err != nil {
		return fmt.Errorf("error processing row: %v", err)
	}

	return nil
}
