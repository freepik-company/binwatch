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

package watch

import (
	//
	"context"
	"log"
	"reflect"
	"strings"
	"time"

	//
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	//
	"binwatch/api/v1alpha1"
	"binwatch/internal/config"
	"binwatch/internal/hashring"
	"binwatch/internal/sources/mysql"
)

const (
	descriptionShort = `Start watching the MySQL binlog`
	descriptionLong  = `
	Start watching the MySQL binlog and track changes that occur in database tables.
	`
)

// NewCommand TODO
func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:                   "watch",
		DisableFlagsInUseLine: true,
		Short:                 descriptionShort,
		Long:                  strings.ReplaceAll(descriptionLong, "\t", ""),

		Run: WatchCommand,
	}

	cmd.Flags().String("config", "config.yaml", "Path to the YAML config file")

	return cmd
}

// WatchCommand TODO
func WatchCommand(cmd *cobra.Command, args []string) {

	// Check the flags for this command
	configPath, err := cmd.Flags().GetString("config")
	if err != nil {
		log.Fatalf("Error getting configuration file path: %v", err)
	}

	// Get and parse the config
	configContent, err := config.ReadFile(configPath)
	if err != nil {
		log.Fatalf("Error parsing configuration file: %v", err)
	}

	// Define logger level. Default is Info
	level := zapcore.InfoLevel
	if configContent.Logger.Level != "" {
		switch configContent.Logger.Level {
		case "debug":
			level = zapcore.DebugLevel
		case "info":
			level = zapcore.InfoLevel
		case "warn":
			level = zapcore.WarnLevel
		case "error":
			level = zapcore.ErrorLevel
		case "dpanic":
			level = zapcore.DPanicLevel
		case "panic":
			level = zapcore.PanicLevel
		case "fatal":
			level = zapcore.FatalLevel
		default:
			log.Printf("Invalid log level: %s. Setting INFO level by default", configContent.Logger.Level)
			level = zapcore.InfoLevel
		}
	}

	// Create a new logger
	logConfig := zap.Config{
		Encoding:         configContent.Logger.Encoding,
		Level:            zap.NewAtomicLevelAt(level),
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
		EncoderConfig:    zap.NewProductionEncoderConfig(),
	}

	// Set timestamp format
	logConfig.EncoderConfig.TimeKey = "time"
	logConfig.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	// Build the logger
	logger, err := logConfig.Build()
	if err != nil {
		log.Fatalf("Error creating logger: %v", err)
	}

	// Configure application's context
	app := v1alpha1.Application{
		Config:           &v1alpha1.ConfigSpec{},
		Logger:           logger,
		Context:          context.Background(),
		BinLogPosition:   0,
		BinLogFile:       "",
		RollBackPosition: 0,
		RollBackFile:     "",
	}

	// Set the configuration inside the global context
	app.Config = &configContent

	// Get server name and add it to logs
	if app.Config.ServerName == "" {
		app.Logger.Fatal("Server name is required in configuration file `server_name`.")
	}
	app.Logger = app.Logger.With(zap.String("server", app.Config.ServerName))

	// Try to add server to the Hashring
	hr := hashring.NewHashRing(1000)
	go hr.SyncWorker(&app, time.Duration(app.Config.Hashring.SyncWorkerTimeMs)*time.Millisecond)

	// If hashring is present, wait for the server list to be populated, any other case continue
	if !reflect.ValueOf(app.Config.Hashring).IsZero() {
		for {
			if len(hr.GetServerList()) != 0 {
				break
			}
			app.Logger.Info("Waiting for hashring servers to be ready...")
			time.Sleep(1 * time.Second)
		}
	}

	// Run MySQL Watcher if MySQL config is present
	if !reflect.DeepEqual(app.Config.Sources.MySQL, v1alpha1.MySQLConfig{}) {
		app.Logger.Info("Starting MySQL watcher")
		mysql.Watcher(&app, hr)
	} else {
		app.Logger.Fatal("No connector configuration found")
	}
}
