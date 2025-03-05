package watch

import (
	"binwatch/api/v1alpha1"
	"binwatch/internal/config"
	"binwatch/internal/hashring"
	"binwatch/internal/sources/mysql"
	"context"
	"go.uber.org/zap/zapcore"
	"log"
	"reflect"
	"time"

	"strings"

	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

const (
	descriptionShort = `TODO`
	descriptionLong  = `
	TODO`
)

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
	logger, err := logConfig.Build()
	if err != nil {
		log.Fatalf("Error creating logger: %v", err)
	}

	// Configure application's context
	app := v1alpha1.Application{
		Config:  &v1alpha1.ConfigSpec{},
		Logger:  logger,
		Context: context.Background(),
	}

	// Set the configuration inside the global context
	app.Config = &configContent

	// Add server to the Hashring
	hr := hashring.NewHashRing(1000)
	go hr.SyncWorker(&app, time.Duration(app.Config.Hashring.SyncWorkerTimeMs)*time.Millisecond)

	if !reflect.ValueOf(app.Config.Hashring).IsZero() {
		for {
			if len(hr.GetServerList()) != 0 {
				break
			}
			time.Sleep(1 * time.Second)
		}
	}

	// Get server name and add it to logs
	if app.Config.ServerName == "" {
		log.Fatalf("Server name is required")
	}
	app.Logger = app.Logger.With(zap.String("server", app.Config.ServerName))

	// Run MySQL Watcher if MySQL config is present
	if !reflect.DeepEqual(app.Config.Sources.MySQL, v1alpha1.MySQLConfig{}) {
		mysql.Watcher(app, hr)
	}
}
