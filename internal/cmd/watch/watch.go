package watch

import (
	"binwatch/api/v1alpha1"
	"binwatch/internal/config"
	"binwatch/internal/sources/mysql"
	"context"
	"go.uber.org/zap/zapcore"
	"log"
	"reflect"

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
	ctx := v1alpha1.Context{
		Config:  &v1alpha1.ConfigSpec{},
		Logger:  logger,
		Context: context.Background(),
	}

	// Set the configuration inside the global context
	ctx.Config = &configContent

	// Run MySQL Watcher if MySQL config is present
	if !reflect.DeepEqual(ctx.Config.Sources.MySQL, v1alpha1.MySQLConfig{}) {
		mysql.Watcher(ctx)
	}
}
