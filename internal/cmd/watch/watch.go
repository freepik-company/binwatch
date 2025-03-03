package watch

import (
	"binwatch/api/v1alpha1"
	"binwatch/internal/config"
	"binwatch/internal/sources/mysql"
	"log"

	"strings"

	"github.com/spf13/cobra"
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

	// Configure application's context
	ctx := v1alpha1.Context{
		Config: &v1alpha1.ConfigSpec{},
	}

	// Get and parse the config
	configContent, err := config.ReadFile(configPath)
	if err != nil {
		log.Fatalf("Error parsing configuration file: %v", err)
	}

	// Set the configuration inside the global context
	ctx.Config = &configContent

	//
	mysql.Watcher(ctx)
}
