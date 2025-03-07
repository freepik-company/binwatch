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

package dump

import (
	//
	"context"
	coreLog "log"
	"reflect"
	"strings"
	//
	"github.com/spf13/cobra"

	//
	"binwatch/api/v1alpha1"
	"binwatch/internal/config"
	"binwatch/internal/log"
	"binwatch/internal/sources/mysql"
)

const (
	descriptionShort = `Dump MySQL and send to Connectors`
	descriptionLong  = `
	Dump MySQL and send to Connectors for processing and storage.
	`
)

// NewCommand TODO
func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:                   "dump",
		DisableFlagsInUseLine: true,
		Short:                 descriptionShort,
		Long:                  strings.ReplaceAll(descriptionLong, "\t", ""),

		Run: DumpCommand,
	}

	cmd.Flags().String("config", "config.yaml", "Path to the YAML config file")

	return cmd
}

// DumpCommand TODO
func DumpCommand(cmd *cobra.Command, args []string) {

	// Configure application's context
	app := v1alpha1.Application{
		Config:  &v1alpha1.ConfigSpec{},
		Context: context.Background(),
	}

	// Check the flags for this command
	configPath, err := cmd.Flags().GetString("config")
	if err != nil {
		coreLog.Fatalf("Error getting configuration file path: %v", err)
	}

	// Get and parse the config
	configContent, err := config.ReadFile(configPath)
	if err != nil {
		coreLog.Fatalf("Error parsing configuration file: %v", err)
	}

	// Set the configuration inside the global context
	app.Config = &configContent

	// Check that server name is configured
	if app.Config.ServerId == "" {
		app.Logger.Fatal("Server name is required in configuration file `server_name`.")
	}

	// Configure logger
	logger, err := log.ConfigureLogger(&app)
	if err != nil {
		coreLog.Fatalf("Error configuring logger: %v", err)
	}
	app.Logger = logger

	// Run MySQL Dumper if MySQL config is present
	if !reflect.DeepEqual(app.Config.Sources.MySQL, v1alpha1.MySQLConfig{}) {
		app.Logger.Info("Starting MySQL dumper")
		mysql.Dumper(&app)
	} else {
		app.Logger.Fatal("No connector configuration found")
	}
}
