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

package sync

import (
	//

	"log"
	"strings"

	"github.com/spf13/cobra"

	//

	"binwatch/internal/binwatch"
)

const (
	descriptionShort = `Dump and watch (sync) MySQL and send to Connectors`
	descriptionLong  = `
	Dump and watch (sync) MySQL and send to Connectors for processing and storage.
	`
)

// NewCommand TODO
func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:                   "sync",
		DisableFlagsInUseLine: true,
		Short:                 descriptionShort,
		Long:                  strings.ReplaceAll(descriptionLong, "\t", ""),

		Run: SyncCommand,
	}

	cmd.Flags().String("config", "config.yaml", "Path to the YAML config file")

	return cmd
}

// SyncCommand TODO
func SyncCommand(cmd *cobra.Command, args []string) {
	// Check the flags for this command
	configPath, err := cmd.Flags().GetString("config")
	if err != nil {
		log.Fatalf("Error getting configuration file path: %s", err.Error())
	}

	var bw *binwatch.BinWatchT
	bw, err = binwatch.NewBinWatch(configPath)
	if err != nil {
		log.Fatalf("error in binwatch instance creation: %s", err.Error())
	}

	bw.Run()
}
