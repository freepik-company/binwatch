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

package cmd

import (
	//
	"strings"

	//
	"github.com/spf13/cobra"

	//
	"binwatch/internal/cmd/sync"
)

const (
	descriptionShort = `Binwatch is a tool for watching files and directories for changes`
	descriptionLong  = `
	BinWatch is a tool designed to subscribe to a MySQL database's binlog and track changes that occur in database tables. 
	These changes are processed and sent to supported connectors in real-time.
	`
)

// NewRootCommand TODO
func NewRootCommand(name string) *cobra.Command {
	c := &cobra.Command{
		Use:   name,
		Short: descriptionShort,
		Long:  strings.ReplaceAll(descriptionLong, "\t", ""),
	}

	c.AddCommand(
		sync.NewCommand(),
	)

	return c
}
