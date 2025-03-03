package main

import (
	"binwatch/internal/cmd"
	"fmt"
	"os"
	"path/filepath"
)

func main() {

	baseName := filepath.Base(os.Args[0])

	if err := cmd.NewRootCommand(baseName).Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
