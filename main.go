package main

import (
	"os"

	"github.com/logic-danderson/aalogbeat/cmd"

	_ "github.com/logic-danderson/aalogbeat/include"
)

func main() {
	if err := cmd.RootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
