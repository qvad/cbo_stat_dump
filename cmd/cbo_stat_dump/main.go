package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/yugabyte/cbo_stat_dump/internal/dump"
)

func main() {
	config := dump.Config{}

	flag.StringVar(&config.Host, "h", "localhost", "Hostname or IP address")
	flag.IntVar(&config.Port, "p", 5433, "Port number")
	flag.StringVar(&config.Database, "d", "", "Database name")
	flag.StringVar(&config.User, "u", "", "Username")
	flag.StringVar(&config.Password, "W", "", "Password") // Note: -W is actually a flag for prompt in psql, but here we take string? Python script uses argparse which implies string unless store_true. Python: '-W', '--password'.
	// Wait, python script says:
	// parser.add_argument('-W', '--password', help='Password')

	flag.StringVar(&config.OutputDir, "o", "", "Output directory")
	flag.StringVar(&config.QueryFile, "q", "", "Query file path")
	flag.BoolVar(&config.YBMode, "yb_mode", false, "Use YugabyteDB mode")
	flag.BoolVar(&config.EnableBaseScansCostModel, "enable_base_scans_cost_model", false, "Enable base scans cost model")
	flag.BoolVar(&config.Verbose, "v", false, "Verbose output")

	flag.Parse()

	if config.Database == "" || config.User == "" {
		fmt.Println("Database and User are required.")
		flag.Usage()
		os.Exit(1)
	}

	if config.OutputDir == "" {
		fmt.Println("Output directory is required.")
		flag.Usage()
		os.Exit(1)
	}

	if err := dump.Run(config); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
