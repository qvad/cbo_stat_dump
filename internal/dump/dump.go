package dump

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5"
)

func Run(cfg Config) error {
	if cfg.Verbose {
		fmt.Println("Starting cbo_stat_dump...")
	}

	// Create output directory
	if err := os.MkdirAll(cfg.OutputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	connConfig, err := pgx.ParseConfig(fmt.Sprintf("postgres://%s:%s@%s:%d/%s", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database))
	if err != nil {
		return fmt.Errorf("failed to parse config: %w", err)
	}

	conn, err := pgx.ConnectConfig(context.Background(), connConfig)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer conn.Close(context.Background())

	dumper := NewDumper(conn, cfg)

	return dumper.Dump()
}

type Dumper struct {
	conn   *pgx.Conn
	config Config
}

func NewDumper(conn *pgx.Conn, cfg Config) *Dumper {
	return &Dumper{
		conn:   conn,
		config: cfg,
	}
}

func (d *Dumper) Dump() error {
	var relationNames []string
	var err error

	if d.config.QueryFile != "" {
		if d.config.Verbose {
			fmt.Println("Analyzing query file to identify relations...")
		}
		relationNames, err = d.GetRelationNamesInQuery(d.config.QueryFile)
		if err != nil {
			return fmt.Errorf("failed to analyze query: %w", err)
		}

		// Export query file copy
		// Python: export_query_file
		// TODO: copy file to output dir

		if err := d.ExportQueryPlan(d.config.QueryFile); err != nil {
			return fmt.Errorf("failed to export query plan: %w", err)
		}
	}

	if d.config.Verbose {
		fmt.Println("Exporting DDL...")
	}
	if err := d.ExportDDL(relationNames); err != nil {
		return fmt.Errorf("failed to export DDL: %w", err)
	}

	if d.config.Verbose {
		fmt.Println("Exporting Statistics...")
	}
	if err := d.ExportStatistics(relationNames); err != nil {
		return fmt.Errorf("failed to export statistics: %w", err)
	}

	if d.config.Verbose {
		fmt.Println("Exporting Extended Statistics...")
	}
	// Check version first? ExportExtendedStatistics logic should probably check version or be safe?
	// Python checks: if pg_major_version >= 15
	var versionStr string
	d.conn.QueryRow(context.Background(), "SHOW server_version_num").Scan(&versionStr)
	var pgVer int
	fmt.Sscanf(versionStr, "%d", &pgVer)
	if pgVer >= 150000 {
		if err := d.ExportExtendedStatistics(relationNames); err != nil {
			return fmt.Errorf("failed to export extended statistics: %w", err)
		}
	}

	if d.config.Verbose {
		fmt.Println("Exporting Version...")
	}
	if err := d.ExportVersion(); err != nil {
		return fmt.Errorf("failed to export version: %w", err)
	}

	if d.config.Verbose {
		fmt.Println("Exporting Overridden GUCs...")
	}
	if err := d.ExportOverriddenGUCs(); err != nil {
		return fmt.Errorf("failed to export GUCs: %w", err)
	}

	if d.config.YBMode {
		if d.config.Verbose {
			fmt.Println("Exporting GFlags...")
		}
		if err := d.ExportGFlags(); err != nil {
			// Non-critical
			fmt.Printf("Warning: %v\n", err)
		}
	}

	return nil
}
