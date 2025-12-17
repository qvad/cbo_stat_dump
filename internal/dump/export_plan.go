package dump

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
)

func (d *Dumper) ExportQueryPlan(queryPath string) error {
	queryBytes, err := os.ReadFile(queryPath)
	if err != nil {
		return fmt.Errorf("failed to read query file: %w", err)
	}
	query := string(queryBytes)

	if d.config.EnableBaseScansCostModel && d.config.YBMode {
		_, err := d.conn.Exec(context.Background(), "SET yb_enable_base_scans_cost_model=ON")
		if err != nil {
			return fmt.Errorf("failed to set yb_enable_base_scans_cost_model: %w", err)
		}
	}

	rows, err := d.conn.Query(context.Background(), "EXPLAIN "+query)
	if err != nil {
		return fmt.Errorf("failed to execute explain: %w", err)
	}
	defer rows.Close()

	outputPath := filepath.Join(d.config.OutputDir, "query_plan.txt")
	f, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create query_plan.txt: %w", err)
	}
	defer f.Close()

	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return fmt.Errorf("failed to scan explain output: %w", err)
		}
		if _, err := f.WriteString(line + "\n"); err != nil {
			return fmt.Errorf("failed to write to query_plan.txt: %w", err)
		}
	}
	return nil
}
