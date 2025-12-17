package dump

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	VersionFile        = "version.txt"
	OverriddenGUCsFile = "overridden_gucs.sql"
	GFlagsFile         = "gflags.json"
)

var CBORelevantGUCParams = map[string]bool{
	"enable_seqscan":                        true,
	"enable_indexscan":                      true,
	"enable_bitmapscan":                     true,
	"enable_indexonlyscan":                  true,
	"enable_tidscan":                        true,
	"enable_sort":                           true,
	"enable_hashagg":                        true,
	"enable_nestloop":                       true,
	"enable_material":                       true,
	"enable_mergejoin":                      true,
	"enable_hashjoin":                       true,
	"enable_gathermerge":                    true,
	"enable_partitionwise_join":             true,
	"enable_partitionwise_aggregate":        true,
	"enable_parallel_append":                true,
	"enable_parallel_hash":                  true,
	"enable_partition_pruning":              true,
	"random_page_cost":                      true,
	"seq_page_cost":                         true,
	"cpu_tuple_cost":                        true,
	"cpu_index_tuple_cost":                  true,
	"cpu_operator_cost":                     true,
	"effective_cache_size":                  true,
	"shared_buffers":                        true,
	"work_mem":                              true,
	"maintenance_work_mem":                  true,
	"default_statistics_target":             true,
	"max_parallel_workers_per_gather":       true,
	"yb_enable_geolocation_costing":         true,
	"yb_enable_batchednl":                   true,
	"yb_enable_parallel_append":             true,
	"yb_enable_bitmapscan":                  true,
	"yb_enable_base_scans_cost_model":       true,
	"yb_bnl_batch_size":                     true,
	"yb_enable_expression_pushdown":         true,
	"yb_test_planner_custom_plan_threshold": true,
}

func (d *Dumper) ExportVersion() error {
	var version string
	if err := d.conn.QueryRow(context.Background(), "SELECT version()").Scan(&version); err != nil {
		return fmt.Errorf("failed to get version: %w", err)
	}
	if err := os.WriteFile(filepath.Join(d.config.OutputDir, VersionFile), []byte(version), 0644); err != nil {
		return fmt.Errorf("failed to write version.txt: %w", err)
	}
	return nil
}

func (d *Dumper) ExportOverriddenGUCs() error {
	rows, err := d.conn.Query(context.Background(), "SELECT name, setting FROM pg_settings WHERE setting <> boot_val")
	if err != nil {
		return fmt.Errorf("failed to query pg_settings: %w", err)
	}
	defer rows.Close()

	var lines []string
	for rows.Next() {
		var name, setting string
		if err := rows.Scan(&name, &setting); err != nil {
			return fmt.Errorf("failed to scan guc: %w", err)
		}
		if CBORelevantGUCParams[name] {
			lines = append(lines, fmt.Sprintf("SET %s='%s';\n", name, setting))
		}
	}

	if err := os.WriteFile(filepath.Join(d.config.OutputDir, OverriddenGUCsFile), []byte(strings.Join(lines, "")), 0644); err != nil {
		return fmt.Errorf("failed to write overridden_gucs.sql: %w", err)
	}
	return nil
}

func (d *Dumper) ExportGFlags() error {
	url := fmt.Sprintf("http://%s:7000/api/v1/varz", d.config.Host)
	client := http.Client{
		Timeout: 2 * time.Second,
	}
	resp, err := client.Get(url)
	if err != nil {
		// Just log or ignore if not reachable (YB specific)
		fmt.Printf("Warning: failed to fetch gflags: %v\n", err)
		return nil
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read gflags response: %w", err)
	}

	var data map[string]interface{}
	if err := json.Unmarshal(body, &data); err != nil {
		return fmt.Errorf("failed to parse gflags json: %w", err)
	}

	gflagsDict := make(map[string]interface{})
	if flags, ok := data["flags"].([]interface{}); ok {
		for _, f := range flags {
			flagMap, ok := f.(map[string]interface{})
			if !ok {
				continue
			}
			if t, ok := flagMap["type"].(string); ok && t == "Custom" {
				if name, ok := flagMap["name"].(string); ok {
					gflagsDict[name] = flagMap["value"]
				}
			}
		}
	}

	jsonOutput, err := json.MarshalIndent(gflagsDict, "", "    ")
	if err != nil {
		return fmt.Errorf("failed to marshal gflags: %w", err)
	}
	if err := os.WriteFile(filepath.Join(d.config.OutputDir, GFlagsFile), jsonOutput, 0644); err != nil {
		return fmt.Errorf("failed to write gflags.json: %w", err)
	}

	return nil
}
