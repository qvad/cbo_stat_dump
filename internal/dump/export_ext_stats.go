package dump

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	StatisticExtJSONFile      = "statistic_ext.json"
	ImportStatisticExtSQLFile = "import_statistics_ext.sql"
)

type ExtendedStatisticsDump struct {
	Version            string               `json:"version"`
	PgStatisticExt     []PgStatisticExt     `json:"pg_statistic_ext"`
	PgStatisticExtData []PgStatisticExtData `json:"pg_statistic_ext_data"`
}

type PgStatisticExt struct {
	Relname       string      `json:"relname"`
	Stxname       string      `json:"stxname"`
	Nspname       string      `json:"nspname"`
	Stxowner      uint32      `json:"stxowner"`
	Stxstattarget int32       `json:"stxstattarget"`
	Stxkeys       string      `json:"stxkeys"`
	Stxkind       interface{} `json:"stxkind"` // char array? usually string like "{d,f}"
	Stxexprs      interface{} `json:"stxexprs"`
}

type PgStatisticExtData struct {
	Stxname          string        `json:"stxname"`
	Stxdinherit      bool          `json:"stxdinherit"`
	Stxdndistinct    interface{}   `json:"stxdndistinct"`    // bytea encoded as string? or raw?
	Stxddependencies interface{}   `json:"stxddependencies"` // bytea
	Stxdmcv          interface{}   `json:"stxdmcv"`          // bytea
	Stxdexpr         []interface{} `json:"stxdexpr"`         // list of stats
}

func (d *Dumper) ExportExtendedStatistics(relationNames []string) error {
	schemasFilter := " AND n.nspname NOT IN ('pg_catalog', 'pg_toast', 'information_schema')"
	relationNamesFilter := ""
	if len(relationNames) > 0 {
		var quotedRels []string
		for _, r := range relationNames {
			quotedRels = append(quotedRels, fmt.Sprintf("'%s'::regclass::oid", r))
		}
		relsStr := strings.Join(quotedRels, ", ")
		relationNamesFilter = fmt.Sprintf(" AND (c.oid IN (%s) OR c.oid IN (SELECT indexrelid FROM pg_index WHERE indrelid IN (%s)))", relsStr, relsStr)
	}

	// 1. Fetch pg_statistic_ext
	queryExt := fmt.Sprintf(`
        SELECT row_to_json(t) FROM 
            (SELECT c.relname, s.stxname, n.nspname, s.stxowner, s.stxstattarget, string_agg(a.attname, ',') as stxkeys, s.stxkind, s.stxexprs 
             FROM 
                pg_class c 
                JOIN pg_statistic_ext s ON c.oid = s.stxrelid 
                JOIN pg_attribute a ON c.oid = a.attrelid AND a.attnum = ANY(s.stxkeys)
                JOIN pg_namespace n ON c.relnamespace = n.oid %s %s
                GROUP BY c.relname, s.stxname, n.nspname, s.stxowner, s.stxstattarget, s.stxkind, s.stxexprs) t
    `, schemasFilter, relationNamesFilter)

	rowsExt, err := d.conn.Query(context.Background(), queryExt)
	if err != nil {
		return fmt.Errorf("failed to query pg_statistic_ext: %w", err)
	}
	defer rowsExt.Close()

	var pgStatExt []PgStatisticExt
	for rowsExt.Next() {
		var jsonBytes []byte
		if err := rowsExt.Scan(&jsonBytes); err != nil {
			return fmt.Errorf("failed to scan pg_statistic_ext json: %w", err)
		}
		var stat PgStatisticExt
		if err := json.Unmarshal(jsonBytes, &stat); err != nil {
			return fmt.Errorf("failed to unmarshal pg_statistic_ext json: %w", err)
		}
		pgStatExt = append(pgStatExt, stat)
	}

	// 2. Fetch pg_statistic_ext_data
	queryExtData := `
        SELECT row_to_json(t) FROM 
            (SELECT s.stxname, d.stxdinherit, d.stxdndistinct::bytea, d.stxddependencies::bytea, d.stxdmcv::bytea, d.stxdexpr
                FROM
                    pg_statistic_ext s JOIN pg_statistic_ext_data d ON s.oid = d.stxoid) t
    `

	rowsExtData, err := d.conn.Query(context.Background(), queryExtData)
	if err != nil {
		return fmt.Errorf("failed to query pg_statistic_ext_data: %w", err)
	}
	defer rowsExtData.Close()

	var pgStatExtData []PgStatisticExtData
	for rowsExtData.Next() {
		var jsonBytes []byte
		if err := rowsExtData.Scan(&jsonBytes); err != nil {
			return fmt.Errorf("failed to scan pg_statistic_ext_data json: %w", err)
		}
		var stat PgStatisticExtData
		if err := json.Unmarshal(jsonBytes, &stat); err != nil {
			return fmt.Errorf("failed to unmarshal pg_statistic_ext_data json: %w", err)
		}
		pgStatExtData = append(pgStatExtData, stat)
	}

	dumpData := ExtendedStatisticsDump{
		Version:            "0.0.1",
		PgStatisticExt:     pgStatExt,
		PgStatisticExtData: pgStatExtData,
	}

	// Write JSON
	jsonOutput, err := json.MarshalIndent(dumpData, "", "    ")
	if err != nil {
		return fmt.Errorf("failed to marshal extended stats to json: %w", err)
	}
	if err := os.WriteFile(filepath.Join(d.config.OutputDir, StatisticExtJSONFile), jsonOutput, 0644); err != nil {
		return fmt.Errorf("failed to write statistic_ext.json: %w", err)
	}

	// Write SQL
	sqlOutput, err := generateImportExtSQL(d.config.YBMode, pgStatExtData)
	if err != nil {
		return fmt.Errorf("failed to generate import ext sql: %w", err)
	}
	if err := os.WriteFile(filepath.Join(d.config.OutputDir, ImportStatisticExtSQLFile), []byte(sqlOutput), 0644); err != nil {
		return fmt.Errorf("failed to write import_statistics_ext.sql: %w", err)
	}

	return nil
}

func generateImportExtSQL(ybMode bool, pgStatExtData []PgStatisticExtData) (string, error) {
	var sb strings.Builder
	if ybMode {
		sb.WriteString("SET yb_non_ddl_txn_for_sys_tables_allowed = ON;\n\n")
	}

	for _, data := range pgStatExtData {
		stxdndistinct := "NULL"
		if data.Stxdndistinct != nil {
			stxdndistinct = fmt.Sprintf("'%s'::bytea", data.Stxdndistinct)
		}
		stxddependencies := "NULL"
		if data.Stxddependencies != nil {
			stxddependencies = fmt.Sprintf("'%s'::bytea", data.Stxddependencies)
		}
		stxdmcv := "NULL"
		if data.Stxdmcv != nil {
			stxdmcv = fmt.Sprintf("'%s'::bytea", data.Stxdmcv)
		}

		stxdexpr := "NULL"
		if data.Stxdexpr != nil {
			// Construct ARRAY[ ... ]::pg_statistic[]
			// Each element is ( ... )
			// This is complex nested structure.
			// Python logic:
			// value_str = "ARRAY["
			// for statistic in stxdexpr: (list of dicts)
			//    value_str += '('
			//    for key, value in statistic.items():
			//       ...

			// We need to know the order of keys in the struct equivalent of pg_statistic type?
			// Python iterates dict items. If dict is unordered, this is risky, but works in py3.7+.
			// Here we have `[]interface{}` where each item is likely a `map[string]interface{}`.

			var arrayElements []string
			for _, item := range data.Stxdexpr {
				statMap, ok := item.(map[string]interface{})
				if !ok {
					continue
				}
				// We need to reconstruct the row for pg_statistic type.
				// This is basically re-using the logic from `getPgStatisticInsertQuery` but for a composite type literal.
				// However, `stxdexpr` column is `pg_statistic[]`.
				// The literal syntax for composite type is `(val1, val2, ...)`
				// We need the fields in order.

				// Order of fields in `pg_statistic` type?
				// starelid, staattnum, stainherit, stanullfrac, stawidth, stadistinct, stakind1..5, staop1..5, stacoll1..5(pg15), stanumbers1..5, stavalues1..5

				// Wait, the python script iterates over keys in the map and appends them.
				// If the map from JSON only contains non-null values, we are in trouble if we don't know the order.
				// But `row_to_json` includes all columns even if null? Yes.

				// So we need to iterate keys in a specific order that matches `pg_statistic` definition.
				// Let's assume standard order.

				keys := []string{
					"starelid", "staattnum", "stainherit", "stanullfrac", "stawidth", "stadistinct",
					"stakind1", "stakind2", "stakind3", "stakind4", "stakind5",
					"staop1", "staop2", "staop3", "staop4", "staop5",
				}
				// Check if stacoll exists in the map to decide if we include them (PG15 detection from data)
				if _, ok := statMap["stacoll1"]; ok {
					keys = append(keys, "stacoll1", "stacoll2", "stacoll3", "stacoll4", "stacoll5")
				}

				keys = append(keys,
					"stanumbers1", "stanumbers2", "stanumbers3", "stanumbers4", "stanumbers5",
					"stavalues1", "stavalues2", "stavalues3", "stavalues4", "stavalues5",
				)

				var rowValues []string
				for _, k := range keys {
					val, exists := statMap[k]
					if !exists {
						// Should not happen if row_to_json is full
						rowValues = append(rowValues, "NULL")
						continue
					}

					if val == nil {
						rowValues = append(rowValues, "NULL")
					} else {
						// Formatting logic
						if strings.HasPrefix(k, "stanumbers") {
							// List of numbers -> ARRAY[...]::real[]
							nums, ok := val.([]interface{})
							if !ok {
								rowValues = append(rowValues, "NULL")
							} else {
								var nStrs []string
								for _, n := range nums {
									nStrs = append(nStrs, fmt.Sprintf("%v", n))
								}
								rowValues = append(rowValues, fmt.Sprintf("ARRAY[%s]::real[]", strings.Join(nStrs, ",")))
							}
						} else if strings.HasPrefix(k, "stavalues") {
							// List -> array_in(...)
							vals, ok := val.([]interface{})
							if !ok {
								rowValues = append(rowValues, "NULL")
							} else {
								var vStrs []string
								for _, v := range vals {
									s := fmt.Sprintf("%v", v)
									// Escape
									s = strings.ReplaceAll(s, "\\", "\\\\")
									s = strings.ReplaceAll(s, "\"", "\\\"")
									s = strings.ReplaceAll(s, "'", "''")
									vStrs = append(vStrs, fmt.Sprintf("\"%s\"", s))
								}
								rowValues = append(rowValues, fmt.Sprintf("array_in('{%s}', 'pg_catalog.int4'::regtype, -1)::anyarray", strings.Join(vStrs, ",")))
								// Python script hardcodes pg_catalog.int4 here?
								// Yes, it seems so. Why int4? Maybe assumption about expression stats?
							}
						} else {
							rowValues = append(rowValues, fmt.Sprintf("'%v'", val))
						}
					}
				}
				arrayElements = append(arrayElements, fmt.Sprintf("(%s)", strings.Join(rowValues, ", ")))
			}
			stxdexpr = fmt.Sprintf("ARRAY[%s]::pg_statistic[]", strings.Join(arrayElements, ", "))
		}

		sb.WriteString(fmt.Sprintf(
			"DELETE FROM pg_statistic_ext_data WHERE stxoid = (SELECT oid FROM pg_statistic_ext WHERE stxname='%s');\n", data.Stxname))
		sb.WriteString(fmt.Sprintf(
			"INSERT INTO pg_statistic_ext_data VALUES ((SELECT oid FROM pg_statistic_ext WHERE stxname='%s'), %t, %s, %s, %s, %s);\n",
			data.Stxname, data.Stxdinherit, stxdndistinct, stxddependencies, stxdmcv, stxdexpr))
	}

	if ybMode {
		sb.WriteString("\nupdate pg_yb_catalog_version set current_version=current_version+1 where db_oid=1;\n")
		sb.WriteString("SET yb_non_ddl_txn_for_sys_tables_allowed = OFF;\n")
	}

	return sb.String(), nil
}
