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
	StatisticsJSONFile      = "statistics.json"
	ImportStatisticsSQLFile = "import_statistics.sql"
)

// RawJSON is used to preserve the original JSON from PostgreSQL without re-formatting
type RawJSON json.RawMessage

func (r RawJSON) MarshalJSON() ([]byte, error) {
	return []byte(r), nil
}

func (r *RawJSON) UnmarshalJSON(data []byte) error {
	*r = append((*r)[0:0], data...)
	return nil
}

// StatisticsDumpRaw uses RawJSON to preserve original formatting
type StatisticsDumpRaw struct {
	Version     string    `json:"version"`
	PgClass     []RawJSON `json:"pg_class"`
	PgStatistic []RawJSON `json:"pg_statistic"`
}

type PgClassStats struct {
	Relname       string  `json:"relname"`
	Relpages      int32   `json:"relpages"`
	Reltuples     float32 `json:"reltuples"`
	Relallvisible int32   `json:"relallvisible"`
	Nspname       string  `json:"nspname"`
}

type PgStatisticStats struct {
	Nspname     string      `json:"nspname"`
	Relname     string      `json:"relname"`
	Attname     string      `json:"attname"`
	Typnspname  string      `json:"typnspname"`
	Typname     string      `json:"typname"`
	Stainherit  bool        `json:"stainherit"`
	Stanullfrac float32     `json:"stanullfrac"`
	Stawidth    int32       `json:"stawidth"`
	Stadistinct float32     `json:"stadistinct"`
	Stakind1    int16       `json:"stakind1"`
	Stakind2    int16       `json:"stakind2"`
	Stakind3    int16       `json:"stakind3"`
	Stakind4    int16       `json:"stakind4"`
	Stakind5    int16       `json:"stakind5"`
	Staop1      interface{} `json:"staop1"`
	Staop2      interface{} `json:"staop2"`
	Staop3      interface{} `json:"staop3"`
	Staop4      interface{} `json:"staop4"`
	Staop5      interface{} `json:"staop5"`
	Stanumbers1 []float32   `json:"stanumbers1"`
	Stanumbers2 []float32   `json:"stanumbers2"`
	Stanumbers3 []float32   `json:"stanumbers3"`
	Stanumbers4 []float32   `json:"stanumbers4"`
	Stanumbers5 []float32   `json:"stanumbers5"`
	// PG15+ - stacoll comes after stanumbers in Python
	Stacoll1   interface{} `json:"stacoll1,omitempty"`
	Stacoll2   interface{} `json:"stacoll2,omitempty"`
	Stacoll3   interface{} `json:"stacoll3,omitempty"`
	Stacoll4   interface{} `json:"stacoll4,omitempty"`
	Stacoll5   interface{} `json:"stacoll5,omitempty"`
	Stavalues1 interface{} `json:"stavalues1"`
	Stavalues2 interface{} `json:"stavalues2"`
	Stavalues3 interface{} `json:"stavalues3"`
	Stavalues4 interface{} `json:"stavalues4"`
	Stavalues5 interface{} `json:"stavalues5"`
}

func (d *Dumper) ExportStatistics(relationNames []string) error {
	var versionStr string
	err := d.conn.QueryRow(context.Background(), "SHOW server_version_num").Scan(&versionStr)
	if err != nil {
		return fmt.Errorf("failed to get server version: %w", err)
	}
	var pgMajorVersion int
	fmt.Sscanf(versionStr, "%d", &pgMajorVersion)
	pgMajorVersion = pgMajorVersion / 10000

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

	// 1. Fetch pg_class stats - preserve raw JSON
	queryClass := fmt.Sprintf(`
		SELECT row_to_json(t) FROM
            (SELECT c.relname, c.relpages, c.reltuples, c.relallvisible, n.nspname
                FROM pg_class c JOIN pg_namespace n on c.relnamespace = n.oid %s %s) t
	`, schemasFilter, relationNamesFilter)

	rowsClass, err := d.conn.Query(context.Background(), queryClass)
	if err != nil {
		return fmt.Errorf("failed to query pg_class: %w", err)
	}
	defer rowsClass.Close()

	var pgClassRaw []RawJSON
	var pgClassStats []PgClassStats
	for rowsClass.Next() {
		var jsonBytes []byte
		if err := rowsClass.Scan(&jsonBytes); err != nil {
			return fmt.Errorf("failed to scan pg_class json: %w", err)
		}
		pgClassRaw = append(pgClassRaw, RawJSON(jsonBytes))

		var stat PgClassStats
		if err := json.Unmarshal(jsonBytes, &stat); err != nil {
			return fmt.Errorf("failed to unmarshal pg_class json: %w", err)
		}
		pgClassStats = append(pgClassStats, stat)
	}

	// 2. Fetch pg_statistic stats
	// Note: Python has stanumbers before stacoll for PG15+
	var queryStat string
	if pgMajorVersion < 15 {
		queryStat = fmt.Sprintf(`
            SELECT row_to_json(t) FROM
                (SELECT
                    n.nspname nspname,
                    c.relname relname,
                    a.attname attname,
                    (select nspname from pg_namespace where oid = t.typnamespace) typnspname,
                    t.typname typname,
                    s.stainherit,
                    s.stanullfrac,
                    s.stawidth,
                    s.stadistinct,
                    s.stakind1,
                    s.stakind2,
                    s.stakind3,
                    s.stakind4,
                    s.stakind5,
                    s.staop1,
                    s.staop2,
                    s.staop3,
                    s.staop4,
                    s.staop5,
                    s.stanumbers1,
                    s.stanumbers2,
                    s.stanumbers3,
                    s.stanumbers4,
                    s.stanumbers5,
                    s.stavalues1,
                    s.stavalues2,
                    s.stavalues3,
                    s.stavalues4,
                    s.stavalues5
                    FROM pg_class c
                        JOIN pg_namespace n on c.relnamespace = n.oid %s %s
                        JOIN pg_statistic s ON s.starelid = c.oid
                        JOIN pg_attribute a ON c.oid = a.attrelid AND s.staattnum = a.attnum
                        JOIN pg_type t ON a.atttypid = t.oid) t
            `, schemasFilter, relationNamesFilter)
	} else {
		// PG15+: stanumbers before stacoll (matching Python)
		queryStat = fmt.Sprintf(`
            SELECT row_to_json(t) FROM
                (SELECT
                    n.nspname nspname,
                    c.relname relname,
                    a.attname attname,
                    (select nspname from pg_namespace where oid = t.typnamespace) typnspname,
                    t.typname typname,
                    s.stainherit,
                    s.stanullfrac,
                    s.stawidth,
                    s.stadistinct,
                    s.stakind1,
                    s.stakind2,
                    s.stakind3,
                    s.stakind4,
                    s.stakind5,
                    s.staop1,
                    s.staop2,
                    s.staop3,
                    s.staop4,
                    s.staop5,
                    s.stanumbers1,
                    s.stanumbers2,
                    s.stanumbers3,
                    s.stanumbers4,
                    s.stanumbers5,
                    s.stacoll1,
                    s.stacoll2,
                    s.stacoll3,
                    s.stacoll4,
                    s.stacoll5,
                    s.stavalues1,
                    s.stavalues2,
                    s.stavalues3,
                    s.stavalues4,
                    s.stavalues5
                    FROM pg_class c
                        JOIN pg_namespace n on c.relnamespace = n.oid %s %s
                        JOIN pg_statistic s ON s.starelid = c.oid
                        JOIN pg_attribute a ON c.oid = a.attrelid AND s.staattnum = a.attnum
                        JOIN pg_type t ON a.atttypid = t.oid) t
            `, schemasFilter, relationNamesFilter)
	}

	rowsStat, err := d.conn.Query(context.Background(), queryStat)
	if err != nil {
		return fmt.Errorf("failed to query pg_statistic: %w", err)
	}
	defer rowsStat.Close()

	var pgStatisticRaw []RawJSON
	var pgStatisticStats []PgStatisticStats
	for rowsStat.Next() {
		var jsonBytes []byte
		if err := rowsStat.Scan(&jsonBytes); err != nil {
			return fmt.Errorf("failed to scan pg_statistic json: %w", err)
		}
		pgStatisticRaw = append(pgStatisticRaw, RawJSON(jsonBytes))

		var stat PgStatisticStats
		if err := json.Unmarshal(jsonBytes, &stat); err != nil {
			return fmt.Errorf("failed to unmarshal pg_statistic json: %w", err)
		}
		pgStatisticStats = append(pgStatisticStats, stat)
	}

	// Write JSON - using custom format to match Python output
	jsonOutput := formatStatisticsJSON("1.0.0", pgClassRaw, pgStatisticRaw)
	if err := os.WriteFile(filepath.Join(d.config.OutputDir, StatisticsJSONFile), []byte(jsonOutput), 0644); err != nil {
		return fmt.Errorf("failed to write statistics.json: %w", err)
	}

	// Write SQL
	sqlOutput, err := generateImportSQL(d.config.YBMode, pgMajorVersion, pgClassStats, pgStatisticStats)
	if err != nil {
		return fmt.Errorf("failed to generate import sql: %w", err)
	}
	if err := os.WriteFile(filepath.Join(d.config.OutputDir, ImportStatisticsSQLFile), []byte(sqlOutput), 0644); err != nil {
		return fmt.Errorf("failed to write import_statistics.sql: %w", err)
	}

	return nil
}

// formatStatisticsJSON formats the statistics JSON to match Python output exactly
// Python uses indent=4 but keeps each row on a single line
func formatStatisticsJSON(version string, pgClass []RawJSON, pgStatistic []RawJSON) string {
	var sb strings.Builder
	sb.WriteString("{\n")
	sb.WriteString(fmt.Sprintf("    \"version\": \"%s\",\n", version))

	// pg_class array
	sb.WriteString("    \"pg_class\": [\n")
	for i, row := range pgClass {
		sb.WriteString("        ")
		sb.Write(row)
		if i < len(pgClass)-1 {
			sb.WriteString(",")
		}
		sb.WriteString("\n")
	}
	sb.WriteString("    ],\n")

	// pg_statistic array
	sb.WriteString("    \"pg_statistic\": [\n")
	for i, row := range pgStatistic {
		sb.WriteString("        ")
		sb.Write(row)
		if i < len(pgStatistic)-1 {
			sb.WriteString(",")
		}
		sb.WriteString("\n")
	}
	sb.WriteString("    ]\n")

	sb.WriteString("}")
	return sb.String()
}

func generateImportSQL(ybMode bool, pgVersion int, pgClass []PgClassStats, pgStat []PgStatisticStats) (string, error) {
	var sb strings.Builder

	if ybMode {
		sb.WriteString("SET yb_non_ddl_txn_for_sys_tables_allowed = ON;\n\n")
	}

	for _, cls := range pgClass {
		// Match Python format exactly
		sb.WriteString(fmt.Sprintf(
			"UPDATE pg_class SET reltuples = %v, relpages = %d, relallvisible = %d WHERE relnamespace = '%s'::regnamespace AND (relname = '%s' OR relname = '%s_pkey');\n",
			cls.Reltuples, cls.Relpages, cls.Relallvisible, cls.Nspname, cls.Relname, cls.Relname))
	}

	for _, stat := range pgStat {
		sql, err := getPgStatisticInsertQuery(pgVersion, stat)
		if err != nil {
			return "", err
		}
		sb.WriteString(sql + "\n")
	}

	if ybMode {
		sb.WriteString("\nupdate pg_yb_catalog_version set current_version=current_version+1 where db_oid=1;\n")
		sb.WriteString("SET yb_non_ddl_txn_for_sys_tables_allowed = OFF;\n")
	}

	return sb.String(), nil
}

func getPgStatisticInsertQuery(pgMajorVersion int, stat PgStatisticStats) (string, error) {
	columnTypes := map[string]string{
		"stainherit":  "boolean",
		"stanullfrac": "real",
		"stawidth":    "integer",
		"stadistinct": "real",
		"stakind1":    "smallint",
		"stakind2":    "smallint",
		"stakind3":    "smallint",
		"stakind4":    "smallint",
		"stakind5":    "smallint",
		"staop1":      "oid",
		"staop2":      "oid",
		"staop3":      "oid",
		"staop4":      "oid",
		"staop5":      "oid",
		"stanumbers1": "real[]",
		"stanumbers2": "real[]",
		"stanumbers3": "real[]",
		"stanumbers4": "real[]",
		"stanumbers5": "real[]",
	}

	if pgMajorVersion >= 15 {
		columnTypes["stacoll1"] = "oid"
		columnTypes["stacoll2"] = "oid"
		columnTypes["stacoll3"] = "oid"
		columnTypes["stacoll4"] = "oid"
		columnTypes["stacoll5"] = "oid"
	}

	stavaluesType := stat.Typnspname + "." + stat.Typname
	if stat.Typnspname == "" {
		stavaluesType = "pg_catalog." + stat.Typname
	}
	for i := 1; i <= 5; i++ {
		columnTypes[fmt.Sprintf("stavalues%d", i)] = stavaluesType
	}

	var columnValues []string

	orderedCols := []string{
		"stainherit", "stanullfrac", "stawidth", "stadistinct",
		"stakind1", "stakind2", "stakind3", "stakind4", "stakind5",
		"staop1", "staop2", "staop3", "staop4", "staop5",
	}
	if pgMajorVersion >= 15 {
		orderedCols = append(orderedCols, "stacoll1", "stacoll2", "stacoll3", "stacoll4", "stacoll5")
	}
	orderedCols = append(orderedCols,
		"stanumbers1", "stanumbers2", "stanumbers3", "stanumbers4", "stanumbers5",
		"stavalues1", "stavalues2", "stavalues3", "stavalues4", "stavalues5")

	for _, col := range orderedCols {
		typ := columnTypes[col]
		var valStr string

		switch col {
		case "stainherit":
			valStr = fmt.Sprintf("%t::%s", stat.Stainherit, typ)
		case "stanullfrac":
			valStr = fmt.Sprintf("%v::%s", stat.Stanullfrac, typ)
		case "stawidth":
			valStr = fmt.Sprintf("%d::%s", stat.Stawidth, typ)
		case "stadistinct":
			valStr = fmt.Sprintf("%v::%s", stat.Stadistinct, typ)
		case "stakind1":
			valStr = fmt.Sprintf("%d::%s", stat.Stakind1, typ)
		case "stakind2":
			valStr = fmt.Sprintf("%d::%s", stat.Stakind2, typ)
		case "stakind3":
			valStr = fmt.Sprintf("%d::%s", stat.Stakind3, typ)
		case "stakind4":
			valStr = fmt.Sprintf("%d::%s", stat.Stakind4, typ)
		case "stakind5":
			valStr = fmt.Sprintf("%d::%s", stat.Stakind5, typ)
		case "staop1":
			valStr = fmt.Sprintf("%v::%s", stat.Staop1, typ)
		case "staop2":
			valStr = fmt.Sprintf("%v::%s", stat.Staop2, typ)
		case "staop3":
			valStr = fmt.Sprintf("%v::%s", stat.Staop3, typ)
		case "staop4":
			valStr = fmt.Sprintf("%v::%s", stat.Staop4, typ)
		case "staop5":
			valStr = fmt.Sprintf("%v::%s", stat.Staop5, typ)
		case "stacoll1":
			valStr = fmt.Sprintf("%v::%s", stat.Stacoll1, typ)
		case "stacoll2":
			valStr = fmt.Sprintf("%v::%s", stat.Stacoll2, typ)
		case "stacoll3":
			valStr = fmt.Sprintf("%v::%s", stat.Stacoll3, typ)
		case "stacoll4":
			valStr = fmt.Sprintf("%v::%s", stat.Stacoll4, typ)
		case "stacoll5":
			valStr = fmt.Sprintf("%v::%s", stat.Stacoll5, typ)
		case "stanumbers1":
			valStr = formatFloatArray(stat.Stanumbers1, typ)
		case "stanumbers2":
			valStr = formatFloatArray(stat.Stanumbers2, typ)
		case "stanumbers3":
			valStr = formatFloatArray(stat.Stanumbers3, typ)
		case "stanumbers4":
			valStr = formatFloatArray(stat.Stanumbers4, typ)
		case "stanumbers5":
			valStr = formatFloatArray(stat.Stanumbers5, typ)
		case "stavalues1":
			valStr = formatValuesArray(stat.Stavalues1, typ)
		case "stavalues2":
			valStr = formatValuesArray(stat.Stavalues2, typ)
		case "stavalues3":
			valStr = formatValuesArray(stat.Stavalues3, typ)
		case "stavalues4":
			valStr = formatValuesArray(stat.Stavalues4, typ)
		case "stavalues5":
			valStr = formatValuesArray(stat.Stavalues5, typ)
		}
		columnValues = append(columnValues, valStr)
	}

	starelid := fmt.Sprintf("'%s.%s'::regclass", stat.Nspname, stat.Relname)
	staattnumSubquery := fmt.Sprintf("(SELECT a.attnum FROM pg_attribute a WHERE a.attrelid = %s and a.attname = '%s')", starelid, stat.Attname)

	vals := strings.Join(columnValues, ", ")

	query := fmt.Sprintf("DELETE FROM pg_statistic WHERE starelid = %s AND staattnum = %s;\nINSERT INTO pg_statistic VALUES (%s, %s, %s);", starelid, staattnumSubquery, starelid, staattnumSubquery, vals)

	return query, nil
}

func formatFloatArray(nums []float32, typ string) string {
	if nums == nil {
		return "NULL::" + typ
	}
	var strs []string
	for _, n := range nums {
		strs = append(strs, fmt.Sprintf("%v", n))
	}
	return fmt.Sprintf("'{%s}'::%s", strings.Join(strs, ","), typ)
}

func formatValuesArray(val interface{}, typ string) string {
	if val == nil {
		return "NULL::" + typ
	}

	listVal, ok := val.([]interface{})
	if !ok {
		return "NULL::" + typ
	}

	var elements []string
	for _, e := range listVal {
		strVal := fmt.Sprintf("%v", e)
		strVal = strings.ReplaceAll(strVal, "\\\\", "\\\\\\")
		strVal = strings.ReplaceAll(strVal, "\"", "\\\"")
		strVal = strings.ReplaceAll(strVal, "'", "''")
		elements = append(elements, fmt.Sprintf("\"%s\"", strVal))
	}
	sqlArray := strings.Join(elements, ", ")
	return fmt.Sprintf("array_in('{%s}', '%s'::regtype, -1)::anyarray", sqlArray, typ)
}
