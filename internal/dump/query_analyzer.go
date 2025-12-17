package dump

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

type ExplainOutput []struct {
	Plan Plan `json:"Plan"`
}

type Plan struct {
	NodeType     string `json:"Node Type"`
	RelationName string `json:"Relation Name,omitempty"`
	Schema       string `json:"Schema,omitempty"`
	Alias        string `json:"Alias,omitempty"`
	Plans        []Plan `json:"Plans,omitempty"`
}

func (d *Dumper) GetRelationNamesInQuery(queryPath string) ([]string, error) {
	queryBytes, err := os.ReadFile(queryPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read query file: %w", err)
	}
	query := string(queryBytes)

	// Set GUCs if needed
	if d.config.EnableBaseScansCostModel && d.config.YBMode {
		_, err := d.conn.Exec(context.Background(), "SET yb_enable_base_scans_cost_model=ON")
		if err != nil {
			return nil, fmt.Errorf("failed to set yb_enable_base_scans_cost_model: %w", err)
		}
	}

	explainQuery := "EXPLAIN (FORMAT JSON) " + query

	// pgx requires a bit of work to scan a JSON result into a byte slice if it's returned as a single column
	// Actually, EXPLAIN returns a result set where the first column is the JSON text.
	// However, it might be split across rows? usually EXPLAIN (FORMAT JSON) returns a single row with single column for simple queries, but let's be safe.
	// The python script fetches all.

	rows, err := d.conn.Query(context.Background(), explainQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to execute explain: %w", err)
	}
	defer rows.Close()

	var sb strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return nil, fmt.Errorf("failed to scan explain output: %w", err)
		}
		sb.WriteString(line)
	}

	var explainOutput ExplainOutput
	if err := json.Unmarshal([]byte(sb.String()), &explainOutput); err != nil {
		return nil, fmt.Errorf("failed to unmarshal explain json: %w", err)
	}

	relations := make(map[string]bool)
	if len(explainOutput) > 0 {
		extractRelations(explainOutput[0].Plan, relations)
	}

	var relationNames []string
	for r := range relations {
		relationNames = append(relationNames, r)
	}
	return relationNames, nil
}

func extractRelations(plan Plan, relations map[string]bool) {
	if plan.RelationName != "" {
		// Python script does specific checks. Let's see.
		// It appends Schema.RelationName if Schema is present, else just RelationName
		name := plan.RelationName
		if plan.Schema != "" {
			name = plan.Schema + "." + plan.RelationName
		}
		relations[name] = true
	}
	for _, subPlan := range plan.Plans {
		extractRelations(subPlan, relations)
	}
}
