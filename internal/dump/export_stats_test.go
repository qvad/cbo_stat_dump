package dump

import (
	"strings"
	"testing"
)

func TestGetPgStatisticInsertQuery(t *testing.T) {
	stat := PgStatisticStats{
		Nspname:     "public",
		Relname:     "users",
		Attname:     "id",
		Typnspname:  "pg_catalog",
		Typname:     "int4",
		Stainherit:  false,
		Stanullfrac: 0.0,
		Stawidth:    4,
		Stadistinct: -1.0,
		Stakind1:    1,
		Staop1:      96,
		Stanumbers1: []float32{0.5, 0.5},
		Stavalues1:  []interface{}{10, 20},
	}

	query, err := getPgStatisticInsertQuery(14, stat)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(query, "INSERT INTO pg_statistic VALUES") {
		t.Errorf("expected INSERT statement, got: %s", query)
	}
	if !strings.Contains(query, "'public.users'::regclass") {
		t.Errorf("expected correct relname, got: %s", query)
	}
	// Check array format
	// stanumbers1: '{0.500000,0.500000}'::real[]
	if !strings.Contains(query, "'{0.500000,0.500000}'::real[]") {
		t.Errorf("expected correct stanumbers1, got: %s", query)
	}
	// stavalues1: array_in('{"10", "20"}', 'pg_catalog.int4'::regtype, -1)::anyarray
	// Note: my implementation puts quotes around numbers in stavalues array string: "10", "20"
	if !strings.Contains(query, "array_in('{\"10\", \"20\"}', 'pg_catalog.int4'::regtype, -1)::anyarray") {
		t.Errorf("expected correct stavalues1, got: %s", query)
	}
}

func TestGenerateImportSQL(t *testing.T) {
	pgClass := []PgClassStats{
		{
			Relname:       "users",
			Relpages:      10,
			Reltuples:     1000,
			Relallvisible: 0,
			Nspname:       "public",
		},
	}
	pgStat := []PgStatisticStats{}

	sql, err := generateImportSQL(true, 14, pgClass, pgStat)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(sql, "UPDATE pg_class SET reltuples = 1000.000000") {
		t.Errorf("expected UPDATE pg_class, got: %s", sql)
	}
	if !strings.Contains(sql, "SET yb_non_ddl_txn_for_sys_tables_allowed = ON") {
		t.Errorf("expected YB specific GUC, got: %s", sql)
	}
}
