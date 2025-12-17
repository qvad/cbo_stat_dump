package dump

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
)

func (d *Dumper) ExportDDL(relationNames []string) error {
	// We assume pg_dump is in PATH.
	// If YBMode is true, we might need yb_pg_dump equivalent?
	// The python script uses 'pg_dump' or 'ysql_dump'?
	// Let's check python script again.
	// L54: DDL_DUMP_BIN = 'pg_dump'
	// L149: ddl_dump_cmd = password_prefix_str + (f"{DDL_DUMP_BIN} {connection_params} -s")

	// It seems it just uses `pg_dump`.

	pgDumpBin := "pg_dump" // Or customizable?

	args := []string{
		"-h", d.config.Host,
		"-p", fmt.Sprintf("%d", d.config.Port),
		"-d", d.config.Database,
		"-U", d.config.User,
		"-s", // Schema only
	}

	if len(relationNames) > 0 {
		for _, rel := range relationNames {
			args = append(args, "-t", rel)
		}
	}

	// Password handling
	cmd := exec.Command(pgDumpBin, args...)
	cmd.Env = append(os.Environ(), "PGPASSWORD="+d.config.Password)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("pg_dump failed: %s, output: %s", err, string(output))
	}

	// Filter output similar to python script
	// Python script filters:
	// (?:^--)|(?:^SET)|(?:^SELECT pg_catalog)|(?:^ALTER .+ OWNER TO)|(?:^CREATE SCHEMA public;$)

	lines := strings.Split(string(output), "\n")
	var filteredOutput strings.Builder

	regex := regexp.MustCompile(`(?:^--)|(?:^SET)|(?:^SELECT pg_catalog)|(?:^ALTER .+ OWNER TO)|(?:^CREATE SCHEMA public;$)`)

	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		if !regex.MatchString(line) {
			filteredOutput.WriteString(line + "\n")
		}
	}

	outputPath := filepath.Join(d.config.OutputDir, "ddl.sql")
	if err := os.WriteFile(outputPath, []byte(filteredOutput.String()), 0644); err != nil {
		return fmt.Errorf("failed to write ddl.sql: %w", err)
	}

	return nil
}
