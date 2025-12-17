package main

import (
	"database/sql"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // Use pgx as stdlib driver
	"github.com/sergi/go-diff/diffmatchpatch"
)

// Global config
var (
	benchmark       string
	ybMode          bool
	benchmarkPath   string
	createProdDB    bool
	prodHost        string
	prodPort        int
	prodUser        string
	prodPassword    string
	prodDatabase    string
	testHost        string
	testPort        int
	testUser        string
	testPassword    string
	ignoreRanTests  bool
	enableBaseScans bool
	colocation      bool
	outDir          string
	debug           bool
)

func main() {
	flag.StringVar(&benchmark, "b", "", "Name of the benchmark (required)")
	flag.BoolVar(&ybMode, "yb_mode", false, "Use YugabyteDB mode")
	flag.StringVar(&benchmarkPath, "benchmark_path", "", "Path to benchmark files")
	flag.BoolVar(&createProdDB, "create_prod_db", false, "Create production database")
	flag.StringVar(&prodHost, "prod_host", "localhost", "Production host")
	flag.IntVar(&prodPort, "prod_port", 5432, "Production port")
	flag.StringVar(&prodUser, "prod_user", "", "Production user")
	flag.StringVar(&prodPassword, "prod_password", "", "Production password")
	flag.StringVar(&prodDatabase, "prod_database", "", "Production database")
	flag.StringVar(&testHost, "test_host", "", "Test host")
	flag.IntVar(&testPort, "test_port", 0, "Test port")
	flag.StringVar(&testUser, "test_user", "", "Test user")
	flag.StringVar(&testPassword, "test_password", "", "Test password")
	flag.BoolVar(&ignoreRanTests, "ignore_ran_tests", false, "Ignore ran tests")
	flag.BoolVar(&enableBaseScans, "enable_base_scans_cost_model", false, "Enable base scans cost model")
	flag.BoolVar(&colocation, "colocation", false, "Enable colocation")
	flag.StringVar(&outDir, "outdir", "", "Output directory")
	flag.BoolVar(&debug, "d", false, "Debug mode")

	flag.Parse()

	if benchmark == "" {
		fmt.Println("Benchmark name is required (-b)")
		os.Exit(1)
	}

	// Defaults
	if benchmarkPath == "" {
		benchmarkPath = filepath.Join("test", benchmark)
	}
	if ybMode {
		if prodUser == "" {
			prodUser = "yugabyte"
		}
		if prodPort == 5432 && !isFlagPassed("prod_port") {
			prodPort = 5433
		}
	} else {
		if prodUser == "" {
			prodUser = "postgres" // Default changed from python script's 'gaurav'
		}
	}

	if testHost == "" {
		testHost = prodHost
	}
	if testPort == 0 {
		testPort = prodPort
	}
	if testUser == "" {
		testUser = prodUser
	}
	if testPassword == "" {
		testPassword = prodPassword
	}
	if prodDatabase == "" {
		prodDatabase = benchmark + "_db"
	}
	if outDir == "" {
		outDir = filepath.Join("test_out_dir", benchmark)
	}

	if colocation && !ybMode {
		fmt.Println("Colocation only supported in YB mode")
		os.Exit(1)
	}

	run()
}

func isFlagPassed(name string) bool {
	found := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}

func run() {
	if createProdDB {
		createProductionDatabase()
	}

	queriesPath := filepath.Join(benchmarkPath, "queries")
	files, err := os.ReadDir(queriesPath)
	if err != nil {
		fmt.Printf("Failed to read queries directory: %v\n", err)
		os.Exit(1)
	}

	failedQueries := []string{}

	for _, f := range files {
		if f.IsDir() {
			continue
		}
		queryName := strings.TrimSuffix(f.Name(), filepath.Ext(f.Name()))
		queryFileAbs := filepath.Join(queriesPath, f.Name())
		queryOutDir := filepath.Join(outDir, queryName)

		if ignoreRanTests {
			if _, err := os.Stat(queryOutDir); err == nil {
				if debug {
					fmt.Printf("Skipping %s\n", queryName)
				}
				continue
			}
		}

		fmt.Printf("Testing %s\n", queryName)

		// Run cbo_stat_dump
		runCBOStatDump(queryOutDir, queryFileAbs)

		// Create Test DB
		testDBName := fmt.Sprintf("%s_%s_test_db", benchmark, queryName)
		dropDatabase(testHost, testPort, testUser, testPassword, testDBName)
		createDatabase(testHost, testPort, testUser, testPassword, testDBName, colocation)

		// Restore stats
		runSQLOnTestDB(testDBName, filepath.Join(queryOutDir, "ddl.sql"))
		runSQLOnTestDB(testDBName, filepath.Join(queryOutDir, "import_statistics.sql"))

		if ybMode {
			// Extended stats? Python script imports it if exists?
			// Python script: run_sql_on_test_database(..., import_statistics.sql)
			// It assumes everything needed is there.
			// Wait, `export_extended_statistics` writes to `import_statistic_ext.sql`.
			// Does python script run `import_statistic_ext.sql`?
			// Looking at python script `test/test_benchmark.py`:
			// L270: run_sql_on_test_database(args, test_db_name, query_outdir + '/import_statistics.sql')
			// It does NOT seem to run `import_statistics_ext.sql`!
			// That might be a bug in the python test runner or intent.
			// I'll stick to python logic for now.

			// Wait, checking python script again.
			// L260-275.
			// It only runs `import_statistics.sql`.
		}

		time.Sleep(100 * time.Millisecond)

		// Explain and Compare
		exportQueryPlan(testDBName, filepath.Join(queryOutDir, "overridden_gucs.sql"), queryFileAbs, queryOutDir)

		if !queryPlansMatch(queryOutDir) {
			failedQueries = append(failedQueries, fmt.Sprintf("%s : %s", f.Name(), filepath.Join(queryOutDir, "query_plan_diff.txt")))
		}

		dropDatabase(testHost, testPort, testUser, testPassword, testDBName)
	}

	if len(failedQueries) > 0 {
		fmt.Println("Following tests failed!")
		for _, f := range failedQueries {
			fmt.Println(f)
		}
		os.Exit(1)
	} else {
		fmt.Println("All tests passed!")
	}
}

func runCBOStatDump(outDir, queryFile string) {
	// We use our built binary
	bin := "./cbo_stat_dump_bin"
	if _, err := os.Stat(bin); os.IsNotExist(err) {
		// try looking in path or assume we are in root
		bin = "cbo_stat_dump_bin" // expect in PATH or current
	}

	args := []string{
		"-h", prodHost,
		"-p", fmt.Sprintf("%d", prodPort),
		"-d", prodDatabase,
		"-u", prodUser,
		"-o", outDir,
		"-q", queryFile,
	}
	if prodPassword != "" {
		args = append(args, "-W", prodPassword)
	}
	if enableBaseScans && ybMode {
		args = append(args, "-enable_base_scans_cost_model")
	}
	if ybMode {
		args = append(args, "-yb_mode")
	}

	cmd := exec.Command(bin, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		fmt.Printf("cbo_stat_dump failed: %v\n", err)
		os.Exit(1)
	}
}

func createProductionDatabase() {
	// Drop and Create
	dropDatabase(prodHost, prodPort, prodUser, prodPassword, prodDatabase)
	createDatabase(prodHost, prodPort, prodUser, prodPassword, prodDatabase, colocation)

	createSQL := filepath.Join(benchmarkPath, "create.sql")
	if ybMode {
		createSQL = filepath.Join(benchmarkPath, "create.yb.sql")
	}
	runSQL(prodHost, prodPort, prodUser, prodPassword, prodDatabase, createSQL)
}

func dropDatabase(host string, port int, user, password, dbName string) {
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%d/postgres?sslmode=disable", user, password, host, port)
	db, err := sql.Open("pgx", connStr)
	if err != nil {
		fmt.Printf("Failed to connect to drop db: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	_, err = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbName))
	if err != nil {
		fmt.Printf("Failed to drop database %s: %v\n", dbName, err)
		os.Exit(1)
	}
}

func createDatabase(host string, port int, user, password, dbName string, colocated bool) {
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%d/postgres?sslmode=disable", user, password, host, port)
	db, err := sql.Open("pgx", connStr)
	if err != nil {
		fmt.Printf("Failed to connect to create db: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	query := fmt.Sprintf("CREATE DATABASE %s", dbName)
	if colocated {
		query += " WITH colocation = true"
	}
	_, err = db.Exec(query)
	if err != nil {
		fmt.Printf("Failed to create database %s: %v\n", dbName, err)
		os.Exit(1)
	}
}

func runSQLOnTestDB(dbName, sqlFile string) {
	runSQL(testHost, testPort, testUser, testPassword, dbName, sqlFile)
}

func runSQL(host string, port int, user, password, dbName, sqlFile string) {
	// Use psql or ysqlsh
	bin := "psql"
	if ybMode {
		bin = "ysqlsh"
	}

	// Check if bin exists, if not fallback to psql
	if _, err := exec.LookPath(bin); err != nil {
		bin = "psql"
	}

	cmd := exec.Command(bin, "-h", host, "-p", fmt.Sprintf("%d", port), "-U", user, "-d", dbName, "-f", sqlFile)
	cmd.Env = append(os.Environ(), "PGPASSWORD="+password)

	if output, err := cmd.CombinedOutput(); err != nil {
		fmt.Printf("Failed to run SQL %s: %v\nOutput: %s\n", sqlFile, err, string(output))
		os.Exit(1)
	}
}

func exportQueryPlan(dbName, gucsFile, queryFile, outDir string) {
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", testUser, testPassword, testHost, testPort, dbName)
	db, err := sql.Open("pgx", connStr)
	if err != nil {
		fmt.Printf("Failed to connect for explain: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	// Use a transaction to ensure we use the same connection for SET and EXPLAIN
	tx, err := db.Begin()
	if err != nil {
		fmt.Printf("Failed to begin transaction: %v\n", err)
		os.Exit(1)
	}
	defer tx.Rollback()

	if _, err := os.Stat(gucsFile); err == nil {
		gucsBytes, _ := os.ReadFile(gucsFile)
		gucs := string(gucsBytes)
		// Split by semicolon or newline. Since we know format is "SET ...;\n", splitting by \n is fine.
		// But simply Exec-ing the whole block might fail if driver doesn't support multiple statements.
		// Let's try splitting by semicolon as a safer bet for multiple statements,
		// or just iterate lines if we trust the format.
		// Given I fixed the generator to be line-based, let's split by newline.
		lines := strings.Split(gucs, "\n")
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			if _, err := tx.Exec(line); err != nil {
				// Warning only? Some GUCs might fail if not supported in target DB version?
				// But for self-test (same DB) it should pass.
				fmt.Printf("Warning: Failed to set GUC: %s. Error: %v\n", line, err)
			}
		}
	}

	if enableBaseScans && ybMode {
		tx.Exec("SET yb_enable_base_scans_cost_model=ON")
	}
	if !ybMode {
		tx.Exec("SET enable_cbo_statistics_simulation=ON")
	}

	queryBytes, _ := os.ReadFile(queryFile)
	query := "EXPLAIN " + string(queryBytes)

	rows, err := tx.Query(query)
	if err != nil {
		fmt.Printf("Failed to explain: %v\nQuery: %s\n", err, query)
		os.Exit(1)
	}
	defer rows.Close()

	outFile := filepath.Join(outDir, "sim_query_plan.txt")
	f, _ := os.Create(outFile)
	defer f.Close()

	for rows.Next() {
		var line string
		rows.Scan(&line)
		f.WriteString(line + "\n")
	}
}

func queryPlansMatch(outDir string) bool {
	prodPlan, _ := os.ReadFile(filepath.Join(outDir, "query_plan.txt"))
	simPlan, _ := os.ReadFile(filepath.Join(outDir, "sim_query_plan.txt"))

	dmp := diffmatchpatch.New()
	diffs := dmp.DiffMain(string(prodPlan), string(simPlan), false)

	if len(diffs) == 1 && diffs[0].Type == diffmatchpatch.DiffEqual {
		return true
	}

	// Generate diff file
	diffText := dmp.DiffPrettyText(diffs)
	os.WriteFile(filepath.Join(outDir, "query_plan_diff.txt"), []byte(diffText), 0644)

	return false
}
