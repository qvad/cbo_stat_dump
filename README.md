# CBO Statistics Dump Tool

This tool is used to export and import PostgreSQL/YugabyteDB statistics to reproduce query optimizer behavior. It has been ported from Python to Go.

## Build

To build the tool and the test runner locally:

```bash
go build -o cbo_stat_dump_bin ./cmd/cbo_stat_dump
go build -o test_benchmark_bin ./cmd/test_benchmark
```

## Usage

### cbo_stat_dump

```bash
./cbo_stat_dump_bin -h <host> -p <port> -d <database> -u <user> -o <output_dir> [-q <query_file>] [-yb_mode]
```

### Test Runner

```bash
./test_benchmark_bin -b <benchmark_name> -yb_mode
```

## Running Tests with Docker

To run the self-test suite (which creates a DB, populates data, dumps stats, and verifies plans), use Docker Compose:

```bash
docker-compose -f docker-compose.test.yml up --build --abort-on-container-exit --exit-code-from tester
```

This will:
1. Start an optimized YugabyteDB instance.
2. Build the test container.
3. Run the self-test.
4. Exit with 0 if successful, non-zero otherwise.

## Project Structure

- `cmd/cbo_stat_dump`: Main application code.
- `cmd/test_benchmark`: Test runner code.
- `internal/`: Core logic.
- `test/`: Benchmark SQL files.
- `docker-compose.test.yml`: Integration test definition.
- `Dockerfile.test`: Test container definition.
