# Tests

This directory contains benchmark data and queries used for integration testing.

## Structure

Each benchmark directory (e.g., `join_order_benchmark`, `tpcds`, `tpch`, `self_test`) should contain:

- `create.sql`: SQL to create the schema and populate data (for the "production" database).
- `queries/`: Directory containing `.sql` files with queries to test.

## Running Tests

Tests are run using the `test_benchmark_bin` binary (built from `cmd/test_benchmark`).

```bash
../test_benchmark_bin -b <benchmark_name> -yb_mode ...
```

See the main `README.md` for full usage instructions.