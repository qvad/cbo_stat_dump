#!/bin/bash
set -e

echo "Waiting for YugabyteDB at $PROD_HOST:$PROD_PORT..."
until pg_isready -h "$PROD_HOST" -p "$PROD_PORT" -U "$PROD_USER"; do
  echo "Waiting for YugabyteDB..."
  sleep 2
done
echo "YugabyteDB is ready."

EXIT_CODE=0

echo "--------------------------------------------------"
echo "Running Basic Test"
echo "--------------------------------------------------"

if test_benchmark \
  -b "basic" \
  --yb_mode \
  --prod_host "$PROD_HOST" \
  --prod_port "$PROD_PORT" \
  --prod_user "$PROD_USER" \
  --create_prod_db \
  -d; then
    echo "Basic test passed!"
else
    echo "Basic test failed!"
    find test_out_dir -name "query_plan_diff.txt" -print -exec cat {} \;
    EXIT_CODE=1
fi

echo "--------------------------------------------------"
echo "Running Join Order Benchmark"
echo "--------------------------------------------------"

if test_benchmark \
  -b "join_order_benchmark" \
  --yb_mode \
  --prod_host "$PROD_HOST" \
  --prod_port "$PROD_PORT" \
  --prod_user "$PROD_USER" \
  --create_prod_db \
  -d; then
    echo "Join Order Benchmark passed!"
else
    echo "Join Order Benchmark failed!"
    find test_out_dir -name "query_plan_diff.txt" -print -exec cat {} \;
    EXIT_CODE=1
fi

exit $EXIT_CODE
