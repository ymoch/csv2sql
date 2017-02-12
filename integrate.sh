#!/bin/bash

################################################################################
# Constant values.
################################################################################
readonly msg_run="*** RUN ***"
readonly msg_fail="*** FAILED ***"
readonly msg_abort="*** ABORT ***"
readonly run_csv2sql="coverage run -a --source=csv2sql -m csv2sql"

################################################################################
# Global variables.
################################################################################
TOTAL_ERROR_CODE=0

################################################################################
# Utility functions.
################################################################################
expect_success() {
  local command=$@

  echo "${msg_run}" "${command}" >&2
  ${command}
  local error_code=$?

  if [ "${error_code}" -ne 0 ]; then
    echo "${msg_fail}" 'Not expected code:' "${error_code}" >&2
    TOTAL_ERROR_CODE=$?
  fi
  return ${error_code}
}

assert_success() {
  local command=$@

  expect_success ${command}
  local error_code=$?
  if [ "${error_code}" -ne 0 ]; then
    echo "${msg_abort}" >&2
    exit ${TOTAL_ERROR_CODE}
  fi
  return ${error_code}
}

expect_failure() {
  local command=$@

  echo "${msg_run}" "${command}" >&2
  ${command}
  local error_code=$?

  if [ "$error_code" -eq 0 ]; then
    echo "${msg_fail}" 'Not expected code:' "${error_code}" >&2
  fi
  return "${error_code}"
}

################################################################################
# Testing foundation.
################################################################################
init_testing() {
  assert_success rm -f .coverage
  assert_success docker-compose down
  assert_success docker-compose up -d psql_server
}

check_result() {
  if [ "$TOTAL_ERROR_CODE" -eq 0 ]; then
    echo "*** SUCCEEDED ***"
   else
    echo "*** FAILED ***"
  fi
  return "$TOTAL_ERROR_CODE"
}

################################################################################
# Testing.
################################################################################
test_units() {
  assert_success coverage run -a --source=csv2sql setup.py test
}

test_any_engine_pattern_file_acceptable() {
  expect_success ${run_csv2sql} pattern > pattern.yml
  expect_success ${run_csv2sql} all -p pattern.yml tbl < data/test-input.csv
}

test_any_engine_null_value_changed() {
  expect_success ${run_csv2sql} \
    all -n NULL null_value_changed < data/test-any-engine.csv \
  | tee /dev/stderr | expect_success docker-compose run psql_client
}

test_any_engine() {
  expect_success ${run_csv2sql} \
    all tbl --lines-for-inference 10 < data/test-input.csv \
  | tee /dev/stderr | expect_success docker-compose run psql_client

  expect_success ${run_csv2sql} \
    schema -r -p pattern.yml -t 3:TEXT tbl < data/test-input.csv \
  | tee /dev/stderr | expect_success docker-compose run psql_client

  expect_success ${run_csv2sql} data -r tbl < data/test-input.csv \
  | tee /dev/stderr | expect_success docker-compose run psql_client
}

################################################################################
# Main.
################################################################################
integrate() {
  init_testing

  test_units
  test_any_engine_pattern_file_acceptable
  test_any_engine_null_value_changed
  test_any_engine

  check_result
}

integrate
exit ${TOTAL_ERROR_CODE}
