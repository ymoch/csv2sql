#!/bin/bash

TOTAL_ERROR_CODE=0

expect_success() {
  local command=$@

  echo 'Run:' ${command} >&2
  ${command}
  local error_code=$?

  if [ "${error_code}" -ne 0 ]; then
    echo 'Not expected code:' ${error_code} >&2
    TOTAL_ERROR_CODE=$?
  fi
  return ${error_code}
}

assert_success() {
  local command=$@

  expect_success ${command}
  local error_code=$?
  if [ "${error_code}" -ne 0 ]; then
    echo 'Abort!' >&2
    exit ${TOTAL_ERROR_CODE}
  fi
  return ${error_code}
}

expect_failure() {
  local command=$@

  echo 'Run: ' ${command}
  ${command}
  local error_code=$?

  if [ "$error_code" -eq 0 ]; then
    echo 'Not expected code: ' ${error_code} >&1
  fi
  return ${error_code}
}

init_testing() {
  assert_success rm -f .coverage
  assert_success docker-compose down
}

unit_test() {
  assert_success coverage run -a --source=csv2sql setup.py test
}

integrate() {
  expect_success coverage run -a --source=csv2sql -m csv2sql \
    all tbl --lines-for-inference 10 < data/test-input.csv \
  | expect_success docker-compose run psql_client

  expect_success coverage run -a --source=csv2sql -m csv2sql \
    pattern > pattern.yml

  expect_success coverage run -a --source=csv2sql -m csv2sql \
    schema -r -p pattern.yml -t 3:TEXT tbl < data/test-input.csv \
  | expect_success docker-compose run psql_client

  expect_success coverage run -a --source=csv2sql -m csv2sql \
    data -r tbl < data/test-input.csv \
  | expect_success docker-compose run psql_client
}

main() {
  init_testing
  unit_test
  integrate
}

main
exit ${TOTAL_ERROR_CODE}
