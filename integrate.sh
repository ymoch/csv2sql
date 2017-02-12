#!/bin/bash -eu

################################################################################
# Testing foundation.
################################################################################
initialize() {
  rm -f .coverage
  docker-compose down
  docker-compose build
  docker-compose up -d psql_server
}

################################################################################
# Testing.
################################################################################
test_units() {
  coverage run -a --source=csv2sql setup.py test
}

test_integration() {
  nosetests integrate.py
}

################################################################################
# Main.
################################################################################
integrate() {
  initialize
  test_units
  test_integration
}

integrate
