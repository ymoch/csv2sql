#!/usr/bin/env python

import unittest
import itertools
from subprocess import Popen, PIPE

from nose.tools import ok_, eq_
from nose_parameterized import parameterized


_RUN = ['coverage', 'run', '-a', '--source=csv2sql', '-m', 'csv2sql']
_RUN_QUERY = {
    'psql': ['docker-compose', 'run', 'psql_client', '--set', 'ON_ERROR_STOP=on'],
}


def _pipe(args_list, stdin=None, stdout=None, stderr=None):
    processes = [
        Popen(args_list[0], stdin=stdin, stdout=PIPE, stderr=stderr)
    ]

    for current_args in args_list[1:-1]:
        prev = processes[-1]
        processes.append(
            Popen(current_args, stdin=prev.stdout, stdout=PIPE, stderr=stderr)
        )
    prev = processes[-1]
    processes.append(
        Popen(args_list[-1], stdin=prev.stdout, stdout=stdout, stderr=stderr)
    )

    processes[-1].stdout = stdout
    return processes


def run_pipe_process(args_list, stdout=None, stderr=None):
    processes = _pipe(args_list, stdout=stdout, stderr=stderr)
    for process in reversed(processes):
        process.communicate()
    return tuple(process.wait() for process in processes)


def _test_query(input_file, args, query_engine, expect_success):
    main_args = _RUN + args + ['-q', query_engine]
    query_args = _RUN_QUERY[query_engine]

    args_list = [('cat', input_file), main_args, query_args]
    statuses = run_pipe_process(args_list)
    eq_(statuses[0:-1], tuple([0] * (len(args_list) - 1)))
    if expect_success:
        eq_(statuses[-1], 0)
    else:
        ok_(statuses != 0)


def assert_query_succeeds(input_file, args, query_engine):
    _test_query(input_file, args, query_engine, True)


def assert_query_fails(input_file, args, query_engine):
    _test_query(input_file, args, query_engine, False)


class TestForAnyEngine(unittest.TestCase):
    @parameterized.expand(itertools.product(list(_RUN_QUERY), [
        ('null_value_acceptable', ('-n', 'NULL')),
        ('column_type_acceptable', ('-t', '2:TEXT')),
        ('all', ('--lines-for-inference', '10')),
    ]))
    def test_for_any_engine(self, query_engine, name_args_pair):
        name, tmp_args = name_args_pair
        table_name = '{0}_{1}'.format(query_engine, name)
        args = ['all', '-r', table_name] + list(tmp_args)
        assert_query_succeeds('data/test-any-engine.csv', args, query_engine)

    @parameterized.expand(list(_RUN_QUERY))
    def test_pattern_file_acceptable(self, query_engine):
        pattern_file_path = 'pattern_{0}.yml'.format(query_engine)
        common_args = ['-q', query_engine]

        args_list = [_RUN + ['pattern'] + common_args]
        with open(pattern_file_path, 'w+') as pattern_file:
            statuses = run_pipe_process(args_list, stdout=pattern_file)
        ok_(all(status == 0 for status in statuses))

        table_name = '{0}_pattern_file_acceptable'.format(query_engine)
        args = ['all', '-r', '-p', pattern_file_path, table_name]
        assert_query_succeeds('data/test-any-engine.csv', args, query_engine)

    @parameterized.expand(list(_RUN_QUERY))
    def test_run_separately_acceptable(self, query_engine):
        table_name = '{0}_run_separately_accepted'.format(query_engine)
        common_args = ['-r', table_name]

        assert_query_succeeds(
            'data/test-any-engine.csv', ['schema'] + common_args, query_engine)
        assert_query_succeeds(
            'data/test-any-engine.csv', ['data'] + common_args, query_engine)

    @parameterized.expand(list(_RUN_QUERY))
    def test_no_rebuild_schema_fail(self, query_engine):
        input_file = 'data/test-any-engine.csv'
        table_name = '{0}_no_rebuild_schema_fail'.format(query_engine)
        assert_query_succeeds(
            input_file, ['schema', '-r', table_name], query_engine)
        assert_query_fails(
            input_file, ['schema', table_name], query_engine)
