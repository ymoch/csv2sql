#!/usr/bin/env python

import unittest
import csv
import itertools
import tempfile
from subprocess import Popen, PIPE

from nose.tools import ok_, eq_
from nose_parameterized import parameterized


_RUN = ['coverage', 'run', '-a', '--source=csv2sql', '-m', 'csv2sql']
_RUN_QUERY = {
    'psql': ['docker-compose', 'run', 'psql_client'],
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


def run_pipe_process(args_list, stdin=None, stdout=None, stderr=None):
    processes = _pipe(args_list, stdin=stdin, stdout=stdout, stderr=stderr)
    for process in reversed(processes):
        process.communicate()
    return tuple(process.wait() for process in processes)


def _test_query(args, query_engine, expect_success, stdin=None):
    main_args = _RUN + args + ['-q', query_engine]
    query_args = _RUN_QUERY[query_engine]

    args_list = [main_args, query_args]

    statuses = run_pipe_process(args_list, stdin=stdin)
    eq_(statuses[0:-1], tuple([0] * (len(args_list) - 1)))
    if expect_success:
        eq_(statuses[-1], 0)
    else:
        ok_(statuses[-1] != 0)


def assert_query_succeeds(*args, **kwargs):
    kwargs['expect_success'] = kwargs.get('expect_success', True)
    _test_query(*args, **kwargs)


def assert_query_fails(*args, **kwargs):
    kwargs['expect_success'] = kwargs.get('expect_success', False)
    _test_query(*args, **kwargs)


class TestForAnyEngine(unittest.TestCase):
    @parameterized.expand(itertools.product(list(_RUN_QUERY), [
        ('null_value_acceptable', ['-n', 'NULL']),
        ('column_type_acceptable', ['-t', '2:TEXT']),
    ]))
    def test_for_any_engine_succeeds(self, query_engine, name_args_pair):
        name, tmp_args = name_args_pair
        table_name = '{0}_{1}_succeeds'.format(query_engine, name)
        args = (
            ['all', '-r', '-i', 'data/test-any-engine.csv', table_name] +
            tmp_args)
        assert_query_succeeds(args, query_engine)

    @parameterized.expand(list(_RUN_QUERY))
    def test_pattern_file_succeeds(self, query_engine):
        pattern_file_path = 'pattern_{0}.yml'.format(query_engine)
        common_args = ['-q', query_engine]

        args_list = [_RUN + ['pattern'] + common_args]
        with open(pattern_file_path, 'w+') as pattern_file:
            statuses = run_pipe_process(args_list, stdout=pattern_file)
        ok_(all(status == 0 for status in statuses))

        table_name = '{0}_pattern_file_succeeds'.format(query_engine)
        args = [
            'all', '-r', '-p', pattern_file_path,
            '-i', 'data/test-any-engine.csv', table_name]
        assert_query_succeeds(args, query_engine)

    @parameterized.expand(list(_RUN_QUERY))
    def test_run_separately_succeeds(self, query_engine):
        table_name = '{0}_run_separately_succeeds'.format(query_engine)
        common_args = ['-r', '-i', 'data/test-any-engine.csv', table_name]

        assert_query_succeeds(['schema'] + common_args, query_engine)
        assert_query_succeeds(['data'] + common_args, query_engine)

    @parameterized.expand(list(_RUN_QUERY))
    def test_no_rebuild_schema_fails(self, query_engine):
        table_name = '{0}_no_rebuild_schema_fails'.format(query_engine)
        common_args = ['schema', '-i', 'data/test-any-engine.csv', table_name]

        assert_query_succeeds(common_args + ['-r'], query_engine)
        assert_query_fails(common_args, query_engine)

    @parameterized.expand(list(_RUN_QUERY))
    def test_small_line_type_inference_fails(self, query_engine):
        table_name = '{0}_small_line_type_inference_fails'.format(query_engine)
        args = [
            'all', '-r', '-i', 'data/test-any-engine.csv',
            '--lines-for-inference', '2', table_name]
        assert_query_fails(args, query_engine)

    @parameterized.expand(list(_RUN_QUERY))
    def test_tsv_succeeds(self, query_engine):
        input_file = 'data/test-any-engine.tsv'
        table_name = '{0}_tsv_succeeds'.format(query_engine)
        assert_query_succeeds(
            ['all', '-r', '-i', input_file, '-d', '\t', table_name],
            query_engine)

    @parameterized.expand(list(_RUN_QUERY))
    def test_long_row_succeeds(self, query_engine):
        with tempfile.NamedTemporaryFile('w+') as input_file:
            writer = csv.writer(input_file)
            writer.writerow(['column'])
            writer.writerow(['A' * 10 * 1024 * 1024])  # 10 Megabytes.
            for _ in range(1024 * 1024):
                writer.writerow(['ABCDEFGHIJKLMNOPQRST'])  # 20 Megabytes.
            input_file.flush()
            input_file.seek(0)

            table_name = '{0}_long_row_succeeds'.format(query_engine)
            assert_query_succeeds(
                ['all', '-r', table_name], query_engine, stdin=input_file)
