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
_QUERY_ENGINES = list(_RUN_QUERY)


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


def prepare_csv_file(rows, dialect=None):
    if not dialect:
        dialect = csv.excel()

    input_file = tempfile.TemporaryFile(mode='w+')
    writer = csv.writer(input_file, dialect=dialect)
    for row in rows:
        writer.writerow(row)

    input_file.seek(0)
    return input_file


class TestForAnyEngine(unittest.TestCase):
    DEFAULT_ROWS = [
        ['key', 'value1', 'value2', 'value3', 'value4', 'value5'],
        ['key1', '0', '012', '1', 'NULL', '8'],
        ['key2', '1', '123', '', '2', '9'],
        ['key3', '2', '234', '3', '3', 'x'],
    ]

    @parameterized.expand(itertools.product(_QUERY_ENGINES, [
        ('null_value_acceptable', ['-n', 'NULL']),
        ('column_type_acceptable', ['-t', '2:TEXT']),
    ]))
    def test_for_any_engine_succeeds(self, query_engine, name_args_pair):
        name, tmp_args = name_args_pair
        table_name = '{0}_{1}_succeeds'.format(query_engine, name)
        args = ['all', '-r', table_name] + tmp_args

        with prepare_csv_file(self.DEFAULT_ROWS) as in_file:
            assert_query_succeeds(args, query_engine, stdin=in_file)

    @parameterized.expand(_QUERY_ENGINES)
    def test_pattern_file_succeeds(self, query_engine):
        pattern_file_path = 'pattern_{0}.yml'.format(query_engine)
        common_args = ['-q', query_engine]

        args_list = [_RUN + ['pattern'] + common_args]
        with open(pattern_file_path, 'w+') as pattern_file:
            statuses = run_pipe_process(args_list, stdout=pattern_file)
        ok_(all(status == 0 for status in statuses))

        table_name = '{0}_pattern_file_succeeds'.format(query_engine)
        args = ['all', '-r', '-p', pattern_file_path, table_name]

        with prepare_csv_file(self.DEFAULT_ROWS) as in_file:
            assert_query_succeeds(args, query_engine, stdin=in_file)

    @parameterized.expand([
        ('{{}}\n',),
    ])
    def test_invalid_pattern_file_fails(self, data):
        with tempfile.NamedTemporaryFile(mode='w+') as pattern_file:
            pattern_file.write(data)
            pattern_file.seek(0)

            args = [_RUN + ['pattern', '-p', pattern_file.name]]
            statuses = run_pipe_process(args)
            ok_(all(status != 0 for status in statuses))

    def test_not_existing_pattern_file_fails(self):
        args = [_RUN + ['pattern', '-p', 'not-existing-path']]
        statuses = run_pipe_process(args)
        ok_(all(status != 0 for status in statuses))

    @parameterized.expand(_QUERY_ENGINES)
    def test_run_separately_succeeds(self, query_engine):
        table_name = '{0}_run_separately_succeeds'.format(query_engine)
        args_schema = ['schema', '-r', table_name]
        args_data = ['data', '-r', table_name]

        with prepare_csv_file(self.DEFAULT_ROWS) as in_file:
            assert_query_succeeds(args_schema, query_engine, stdin=in_file)
            in_file.seek(0)
            assert_query_succeeds(args_data, query_engine, stdin=in_file)

    @parameterized.expand(_QUERY_ENGINES)
    def test_no_rebuild_schema_fails(self, query_engine):
        table_name = '{0}_no_rebuild_schema_fails'.format(query_engine)
        args_rebuild = ['schema', '-r', table_name]
        args_no_rebuild = ['schema', table_name]

        with prepare_csv_file(self.DEFAULT_ROWS) as in_file:
            assert_query_succeeds(args_rebuild, query_engine, stdin=in_file)
            in_file.seek(0)
            assert_query_fails(args_no_rebuild, query_engine, stdin=in_file)

    @parameterized.expand(_QUERY_ENGINES)
    def test_small_line_type_inference_fails(self, query_engine):
        table_name = '{0}_small_line_type_inference_fails'.format(query_engine)
        args = ['all', '-r', '--lines-for-inference', '2', table_name]

        with prepare_csv_file(self.DEFAULT_ROWS) as in_file:
            assert_query_fails(args, query_engine, stdin=in_file)

    @parameterized.expand(_QUERY_ENGINES)
    def test_tsv_succeeds(self, query_engine):
        table_name = '{0}_tsv_succeeds'.format(query_engine)
        args = ['all', '-r', '-d', '\t', table_name]

        with prepare_csv_file(
                self.DEFAULT_ROWS, dialect=csv.excel_tab()) as in_file:
            assert_query_succeeds(args, query_engine, stdin=in_file)

    @parameterized.expand(_QUERY_ENGINES)
    def test_long_row_succeeds(self, query_engine):
        rows = itertools.chain(
            [['column']],
            [['A' * 10 * 1048576]],  # 10 MB.
            itertools.repeat(['ABCDEFGHIJKLMNOPQRST'], 1048576),  # 20 MB.
        )
        table_name = '{0}_long_row_succeeds'.format(query_engine)
        args = ['all', '-r', table_name]

        with prepare_csv_file(rows) as in_file:
            assert_query_succeeds(args, query_engine, stdin=in_file)


class TestPsql(unittest.TestCase):
    def test_dangerous_integer_succeeds(self):
        rows = [
            ['too_large_integer', 'too_small_integer'],
            ['2147483648', '-2147483649'],
        ]
        table_name = 'psql_dangerous_integer'
        args = ['all', '-r', table_name]
        with prepare_csv_file(rows) as in_file:
            assert_query_succeeds(args, 'psql', stdin=in_file)

    def test_dangerous_double_succeeds(self):
        rows = [
            ['too_precise_double'],
            ['1.234567890123456'],
        ]
        table_name = 'psql_dangerous_double'
        args = ['all', '-r', table_name]
        with prepare_csv_file(rows) as in_file:
            assert_query_succeeds(args, 'psql', stdin=in_file)


if __name__ == '__main__':
    unittest.main()
