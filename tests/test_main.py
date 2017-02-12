from unittest import TestCase

from nose.tools import ok_, eq_, raises
from nose_parameterized import parameterized

from csv2sql.main import parse_args


class ParseArgs(TestCase):
    @parameterized.expand([
        ('all',),
        ('schema',),
    ])
    def test_schema_dumper(self, command_name):
        arguments = [command_name, 'table-name']
        actual = parse_args(arguments)
        eq_(actual.table_name, 'table-name')
        ok_(hasattr(actual, 'in_file'))
        ok_(hasattr(actual, 'out_file'))
        ok_(hasattr(actual, 'null'))
        ok_(hasattr(actual, 'delimiter'))
        ok_(hasattr(actual, 'pattern_file'))
        ok_(hasattr(actual, 'rebuild'))
        ok_(hasattr(actual, 'column_type'))
        ok_(hasattr(actual, 'lines_for_inference'))
        ok_(hasattr(actual, 'command'))
        ok_(hasattr(actual, 'query_engine'))
        ok_(hasattr(actual, 'index_types'))

    @parameterized.expand([
        ('data',),
    ])
    def test_insertion_dumper(self, command_name):
        arguments = [command_name, 'table-name']
        actual = parse_args(arguments)
        eq_(actual.table_name, 'table-name')
        ok_(hasattr(actual, 'in_file'))
        ok_(hasattr(actual, 'out_file'))
        ok_(hasattr(actual, 'null'))
        ok_(hasattr(actual, 'delimiter'))
        ok_(hasattr(actual, 'rebuild'))
        ok_(hasattr(actual, 'command'))
        ok_(hasattr(actual, 'query_engine'))

    @parameterized.expand([
        (['-t', '1-type'],),
        (['--column-type', '0:type'],),
    ])
    @raises(ValueError)
    def test_invalid_column_type(self, type_column_args):
        arguments = ['all', 'table-name'] + type_column_args
        parse_args(arguments)
