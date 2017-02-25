"""PostgreSQL engine."""

import copy
import csv
from collections import OrderedDict


__DEFAULT_TYPE_PATTERN = [
    OrderedDict([
        ('typename', 'INTEGER'),
        ('predicate', OrderedDict([
            ('type', 'all-of'),
            ('args', [
                OrderedDict([
                    ('type', 'compatible'),
                    ('args', ['int']),
                ]),
                OrderedDict([
                    ('type', 'not'),
                    ('args', [
                        OrderedDict([
                            ('type', 'match'),
                            ('args', ['^0[0-9]+']),
                        ]),
                    ]),
                ]),
            ]),
        ])),
    ]),
    OrderedDict([
        ('typename', 'DOUBLE PRECISION'),
        ('predicate', OrderedDict([
            ('type', 'all-of'),
            ('args', [
                OrderedDict([
                    ('type', 'compatible'),
                    ('args', ['float']),
                ]),
                OrderedDict([
                    ('type', 'not'),
                    ('args', [
                        OrderedDict([
                            ('type', 'match'),
                            ('args', ['^0[0-9]+']),
                        ]),
                    ]),
                ]),
            ]),
        ])),
    ]),
    OrderedDict([
        ('typename', 'VARCHAR(255)'),
        ('predicate', OrderedDict([
            ('type', 'shorter-than'),
            ('args', [255]),
        ])),
    ]),
    OrderedDict([
        ('typename', 'TEXT'),
        ('predicate', OrderedDict([
            ('type', 'any')
        ])),
    ]),
]
_LINE_TERMINATOR = '\n'


def type_patterns():
    """Return the default type pattern."""
    return copy.deepcopy(__DEFAULT_TYPE_PATTERN)


def _quote_schema(name):
    escaped = name.replace('"', '\\"')
    return '"{0}"'.format(escaped)


def write_schema_statement(out_stream, table_name, column_types, rebuild=False):
    """Write the schema query into `out_stream`.
    When `rebuild` is true, it prepends the query
    'DROP TABLE IF EXISTS `table_name`.
    """
    if rebuild:
        out_stream.write('DROP TABLE IF EXISTS {0};'.format(table_name))
        out_stream.write(_LINE_TERMINATOR)

    out_stream.write('CREATE TABLE {0} ('.format(table_name))
    out_stream.write(_LINE_TERMINATOR)
    for index, column_type in enumerate(column_types):
        if index != 0:
            out_stream.write(',')
            out_stream.write(_LINE_TERMINATOR)
        column_name, type_name = column_type[0], column_type[1]
        out_stream.write(
            '  {0} {1}'.format(_quote_schema(column_name), type_name))
    out_stream.write(_LINE_TERMINATOR)
    out_stream.write(');')
    out_stream.write(_LINE_TERMINATOR)


def write_insert_statement(
        out_stream, table_name, reader, null_value, rebuild=False):
    """Write the insert query into `out_stream`.
    When `rebuild` is true, it prepends the query
    'TRUNCATE TABLE `table_name`.
    """
    if rebuild:
        out_stream.write('TRUNCATE TABLE {0};'.format(table_name))
        out_stream.write(_LINE_TERMINATOR)

    out_stream.write(
        'COPY {0} FROM STDIN WITH NULL \'{1}\' CSV;'.format(
            table_name,
            null_value,
        )
    )
    out_stream.write(_LINE_TERMINATOR)

    writer_dialect = csv.excel()
    writer_dialect.lineterminator = _LINE_TERMINATOR
    writer = csv.writer(out_stream, dialect=writer_dialect)
    for row in reader:
        writer.writerow(row)

    out_stream.write('\\.')
    out_stream.write(_LINE_TERMINATOR)
