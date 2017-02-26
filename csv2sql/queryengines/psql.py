"""PostgreSQL engine."""

import copy
import csv
from collections import OrderedDict

from six.moves import cStringIO as StringIO


_DEFAULT_TYPE_PATTERN = [
    OrderedDict([
        ('typename', 'INTEGER'),
        ('predicate', OrderedDict([
            ('type', 'all-of'),
            ('args', [
                OrderedDict([
                    ('type', 'compatible'),
                    ('args', 'int'),
                ]),
                OrderedDict([
                    ('type', 'not'),
                    ('args', [
                        OrderedDict([
                            ('type', 'match'),
                            ('args', '^0[0-9]+'),
                        ]),
                    ]),
                ]),
                OrderedDict([
                    ('type', 'greater-than-or-equal-to'),
                    ('args', -2147483648),
                ]),
                OrderedDict([
                    ('type', 'less-than-or-equal-to'),
                    ('args', 2147483647),
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
                    ('args', 'float'),
                ]),
                OrderedDict([
                    ('type', 'not'),
                    ('args', [
                        OrderedDict([
                            ('type', 'compatible'),
                            ('args', 'int'),
                        ]),
                    ]),
                ]),
                OrderedDict([
                    ('type', 'not'),
                    ('args', [
                        OrderedDict([
                            ('type', 'match'),
                            ('args', '^0[0-9]+'),
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
            ('args', 255),
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


class _WriterWrapper(object):
    def __init__(self, stream, *args, **kwargs):
        self._stream = stream
        self._queue = StringIO()
        self._writer = csv.writer(self._queue, *args, **kwargs)

    def writerow(self, row):
        self._writer.writerow(row)

        data = self._queue.getvalue()
        if data == '\\.\r\n':
            data = '"\\."\r\n'.format(data)

        self._stream.write(data)
        self._queue.seek(0)
        self._queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


def type_patterns():
    """Return the default type pattern."""
    return copy.deepcopy(_DEFAULT_TYPE_PATTERN)


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

    writer = _WriterWrapper(out_stream, dialect='excel')
    writer.writerows(reader)

    out_stream.write('\\.')
    out_stream.write(_LINE_TERMINATOR)
