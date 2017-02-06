from __future__ import unicode_literals
from unittest import TestCase
from io import StringIO
from itertools import islice

from nose.tools import eq_
from nose_parameterized import parameterized

from csv2sql.core.prefetching import RewindableFileIterator


class TestRewindableFileIterator(TestCase):
    @parameterized.expand([
        (StringIO(''), 0, [], []),
        (StringIO(''), 1, [], []),
        (StringIO('A\nB\n'), 0, [], ['A\n', 'B\n']),
        (StringIO('A\nB\n'), 1, ['A\n'], ['A\n', 'B\n']),
        (StringIO('A\nB\n'), 2, ['A\n', 'B\n'], ['A\n', 'B\n']),
        (StringIO('A\nB\n'), 3, ['A\n', 'B\n'], ['A\n', 'B\n']),
    ])
    def test(self, in_file, num_line, expected_pre_fetching, expected_all):
        with RewindableFileIterator(in_file) as file_iterator:
            actual_pre_fetching = list(islice(file_iterator, num_line))
            file_iterator.rewind()
            actual_all = list(file_iterator)
        eq_(list(actual_pre_fetching), expected_pre_fetching)
        eq_(list(actual_all), expected_all)
