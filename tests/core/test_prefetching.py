from __future__ import unicode_literals
from unittest import TestCase
from io import StringIO
from itertools import islice

from nose.tools import ok_, eq_
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
    def test_iteration(
            self, in_file, num_line, expected_pre_fetching, expected_all):
        with RewindableFileIterator(in_file) as file_iterator:
            actual_pre_fetching = list(islice(file_iterator, num_line))
            file_iterator.rewind()
            actual_all = list(file_iterator)
            ok_(not file_iterator.closed)
        eq_(list(actual_pre_fetching), expected_pre_fetching)
        eq_(list(actual_all), expected_all)
        ok_(file_iterator.closed)

    @parameterized.expand([
        (StringIO(''), 0, [], []),
        (StringIO(''), 1, [], []),
        (StringIO('A\nB\n'), 0, [], ['A\n', 'B\n']),
        (StringIO('A\nB\n'), 1, ['A\n'], ['B\n']),
        (StringIO('A\nB\n'), 2, ['A\n', 'B\n'], []),
    ])
    def test_freeze(
            self, in_file, num_line, expected_pre_fetching, expected_frozen):
        with RewindableFileIterator(in_file) as file_iterator:
            actual_pre_fetching = list(islice(file_iterator, num_line))
            actual_frozen = list(file_iterator.freeze())
        eq_(list(actual_pre_fetching), expected_pre_fetching)
        eq_(list(actual_frozen), expected_frozen)
        ok_(file_iterator.closed)
