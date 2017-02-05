from unittest import TestCase

from mock import patch
from nose.tools import eq_, raises
from nose_parameterized import parameterized

from csv2sql.core.type_inference import interpret_predicate
from csv2sql.core.type_inference import interpret_patterns
from csv2sql.core.type_inference import TypeInferenceError
from csv2sql.core.type_inference import TypeInferrer
from csv2sql.core.type_inference import decide_types


class TestInterpretOnePredicate(TestCase):
    obj_compatible_int = {'type': 'compatible', 'args': ['int']}
    obj_compatible_float = {'type': 'compatible', 'args': ['float']}
    obj_shorter_than = {'type': 'shorter-than', 'args': ['5']}
    obj_match = {'type': 'match', 'args': ['abc*']}
    obj_all_of = {'type': 'all-of', 'args': [obj_shorter_than, obj_match]}
    obj_all_of_empty = {'type': 'all-of'}
    obj_any_of = {'type': 'any-of', 'args': [obj_shorter_than, obj_match]}
    obj_any_of_empty = {'type': 'any-of'}
    obj_any = {'type': 'any'}
    obj_not = {'type': 'not', 'args': [obj_any]}

    @parameterized.expand([
        (obj_compatible_int, '0', True),
        (obj_compatible_int, '1', True),
        (obj_compatible_int, '01', True),
        (obj_compatible_int, '-1', True),
        (obj_compatible_int, '0.1', False),
        (obj_compatible_int, 'A', False),
        (obj_compatible_float, '1', True),
        (obj_compatible_float, '0.1', True),
        (obj_compatible_float, '-0.1', True),
        (obj_compatible_float, '-1.234e-5', True),
        (obj_compatible_float, 'A', False),
        (obj_shorter_than, '', True),
        (obj_shorter_than, 'psql', True),
        (obj_shorter_than, 'mysql', False),
        (obj_match, 'bc', False),
        (obj_match, 'ab', True),
        (obj_match, 'zabz', True),
        (obj_all_of, 'ab', True),
        (obj_all_of, 'bc', False),
        (obj_all_of, 'abccc', False),
        (obj_all_of, 'bcccc', False),
        (obj_all_of_empty, '', True),
        (obj_any_of, 'ab', True),
        (obj_any_of, 'bc', True),
        (obj_any_of, 'abccc', True),
        (obj_any_of, 'bcccc', False),
        (obj_any_of_empty, '', False),
        (obj_any, '', True),
        (obj_not, '', False),
    ])
    def test(self, obj, value, expected):
        predicate = interpret_predicate(obj)
        actual = predicate(value)
        eq_(actual, expected)


class TestInterpretPattern(TestCase):
    @staticmethod
    @patch('csv2sql.core.type_inference.interpret_predicate',
           side_effect=lambda pred: pred)
    def test(interpret_predicate_mock):
        obj = [
            {'typename': 'A', 'predicate': 'pred1'},
            {'typename': 'B', 'predicate': 'pred2'},
        ]
        expected = [(item['typename'], item['predicate']) for item in obj]

        actual = interpret_patterns(obj)
        eq_(actual, expected)
        eq_(interpret_predicate_mock.call_args_list,
            [((item['predicate'],),) for item in obj])


class TestTypeInferer(TestCase):
    empty_patterns = []
    patterns = [
        ('type1', lambda x: x is '1'),
        ('type2', lambda x: x is '2'),
    ]

    @raises(TypeInferenceError)
    def test_empty_pattern_raises_an_error(self):
        TypeInferrer(self.empty_patterns)

    @raises(TypeInferenceError)
    def test_no_matching_patterns_raises_an_error(self):
        inferrer = TypeInferrer(self.patterns)
        inferrer.read_item('0')

    @parameterized.expand([
        ('', [], 'type1'),
        ('', ['1'], 'type1'),
        ('', [''], 'type1'),
        ('\\N', ['\\N'], 'type1'),
        ('', ['1', '2'], 'type2'),
        ('', ['2'], 'type2'),
    ])
    def test(self, null_value, items, expected):
        inferrer = TypeInferrer(self.patterns, null_value=null_value)
        for item in items:
            inferrer.read_item(item)
        eq_(inferrer.type_name, expected)


class TestDecideTypes(TestCase):
    reader = [('V1', 'V2')]
    column_names = ('T1', 'T2')
    patterns = [('null', lambda _: False), ('inferred', lambda _: True)]

    @parameterized.expand([
        ({}, ['inferred', 'inferred']),
        ({'null_value': 'V1'}, ['null', 'inferred']),
        ({'index_types': [(1, 'index-type')]}, ['inferred', 'index-type'])
    ])
    def test(self, kwargs, expected):
        actual = decide_types(
            self.patterns, self.reader, self.column_names, **kwargs)
        eq_(actual, expected)
