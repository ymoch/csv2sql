"""Type pattern."""

import re
import decimal
import operator
import itertools
import functools

from csv2sql.core.error import InterpretationError, TypeInferenceError


def _compatible(cast_type, value):
    try:
        cast_type(value)
    except ValueError:
        return False
    return True


_COMPATIBLE_PREDICATES = {
    'int': functools.partial(_compatible, int),
    'float': functools.partial(_compatible, float),
}
_DEFAULT_NULL_VALUE = ''


def _create_compatible_predicate(args):
    if len(args) != 1:
        raise InterpretationError(
            'Compatible predicate takes only 1 argument, '
            'given {0}.'.format(len(args)))

    cast_type_name = args[0]
    try:
        return _COMPATIBLE_PREDICATES[cast_type_name]
    except:
        raise InterpretationError(
            'Compatible predicate takes one of ({0}), '
            'given {1}'.format(
                '|'.join(list(_COMPATIBLE_PREDICATES)),
                cast_type_name
            )
        )


def _create_compare_predicate(operator_, args):
    if len(args) != 1:
        raise InterpretationError(
            'Compare predicate takes only 1 argument, '
            'given {0}.'.format(len(args)))

    try:
        comp_value = decimal.Decimal(args[0])
    except:
        raise InterpretationError(
            'Compare predicate takes only a decimal argument, '
            'given {0}.'.format(args[0]))

    return lambda value: operator_(decimal.Decimal(value), comp_value)


def _create_shorter_than_predicate(args):
    if len(args) != 1:
        raise InterpretationError(
            'Shorter-than predicate takes only 1 argument, '
            'given {0}.'.format(len(args)))

    try:
        max_length = int(args[0])
    except:
        raise InterpretationError(
            'Shorter-than predicate takes only an integer argument, '
            'given {0}.'.format(args[0]))
    return lambda value: len(value) < max_length


def _create_match_predicate(args):
    if len(args) != 1:
        raise InterpretationError(
            'Match predicate takes only 1 argument, '
            'given {0}.'.format(len(args)))

    pattern = re.compile(args[0])
    return lambda value: bool(pattern.search(value))


def _create_all_of_predicate(args):
    predicates = [interpret_predicate(obj) for obj in args]
    return lambda value: all(predicate(value) for predicate in predicates)


def _create_any_of_predicate(args):
    predicates = [interpret_predicate(obj) for obj in args]
    return lambda value: any(predicate(value) for predicate in predicates)


def _create_not_predicate(args):
    positive_predicate = interpret_predicate(args[0])
    return lambda value: not positive_predicate(value)


def _always_true(_):
    return True


def _create_any_predicate(args):
    if len(args) != 0:
        raise InterpretationError('Match predicate takes no argument.')

    return _always_true


_PREDICATE_GENERATORS = {
    'compatible': _create_compatible_predicate,
    'less-than': functools.partial(_create_compare_predicate, operator.lt),
    'less-than-or-equal-to': functools.partial(
        _create_compare_predicate, operator.le),
    'greater-than': functools.partial(_create_compare_predicate, operator.gt),
    'greater-than-or-equal-to': functools.partial(
        _create_compare_predicate, operator.ge),
    'shorter-than': _create_shorter_than_predicate,
    'match': _create_match_predicate,
    'all-of': _create_all_of_predicate,
    'any-of': _create_any_of_predicate,
    'any': _create_any_predicate,
    'not': _create_not_predicate,
}


def interpret_predicate(obj):
    """Interpret a predicate."""
    try:
        predicate_type = obj['type']
    except:
        raise InterpretationError('Predicate type must be specified.')

    try:
        predicate_generator = _PREDICATE_GENERATORS[predicate_type]
    except:
        raise InterpretationError(
            'Predicate type`{0}` is invalid'.format(predicate_type))

    args = obj.get('args', [])  # `args` is an optional value.
    if (
            isinstance(args, str) or
            isinstance(args, bytes) or
            not hasattr(args, '__iter__')
    ):
        args = [args]

    predicate = predicate_generator(args)  # Can raise InterpretationError.
    return predicate


def _interpret_one_type_pattern(obj):
    typename = obj['typename']
    predicate = interpret_predicate(obj['predicate'])
    return typename, predicate


def interpret_patterns(obj):
    """Interpret the type-pattern object."""
    return [_interpret_one_type_pattern(item) for item in obj]


class TypeInferrer(object):
    """Infers the type while reading items."""

    def __init__(self, patterns, null_value=_DEFAULT_NULL_VALUE):
        """Initialize."""
        self._iterator = iter(patterns)
        self._null_value = null_value

        try:
            self._current = next(self._iterator)
        except StopIteration:
            raise TypeInferenceError('Type pattern is empty.')

    def read_item(self, item):
        """Read `item` and consume type patterns
        while their predicates are not satisfied.
        When the value is NULL, not consume any pattern.
        """
        if item == self._null_value:
            return

        try:
            while not self._current[1](item):
                self._current = next(self._iterator)
        except StopIteration:
            raise TypeInferenceError(
                'Matching pattern is not found for: {0}'.format(item))

    @property
    def type_name(self):
        """Return the current type pattern."""
        return self._current[0]


class _Inference(object):
    def __init__(self, index, patterns, null_value):
        """Initialize."""
        self._index = int(index)
        self._key = operator.itemgetter(self._index)
        self._inferrer = TypeInferrer(patterns, null_value)

    def read_row(self, row):
        """Read a row."""
        item = self._key(row)
        self._inferrer.read_item(item)

    @property
    def index(self):
        """Return the index."""
        return self._index

    @property
    def type_name(self):
        """Return the type name."""
        return self._inferrer.type_name


def decide_types(patterns, reader, column_names, **kwargs):
    """Decide the types and returns the list of types.
    Given `null_value`, it is treated as NULL and type inference skips it.
    Given `index_types` as a list of (index, typename),
    the types of the specified columns will not be calculated
    and will be set the pre-defined type names.
    """
    null_value = kwargs.get('null_value', _DEFAULT_NULL_VALUE)
    index_types = kwargs.get('index_types', [])

    typename_maps = dict(
        (int(index), typename) for (index, typename) in index_types)

    inferences = [
        _Inference(index, patterns, null_value)
        for index in range(len(column_names))
        if index not in typename_maps.keys()]
    for row, inference in itertools.product(reader, inferences):
        inference.read_row(row)

    typename_maps.update(
        dict((item.index, item.type_name) for item in inferences)
    )

    type_names = [typename_maps[index] for index in range(len(column_names))]
    return type_names
