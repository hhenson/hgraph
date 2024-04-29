import inspect
from dataclasses import dataclass

import pytest

from hg_oap.utils.op import *
from hg_oap.utils.op import FailedOp, Expression


@dataclass
class A:
    x: int = 1

    def f(self, y):
        return self.x + y


a = ParameterOp(_name='a', _index=0)
_0 = ParameterOp(_index=0)
_1 = ParameterOp(_index=1)

@pytest.mark.parametrize(
    ('expr', 'repr_', 'args', 'kwargs', 'res'),
    (
        (a.x, 'a.x', [A()], {}, 1),
        (a.x, 'a.x', [], {'a':A()}, 1),
        (a.x, 'a.x', [A(x=2)], {}, 2),

        (a.f(1), 'a.f(1)', [A()], {}, 2),
        (a.f(_1), 'a.f(_1)', [A(), 2], {}, 3),

        (_0[1], '_0[1]', [[1, 2]], {}, 2),
        (_0[_1], '_0[_1]', [[1, 2], 1], {}, 2),
        (_0[1].f(2), '_0[1].f(2)', [[A(), A()]], {}, 3),

        (round(_0, 2), 'round(_0, 2)', [0.123], {}, 0.12),
        (round(_0, _1), 'round(_0, _1)', [0.123, 2], {}, 0.12),

        (1 < _0, '_0 > 1', [2], {}, True),
        (_0 < 1, '_0 < 1', [2], {}, False),
        (1 > _0, '_0 < 1', [2], {}, False),
        (_0 > 1, '_0 > 1', [2], {}, True),
        (1 <= _0, '_0 >= 1', [2], {}, True),
        (_0 <= 1, '_0 <= 1', [2], {}, False),
        (1 >= _0, '_0 <= 1', [2], {}, False),
        (_0 >= 1, '_0 >= 1', [2], {}, True),

        (1 < _0 < 3, '1 < _0 < 3', [1], {}, False),
        (1 < _0 < 3, '1 < _0 < 3', [2], {}, True),
        (1 < _0 < 3, '1 < _0 < 3', [3], {}, False),

        (_0 + 1, '_0 + 1', [1], {}, 2),
        (1 + _0, '1 + _0', [1], {}, 2),
        (_0 + _1, '_0 + _1', [1, 2], {}, 3),

        (_0 - 1, '_0 - 1', [1], {}, 0),
        (1 - _0, '1 - _0', [1], {}, 0),
        (_0 - _1, '_0 - _1', [1, 2], {}, -1),

        (_0 * 2, '_0 * 2', [1], {}, 2),
        (2 * _0, '2 * _0', [1], {}, 2),
        (_0 * _1, '_0 * _1', [1, 2], {}, 2),

        (_0 / 2, '_0 / 2', [1], {}, 0.5),
        (2 / _0, '2 / _0', [1], {}, 2.0),
        (_0 / _1, '_0 / _1', [1, 2], {}, 0.5),

        (_0 // 2, '_0 // 2', [1], {}, 0),
        (2 // _0, '2 // _0', [1], {}, 2),
        (_0 // _1, '_0 // _1', [1, 2], {}, 0),

        (_0 % 2, '_0 % 2', [1], {}, 1),
        (2 % _0, '2 % _0', [1], {}, 0),
        (_0 % _1, '_0 % _1', [1, 2], {}, 1),

        (_0 ** 2, '_0 ** 2', [1], {}, 1),
        (2 ** _0, '2 ** _0', [1], {}, 2),
        (_0 ** _1, '_0 ** _1', [1, 2], {}, 1),

        (_0 << 2, '_0 << 2', [1], {}, 4),
        (2 << _0, '2 << _0', [1], {}, 4),
        (_0 << _1, '_0 << _1', [1, 2], {}, 4),

        (_0 >> 2, '_0 >> 2', [4], {}, 1),
        (2 >> _0, '2 >> _0', [1], {}, 1),
        (_0 >> _1, '_0 >> _1', [2, 1], {}, 1),

        (_0 & 2, '_0 & 2', [2], {}, 2),
        (2 & _0, '2 & _0', [2], {}, 2),
        (_0 & _1, '_0 & _1', [2, 2], {}, 2),

        (_0 ^ 2, '_0 ^ 2', [2], {}, 0),
        (2 ^ _0, '2 ^ _0', [2], {}, 0),
        (_0 ^ _1, '_0 ^ _1', [2, 2], {}, 0),

        (_0 | 2, '_0 | 2', [2], {}, 2),
        (2 | _0, '2 | _0', [2], {}, 2),
        (_0 | _1, '_0 | _1', [2, 2], {}, 2),

        (+_0, '+_0', [2], {}, 2),
        (-_0, '-_0', [2], {}, -2),
        (~_0, '~_0', [2], {}, ~2),

        (abs(_0), 'abs(_0)', [-1], {}, 1),

        (1 + _0 * 2, '1 + _0 * 2', [1], {}, 3),
        (_1 + _0 * 2, '_1 + _0 * 2', [1, 2], {}, 4),
        ((1 + _0) * 2, '(1 + _0) * 2', [1], {}, 4),
        ((_1 + _0) * 2, '(_1 + _0) * 2', [1, 2], {}, 6),

        (_0 + 1 < 2, '_0 + 1 < 2', [1], {}, False),
        (1 + _0 < 2, '1 + _0 < 2', [1], {}, False),

        (1 + abs(_0 + _1), '1 + abs(_0 + _1)', [-1, 0.1], {}, 1.9),
    ))
def test_magic(expr, repr_, args, kwargs, res):
    assert repr(expr) == repr_
    assert calc(expr, *args, **kwargs) == res


@pytest.mark.xfail(raises=FailedOp)
@pytest.mark.parametrize(
    ('expr', 'args', 'kwargs'),
    (
            (_1, [0], {}),
            (a, [], {}),
    )
)
def test_magic_errors(expr, args, kwargs):
    calc(expr, *args, **kwargs)


def test_magic_list_comprehension():
    str_ = lazy(str)

    expr = [i for i in lazy(range)(_0)]
    assert calc(expr, 4) == [0, 1, 2, 3]

    expr = [1, 2, *[i for i in lazy(range)(2)]]
    assert calc(expr) == [1, 2, 0, 1]

    expr = [1, 2, [i for i in lazy(range)(2)]]
    assert calc(expr) == [1, 2, [0, 1]]

    expr = [str_(i) + str_(k) for i in _0 for j in _1 for k in j]
    assert calc(expr, [0, 1], [[0], [1]]) == ['00', '01', '10', '11']


def test_magic_dict_comprehension():
    expr = {k: i for i in _0 for k in i}
    assert calc(expr, [[0], [1]]) == {0: [0], 1: [1]}

    expr1 = {i[0]: i[1] for i in _0.items()}
    assert calc(expr1, {1: "1", 2: "2", 3: "3"}) == {1: "1", 2: "2", 3: "3"}

    expr2 = {0: '0', **expr1}
    assert calc(expr2, {1: "1", 2: "2", 3: "3"}) == {0: "0", 1: "1", 2: "2", 3: "3"}


def test_expression():
    expr = Expression(_1 + _0)
    s = inspect.signature(expr)
    assert len(s.parameters) == 2
    assert list(s.parameters.keys()) == ['i0', 'i1']
    assert s.parameters['i0'].name == 'i0'
    assert s.parameters['i0'].kind == inspect.Parameter.POSITIONAL_OR_KEYWORD
    assert s.parameters['i1'].name == 'i1'
    assert s.parameters['i1'].kind == inspect.Parameter.POSITIONAL_OR_KEYWORD
    assert expr(1, 2) == 3

    _key = ParameterOp(_name='key')
    expr1 = Expression(_key + 1)
    s = inspect.signature(expr1)
    assert len(s.parameters) == 1
    assert list(s.parameters.keys()) == ['key']
    assert s.parameters['key'].name == 'key'
    assert s.parameters['key'].kind == inspect.Parameter.KEYWORD_ONLY
    assert expr1(key=1) == 2
