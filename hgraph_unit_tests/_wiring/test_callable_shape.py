import pytest

from hgraph import equal_lambdas

pytestmark = pytest.mark.smoke


def test_equal_lambdas_ignores_lambda_parameter_names():
    lhs = lambda x, y: x + y
    rhs = lambda a, b: a + b

    assert equal_lambdas(lhs, rhs)


def test_equal_lambdas_handles_lambdas_defined_on_the_same_line():
    lhs, rhs = (lambda x: x + 1), (lambda y: y + 1)

    assert equal_lambdas(lhs, rhs)


def test_equal_lambdas_preserves_parameter_order():
    lhs = lambda x, y: x + y
    rhs = lambda a, b: b + a

    assert not equal_lambdas(lhs, rhs)


def test_equal_lambdas_distinguishes_different_constants():
    lhs = lambda x: x + 1
    rhs = lambda y: y + 2

    assert not equal_lambdas(lhs, rhs)


def test_equal_lambdas_matches_attribute_and_item_access():
    lhs = lambda x: x.a[0]
    rhs = lambda y: y.a[0]

    assert equal_lambdas(lhs, rhs)


def test_equal_lambdas_distinguishes_different_attribute_names():
    lhs = lambda x: x.a[0]
    rhs = lambda y: y.b[0]

    assert not equal_lambdas(lhs, rhs)


def test_equal_lambdas_distinguishes_different_item_indices():
    lhs = lambda x: x.a[0]
    rhs = lambda y: y.a[1]

    assert not equal_lambdas(lhs, rhs)


def test_equal_lambdas_tracks_nested_lambda_scopes():
    lhs = lambda x: (lambda y: x + y)
    rhs = lambda a: (lambda b: a + b)

    assert equal_lambdas(lhs, rhs)


def test_equal_lambdas_distinguishes_different_nested_scope_usage():
    lhs = lambda x: (lambda y: x + y)
    rhs = lambda a: (lambda b: b + b)

    assert not equal_lambdas(lhs, rhs)


def test_equal_lambdas_distinguishes_different_closure_values():
    def make(delta: int):
        return lambda x: x + delta

    assert not equal_lambdas(make(1), make(2))


def test_equal_lambdas_tracks_nested_lambda_scopes_in_calls():
    lhs = lambda x: abs(lambda y: x + y)
    rhs = lambda a: abs(lambda b: a + b)

    assert equal_lambdas(lhs, rhs)


def test_equal_lambdas_tracks_nested_lambda_scopes_in_calls_not_equal():
    lhs = lambda x: abs(lambda y: x + y)
    rhs = lambda a: abs(lambda b: b + a)

    assert not equal_lambdas(lhs, rhs)


def test_equal_lambdas_matches_comprehension_shapes():
    lhs = lambda xs: [x + 1 for x in xs]
    rhs = lambda ys: [y + 1 for y in ys]

    assert equal_lambdas(lhs, rhs)


def test_equal_lambdas_distinguishes_comprehension_body_changes():
    lhs = lambda xs: [x + 1 for x in xs]
    rhs = lambda ys: [y + 2 for y in ys]

    assert not equal_lambdas(lhs, rhs)


def test_equal_lambdas_compares_default_values_without_parameter_names():
    lhs = lambda x=1, *, y=2: x + y
    rhs = lambda a=1, *, b=2: a + b

    assert equal_lambdas(lhs, rhs)


def test_equal_lambdas_distinguishes_different_default_values():
    lhs = lambda x=1, *, y=2: x + y
    rhs = lambda a=1, *, b=3: a + b

    assert not equal_lambdas(lhs, rhs)


def test_equal_lambdas_preserves_called_names():
    lhs = lambda x: abs(x)
    rhs = lambda y: round(y)

    assert not equal_lambdas(lhs, rhs)
