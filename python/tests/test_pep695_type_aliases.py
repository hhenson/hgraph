"""PEP 695 ``type X = ...`` aliases in hgraph annotations.

A ``TypeAliasType`` is a distinct runtime object rather than the type it
names, so an annotation carrying one reached the type machinery as an unknown
object. Two entry points interpret annotations — ``_value_type`` for scalars
and ``_pattern_of`` for time-series — and each failed differently:

* ``TS[Price]`` raised ``unsupported scalar type for hgraph: Price`` at
  decoration;
* ``PriceTS`` (an alias naming a time-series) *decorated fine* and then raised
  ``not a time-series annotation: PriceTS`` when the node was called — the
  worse failure, because the definition looked accepted.

Both now resolve through ``resolve_type_alias``.
"""

from dataclasses import dataclass


import hgraph as hg

type Price = float
type Sym = str
type PriceTS = hg.TS[float]
type Chained = Price
type Pair[T] = tuple[T, T]
type Mapping2[K, V] = dict[K, V]


def test_an_alias_names_a_scalar_type():
    @hg.compute_node
    def node(ts: hg.TS[Price]) -> hg.TS[float]:
        return ts.value

    assert hg.eval_node(node, [1.5]) == [1.5]


def test_an_alias_names_a_time_series_type():
    """This one used to decorate successfully and fail at call time."""

    @hg.compute_node
    def node(ts: PriceTS) -> hg.TS[float]:
        return ts.value

    assert hg.eval_node(node, [2.5]) == [2.5]


def test_aliases_resolve_transitively():
    @hg.compute_node
    def node(ts: hg.TS[Chained]) -> hg.TS[float]:
        return ts.value

    assert hg.eval_node(node, [3.5]) == [3.5]


def test_a_parameterised_alias_keeps_its_arguments():
    """The trap in this feature.

    ``Pair[int]`` is a ``GenericAlias`` whose ``__origin__`` is the alias, and
    it *also* proxies ``__value__`` to the alias body ``tuple[T, T]``. Reading
    ``__value__`` therefore looks like it resolves the alias while silently
    discarding the ``int`` — leaving a type still parameterised by a free
    variable. Summing the pair is what catches that: it only type-checks and
    evaluates if the substitution actually happened.
    """

    @hg.compute_node
    def node(ts: hg.TS[Pair[int]]) -> hg.TS[int]:
        return ts.value[0] + ts.value[1]

    assert hg.eval_node(node, [(2, 3)]) == [5]


def test_a_multi_parameter_alias_substitutes_in_order():
    @hg.compute_node
    def node(ts: hg.TS[Mapping2[str, int]]) -> hg.TS[int]:
        return sum(ts.value.values())

    assert hg.eval_node(node, [{"a": 1, "b": 2}]) == [3]


def test_an_alias_works_as_a_plain_scalar_argument():
    @hg.compute_node
    def node(ts: hg.TS[float], mult: Price) -> hg.TS[float]:
        return ts.value * mult

    assert hg.eval_node(node, [2.0], 3.0) == [6.0]


def test_an_alias_works_as_a_tsd_key():
    @hg.compute_node
    def node(tsd: hg.TSD[Sym, hg.TS[float]]) -> hg.TS[int]:
        return len(tsd.value)

    assert hg.eval_node(node, [{"a": 1.0}]) == [1]


def test_an_alias_works_inside_a_compound_scalar():
    @dataclass(frozen=True)
    class Quote(hg.CompoundScalar):
        sym: Sym
        px: Price

    @hg.compute_node
    def node(ts: hg.TS[Quote]) -> hg.TS[float]:
        return ts.value.px

    assert hg.eval_node(node, [Quote("a", 4.0)]) == [4.0]


def test_an_alias_works_inside_a_time_series_schema():
    class Bundle(hg.TimeSeriesSchema):
        px: hg.TS[Price]

    @hg.compute_node
    def node(tsb: hg.TSB[Bundle]) -> hg.TS[float]:
        return tsb.px.value

    assert hg.eval_node(node, [{"px": 5.0}]) == [5.0]


def test_a_pep695_generic_function_resolves_per_call():
    @hg.compute_node
    def node[T: hg.SCALAR](ts: hg.TS[T]) -> hg.TS[T]:
        return ts.value

    assert hg.eval_node(node, [7]) == [7]
    assert hg.eval_node(node, ["a"]) == ["a"]


def test_resolution_is_a_no_op_for_everything_else():
    """Called on every annotation, so it must not disturb ordinary types."""
    from hgraph._types import resolve_type_alias

    for annotation in (int, float, str, hg.TS[int], tuple[int, str], None):
        assert resolve_type_alias(annotation) is annotation


def test_a_deep_alias_chain_resolves():
    """The resolver walks a chain rather than unwrapping once, so a chain
    deeper than one link has to arrive at the underlying type."""
    from hgraph._types import resolve_type_alias

    type L1 = int
    type L2 = L1
    type L3 = L2
    type L4 = L3

    assert resolve_type_alias(L4) is int

    @hg.compute_node
    def node(ts: hg.TS[L4]) -> hg.TS[int]:
        return ts.value

    assert hg.eval_node(node, [9]) == [9]
