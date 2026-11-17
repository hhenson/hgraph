"""``TS[Optional[T]]`` (RFC 0050): a time series whose tick may be a null.

A node returns ``Null`` to tick a value that is unset, ``None`` to not tick,
and a payload to tick a value that holds it. A reader sees ``None`` as the
``value`` of a tick whose value is unset, with the input valid, and ``Null``
as its ``delta_value``.
"""

import pickle
from typing import Optional

import pytest

import hgraph as hg
from hgraph import CompoundScalar, Null, TS, compute_node, eval_node, graph


@compute_node
def cleared(command: TS[int]) -> TS[Optional[int]]:
    if command.value == 0:
        return Null  # a tick whose value is null
    if command.value < 0:
        return None  # no tick, as on every output
    return command.value  # a tick that holds the value


def test_a_node_ticks_a_payload_a_null_or_nothing():
    assert eval_node(cleared, [5, 0, -1, 7, 0, 0]) == [5, Null, None, 7, Null, Null]


def test_none_and_null_are_two_spellings_in_a_vector():
    @compute_node
    def passed(value: TS[Optional[int]]) -> TS[Optional[int]]:
        return value.delta_value

    # ``None`` is no tick and ``Null`` a tick whose value is unset, on the
    # way in and on the way out.
    assert eval_node(passed, [5, Null, None, 7]) == [5, Null, None, 7]


def test_value_is_none_and_delta_is_null_for_a_tick_that_is_unset():
    @compute_node
    def seen(value: TS[Optional[int]]) -> TS[str]:
        delta = value.delta_value
        return (
            f"valid={value.valid} modified={value.modified} "
            f"value={value.value!r} delta={'Null' if delta is Null else repr(delta)}"
        )

    assert eval_node(seen, [5, Null, None, Null]) == [
        "valid=True modified=True value=5 delta=5",
        "valid=True modified=True value=None delta=Null",
        None,
        "valid=True modified=True value=None delta=Null",
    ]


def test_a_tick_that_is_unset_stays_valid_while_another_input_ticks():
    @compute_node
    def held(value: TS[Optional[int]], other: TS[int]) -> TS[str]:
        delta = value.delta_value
        return f"{value.valid} {value.value!r} {'Null' if delta is Null else repr(delta)} {other.value}"

    # The second cycle is driven by ``other`` alone: the unset value is
    # still the value, and it is not modified, so its delta is ``None``.
    assert eval_node(held, [Null, None, 3], [1, 2, 3]) == [
        "True None Null 1",
        "True None None 2",
        "True 3 3 3",
    ]


def test_the_pipe_spelling_is_the_same_type():
    assert TS[int | None] == TS[Optional[int]]
    assert TS[Optional[int]] != TS[int]

    @compute_node
    def cleared_pipe(command: TS[int]) -> TS[int | None]:
        return Null if command.value == 0 else command.value

    assert eval_node(cleared_pipe, [5, 0]) == [5, Null]


def test_an_output_takes_null_as_its_value_and_none_invalidates():
    @compute_node
    def assigned(command: TS[int], _output: hg.TS_OUT[Optional[int]] = None) -> TS[Optional[int]]:
        if command.value == 0:
            _output.value = Null
        elif command.value > 0:
            _output.value = command.value

    assert eval_node(assigned, [5, 0, -1, 7]) == [5, Null, None, 7]


def test_null_is_refused_by_an_output_that_is_no_optional():
    @compute_node
    def wrong(command: TS[int]) -> TS[int]:
        return Null

    with pytest.raises(Exception) as raised:
        eval_node(wrong, [1])
    chain = []
    error = raised.value
    while error is not None:
        chain.append(error)
        error = error.__cause__
    assert any(isinstance(error, TypeError) and "Null" in str(error) for error in chain), chain

    @compute_node
    def wrong_collection(command: TS[int]) -> hg.TSS[int]:
        return Null

    with pytest.raises(Exception) as raised:
        eval_node(wrong_collection, [1])
    chain = []
    error = raised.value
    while error is not None:
        chain.append(error)
        error = error.__cause__
    assert any(isinstance(error, TypeError) and "Null" in str(error) for error in chain), chain


def test_a_payload_of_another_type_is_refused():
    @compute_node
    def wrong(command: TS[int]) -> TS[Optional[int]]:
        return "text"

    with pytest.raises(Exception):
        eval_node(wrong, [1])


def test_a_payload_may_be_a_compound_scalar():
    class Quote(CompoundScalar):
        bid: float
        ask: Optional[float] = None

    @compute_node
    def quoted(command: TS[int]) -> TS[Optional[Quote]]:
        if command.value == 0:
            return Null
        return Quote(bid=float(command.value))

    result = eval_node(quoted, [2, 0])
    assert result[0] == Quote(bid=2.0)
    assert result[1] is Null


def test_an_optional_field_of_a_compound_scalar_keeps_its_reading():
    class Order(CompoundScalar):
        price: float
        note: Optional[str] = None

    @compute_node
    def note_of(order: TS[Order]) -> TS[str]:
        return "none" if order.value.note is None else order.value.note

    # A field that may be unset is not an Optional value: it reads ``None``
    # when unset, and no ``Null`` is involved.
    assert eval_node(note_of, [Order(price=1.0), Order(price=2.0, note="x")]) == ["none", "x"]


def test_null_is_one_falsy_instance():
    assert Null is type(Null)()
    assert not Null
    assert repr(Null) == "Null"
    assert Null is not None
    assert pickle.loads(pickle.dumps(Null)) is Null
    assert hg.Null is Null


def test_a_graph_passes_an_optional_series_between_nodes():
    @compute_node
    def or_minus_one(value: TS[Optional[int]]) -> TS[int]:
        return -1 if value.value is None else value.value

    @graph
    def app(command: TS[int]) -> TS[int]:
        return or_minus_one(cleared(command))

    assert eval_node(app, [5, 0, -1, 7]) == [5, -1, None, 7]


def test_an_alias_that_names_an_optional_is_one():
    type MaybeInt = int | None
    type Maybe[T] = T | None

    assert TS[MaybeInt] == TS[Optional[int]]
    assert TS[Maybe[int]] == TS[Optional[int]]

    @compute_node
    def cleared_alias(command: TS[int]) -> TS[MaybeInt]:
        return Null if command.value == 0 else command.value

    assert eval_node(cleared_alias, [5, 0]) == [5, Null]


def test_a_sampled_rebind_reads_its_delta_in_the_delta_form():
    # ``switch_`` rebinds its output to another branch's series. A branch
    # that was already valid is sampled: its current value is the delta of
    # the cycle it is switched to, and for a value that is unset that
    # delta is ``Null``, as it is for a tick of its own.
    @compute_node
    def seen(value: TS[Optional[int]]) -> TS[str]:
        delta = value.delta_value
        return f"{value.value!r} {'Null' if delta is Null else repr(delta)}"

    @graph
    def app(select: TS[bool], command: TS[int], other: TS[int]) -> TS[str]:
        chosen = hg.if_then_else(select, cleared(command), cleared(other))
        return seen(chosen)

    # The branch switched to holds a payload, and then one that is unset.
    result = eval_node(app, [True, None, False], [0, None, None], [7, None, None])
    assert result[0] == "None Null"
    assert result[2] == "7 7"
    result = eval_node(app, [True, None, False], [5, None, None], [0, None, None])
    assert result[0] == "5 5"
    assert result[2] == "None Null"


def _shown(delta) -> str:
    """A delta, or a value read as one, with ``Null`` spelled out."""
    if delta is Null:
        return "Null"
    if isinstance(delta, dict):
        return "{" + ", ".join(f"{key}: {_shown(item)}" for key, item in sorted(delta.items())) + "}"
    if isinstance(delta, (tuple, list)):
        return "(" + ", ".join(_shown(item) for item in delta) + ")"
    return repr(delta)


def test_a_sampled_list_in_a_dictionary_keeps_an_unset_child_as_null():
    # A sampled rebind gives an aggregate's current value where its delta
    # is asked for. Each child is given in the form a delta of it has, all
    # the way down, so a child whose value is unset reads ``Null`` and not
    # ``None``, which an output would take for "this child did not tick".
    Pairs = hg.TSD[str, hg.TSL[TS[Optional[int]], hg.Size[2]]]

    @compute_node
    def keyed(command: TS[int]) -> Pairs:
        return {"x": (Null if command.value == 0 else command.value, 7)}

    @compute_node
    def seen(value: Pairs) -> TS[str]:
        return _shown(value.delta_value)

    @graph
    def app(select: TS[bool], a: TS[int], b: TS[int]) -> TS[str]:
        return seen(hg.if_then_else(select, keyed(a), keyed(b)))

    # The dictionary switched to was valid already, and holds the unset child.
    result = eval_node(app, [True, None, False], [5, None, None], [0, None, None])
    assert "Null" in result[2] and "7" in result[2]
    assert "None" not in result[2]


def test_a_sampled_bundle_in_a_dictionary_keeps_an_unset_child_as_null():
    class Pair(hg.TimeSeriesSchema):
        left: TS[Optional[int]]
        right: TS[Optional[int]]

    Pairs = hg.TSD[str, hg.TSB[Pair]]

    @compute_node
    def keyed(command: TS[int]) -> Pairs:
        return {"x": {"left": Null if command.value == 0 else command.value, "right": 7}}

    @compute_node
    def seen(value: Pairs) -> TS[str]:
        return _shown(value.delta_value)

    @graph
    def app(select: TS[bool], a: TS[int], b: TS[int]) -> TS[str]:
        return seen(hg.if_then_else(select, keyed(a), keyed(b)))

    result = eval_node(app, [True, None, False], [5, None, None], [0, None, None])
    assert "Null" in result[2] and "7" in result[2]
    assert "None" not in result[2]


def test_a_sampled_dictionary_keeps_an_unset_child_as_null():
    @compute_node
    def keyed(command: TS[int]) -> hg.TSD[str, TS[Optional[int]]]:
        return {"x": Null if command.value == 0 else command.value}

    @compute_node
    def seen(value: hg.TSD[str, TS[Optional[int]]]) -> TS[str]:
        return _shown(value.delta_value)

    @graph
    def app(select: TS[bool], a: TS[int], b: TS[int]) -> TS[str]:
        return seen(hg.if_then_else(select, keyed(a), keyed(b)))

    result = eval_node(app, [True, None, False], [0, None, None], [7, None, None])
    assert result[0] == "{x: Null}"
    assert "7" in result[2]
    # And the other way: the dictionary switched to holds the unset child.
    result = eval_node(app, [True, None, False], [7, None, None], [0, None, None])
    assert "Null" in result[2] and "None" not in result[2]
