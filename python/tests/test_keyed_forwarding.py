"""Keyed forwarding paths preserve the producer and its cycle delta."""

import _hgraph
import pytest

import hgraph as hg
from hgraph._wiring._core import _unwrap
from hgraph._wiring._graph import _as_wired
from hgraph.test import eval_node


Book = hg.TSD[int, hg.TS[int]]
Keys = hg.TSS[int]


def _switch(selector, branches, output, *inputs):
    cases = _hgraph.switch_cases(
        {key: _as_wired(branch) for key, branch in branches.items()},
        key_type=_unwrap(selector).ts_type,
    )
    return hg.wire("switch_", selector, cases, *inputs, __output_type__=output)


@hg.graph
def _left(a: Book, b: Book) -> Book:
    return a


@hg.graph
def _right(a: Book, b: Book) -> Book:
    return b


@hg.compute_node(active=("choice",), valid=("choice",))
def _choose_book(choice: hg.TS[int], a: hg.REF[Book], b: hg.REF[Book]) -> hg.REF[Book]:
    if choice.value == 0:
        return a.value
    if choice.value == 1:
        return b.value
    return hg.TimeSeriesReference.make()


@hg.graph
def _book_mux(choice: hg.TS[int], a: Book, b: Book) -> Book:
    return _switch(choice, {0: _left, 1: _right}, Book, a, b)


def test_three_switches_sample_overlap_and_report_the_prior_items():
    observations = []

    @hg.sink_node(valid=())
    def observe(value: Book):
        observations.append((
            dict(value.value),
            dict(value.delta_value),
            set(value.added_keys()),
            {key: child.value for key, child in value.removed_items()},
            set(value.key_set.delta_value) if value.key_set.modified else set(),
        ))

    @hg.graph
    def run(choice: hg.TS[int], a: Book, b: Book):
        inner = _book_mux(choice, a, b)
        middle = _book_mux(hg.const(0), inner, b)
        observe(_book_mux(hg.const(0), middle, b))

    eval_node(run, [0, 1], [{1: 10, 2: 20}], [{2: 21, 3: 30}])
    assert observations == [
        ({1: 10, 2: 20}, {1: 10, 2: 20}, {1, 2}, {}, {1, 2}),
        ({2: 21, 3: 30}, {1: hg.REMOVE, 2: 21, 3: 30}, {3}, {1: 10}, {hg.Removed(1), 3}),
    ]


@pytest.mark.parametrize("outer_move", [None, 1, 2])
def test_dictionary_withdrawal_retains_prior_keys_when_outer_moves_in_the_same_cycle(outer_move):
    observations = []

    @hg.sink_node(valid=())
    def observe(value: Book):
        if value.modified:
            observations.append((value.valid, dict(value.delta_value),
                                 {key: item.value for key, item in value.removed_items()}))

    @hg.graph
    def run(inner: hg.TS[int], outer: hg.TS[int], a: Book, never: Book):
        forwarded = _book_mux(hg.const(0), _choose_book(inner, a, never), never)
        observe(_choose_book(outer, forwarded, never))

    eval_node(run, [0, 2], [0, outer_move], [{1: 10, 2: 20}], [None])
    assert observations == [
        (True, {1: 10, 2: 20}, {}),
        (False, {1: hg.REMOVE, 2: hg.REMOVE}, {1: 10, 2: 20}),
    ]


@hg.compute_node(active=("keep",), valid=("keep",))
def _choose_set(keep: hg.TS[bool], source: hg.REF[Keys]) -> hg.REF[Keys]:
    return source.value if keep.value else hg.TimeSeriesReference.make()


@hg.graph
def _pass_set(source: Keys) -> Keys:
    return source


def test_set_delta_is_visible_after_an_inner_link_withdraws():
    observations = []

    @hg.sink_node(valid=())
    def observe(values: Keys):
        if values.modified:
            observations.append((values.valid, set(values.added()), set(values.removed()), set(values.delta_value)))

    @hg.graph
    def run(keep: hg.TS[bool], source: Keys):
        moving = _choose_set(keep, source)
        outer = _switch(hg.const(0), {0: _pass_set}, Keys, moving)
        observe(outer)

    eval_node(run, [True, False], [{1, 2}])
    assert observations == [
        (True, {1, 2}, set(), {1, 2}),
        (False, set(), {1, 2}, {hg.Removed(1), hg.Removed(2)}),
    ]


def test_round_trip_restores_the_dictionary_and_key_set_clocks():
    observations = []

    @hg.sink_node(active=("clock",), valid=("clock",))
    def observe(clock: hg.TS[int], value: Book):
        observations.append((value.modified, value.key_set.modified, dict(value.value)))

    @hg.graph
    def run(inner: hg.TS[int], outer: hg.TS[int], a: Book, b: Book, clock: hg.TS[int]):
        observe(clock, _book_mux(outer, _book_mux(inner, a, b), a))

    eval_node(run, [0, 1], [0, 1], [{1: 10}, None, {3: 30}], [{2: 20}], [0, 1, 2])
    assert observations == [
        (True, True, {1: 10}),
        (False, False, {1: 10}),
        (True, True, {1: 10, 3: 30}),
    ]


def test_set_link_follows_a_dictionary_key_set_on_move_and_withdrawal():
    @hg.graph
    def run(choice: hg.TS[int], a: Book, b: Book) -> Keys:
        dictionary = _book_mux(hg.const(0), _choose_book(choice, a, b), b)
        return _switch(hg.const(0), {0: _pass_set}, Keys, hg.keys_(dictionary))

    assert eval_node(run, [0, 1, 2], [{1: 10, 2: 20}], [{2: 21, 3: 30}]) == [
        hg.set_delta(added={1, 2}, tp=int),
        hg.set_delta(added={3}, removed={1}, tp=int),
        hg.set_delta(removed={2, 3}, tp=int),
    ]


@hg.graph
def _publish_book(a: Book, b: Book) -> Book:
    return hg.pass_through_node(a)


@hg.graph
def _forwarding_reference(a: Book, b: Book) -> hg.REF[Book]:
    forwarded = hg.try_except(_publish_book, a, b).out
    return _choose_book(hg.const(0), forwarded, b)


@hg.graph
def _right_reference(a: Book, b: Book) -> hg.REF[Book]:
    return _choose_book(hg.const(1), a, b)


@hg.graph
def _branch_reference(choice: hg.TS[int], a: Book, b: Book) -> hg.REF[Book]:
    return _switch(choice, {0: _forwarding_reference, 1: _right_reference}, hg.REF[Book], a, b)


def test_saved_reference_to_a_stopped_forwarding_branch_expires():
    @hg.compute_node(active=("save",), valid=("save",))
    def save_reference(save: hg.TS[bool], selected: hg.REF[Book]) -> hg.REF[Book]:
        if save.value:
            return selected.value

    observations = []

    @hg.sink_node(active=("clock",), valid=("clock",))
    def observe(clock: hg.TS[int], saved: Book, selected: Book):
        observations.append((dict(saved.value) if saved.valid else None, dict(selected.value)))

    @hg.graph
    def run(choice: hg.TS[int], a: Book, b: Book, save: hg.TS[bool], clock: hg.TS[int]):
        selected = _branch_reference(choice, a, b)
        observe(clock, save_reference(save, selected), selected)

    eval_node(run, [0, 1], [{1: 10}], [{2: 20}], [True], [0, 1, 2])
    assert observations == [
        ({1: 10}, {1: 10}),
        ({1: 10}, {2: 20}),
        (None, {2: 20}),
    ]


def test_saved_dictionary_reference_can_be_overwritten_after_expiry():
    @hg.graph
    def owned_left(a: Book, b: Book) -> hg.REF[Book]:
        owned = hg.pass_through_node(a)
        return _choose_book(hg.const(0), owned, b)

    @hg.graph
    def owned_right(a: Book, b: Book) -> hg.REF[Book]:
        owned = hg.pass_through_node(b)
        return _choose_book(hg.const(0), owned, a)

    @hg.compute_node(active=("save",), valid=("save",))
    def save_reference(save: hg.TS[bool], selected: hg.REF[Book]) -> hg.REF[Book]:
        if save.value:
            return selected.value

    observations = []

    @hg.sink_node(active=("clock",), valid=("clock",))
    def observe(clock: hg.TS[int], saved: Book):
        observations.append(dict(saved.value) if saved.valid else None)

    @hg.graph
    def run(choice: hg.TS[int], a: Book, b: Book, save: hg.TS[bool], clock: hg.TS[int]):
        selected = _switch(choice, {0: owned_left, 1: owned_right}, hg.REF[Book], a, b)
        observe(clock, save_reference(save, selected))

    eval_node(run, [0, 1, None, None], [{1: 10}], [{2: 20}], [True, False, False, True], [0, 1, 2, 3])
    assert observations == [{1: 10}, {1: 10}, None, {2: 20}]


def test_saved_set_reference_can_be_overwritten_after_expiry():
    @hg.compute_node(active=("trigger",), valid=("trigger",))
    def take_reference(trigger: hg.TS[bool], item: hg.REF[Keys]) -> hg.REF[Keys]:
        return item.value

    @hg.graph
    def owned_left(a: Keys, b: Keys) -> hg.REF[Keys]:
        return take_reference(hg.const(True), hg.pass_through_node(a))

    @hg.graph
    def owned_right(a: Keys, b: Keys) -> hg.REF[Keys]:
        return take_reference(hg.const(True), hg.pass_through_node(b))

    @hg.compute_node(active=("save",), valid=("save",))
    def save_reference(save: hg.TS[bool], selected: hg.REF[Keys]) -> hg.REF[Keys]:
        if save.value:
            return selected.value

    @hg.graph
    def run(choice: hg.TS[int], a: Keys, b: Keys, save: hg.TS[bool]) -> Keys:
        selected = _switch(choice, {0: owned_left, 1: owned_right}, hg.REF[Keys], a, b)
        return save_reference(save, selected)

    assert eval_node(run, [0, 1, None, None], [{1}], [{2}], [True, False, False, True]) == [
        hg.set_delta(added={1}, tp=int), None, None, hg.set_delta(added={2}, tp=int),
    ]


def test_map_can_retire_a_forwarding_endpoint_before_a_higher_rank_retarget():
    @hg.graph
    def branch(item: Book) -> hg.REF[Book]:
        return _forwarding_reference(item, item)

    @hg.compute_node(valid=("key",))
    def select(items: hg.TSD[int, hg.REF[Book]], key: hg.TS[int]) -> hg.REF[Book]:
        return items[key.value].value if key.value in items else hg.TimeSeriesReference.make()

    @hg.graph
    def run(items: hg.TSD[int, Book], key: hg.TS[int]) -> Book:
        return select(hg.map_(branch, items), key)

    assert eval_node(run, [{0: {1: 10, 2: 20}, 1: {2: 21, 3: 30}}, {0: hg.REMOVE}], [0, 1]) == [
        {1: 10, 2: 20}, {1: hg.REMOVE, 2: 21, 3: 30},
    ]


def test_key_lookup_wakes_when_an_upstream_forwarding_dictionary_withdraws():
    @hg.compute_node(valid=())
    def present(item: hg.REF[hg.TS[int]]) -> hg.TS[bool]:
        return not item.value.is_empty

    @hg.graph
    def run(choice: hg.TS[int], a: Book, b: Book) -> hg.TS[bool]:
        followed = _book_mux(hg.const(0), _choose_book(choice, a, b), b)
        return present(followed[1])

    assert eval_node(run, [0, 2], [{1: 10}], [None]) == [True, False]


def test_first_binding_does_not_report_the_new_producers_old_key():
    @hg.graph
    def run(choice: hg.TS[int], a: Book, b: Book) -> Book:
        return _choose_book(choice, a, b)

    assert eval_node(run, [None, 0], [{1: 10, 2: 20}, {1: hg.REMOVE}], [None]) == [None, {2: 20}]


def test_list_dictionary_distinguishes_membership_and_value_removal_on_retarget():
    Row = hg.TSL[hg.TS[int], hg.Size[2]]
    Rows = hg.TSD[int, Row]
    observations = []

    @hg.compute_node
    def source(step: hg.TS[int], _output: Rows = None) -> Rows:
        if step.value == 0:
            _output.get_or_create(1)[0].value = 10
            _output.get_or_create(2)

    @hg.compute_node(active=("choose",), valid=("choose",))
    def select(choose: hg.TS[bool], a: hg.REF[Rows], b: hg.REF[Rows]) -> hg.REF[Rows]:
        return b.value if choose.value else a.value

    @hg.sink_node(valid=())
    def observe(value: Rows):
        observations.append((
            set(value.removed_keys()),
            {key for key, _ in value.removed_items()},
            set(value.key_set.delta_value),
        ))

    @hg.graph
    def run(step: hg.TS[int], choose: hg.TS[bool], b: Rows):
        observe(select(choose, source(step), b))

    eval_node(run, [0], [False, True], [{5: {0: 50}}])
    assert observations[-1] == ({1, 2}, {1}, {hg.Removed(1), hg.Removed(2), 5})


@hg.graph
def _count_recovered_mesh_keys(key: hg.TS[int]) -> hg.TS[int]:
    return hg.len_(hg.get_mesh("recovered_keys").key_set)


@hg.compute_node
def _mesh_key_changes(step: hg.TS[int]) -> Keys:
    if step.value == 0:
        return {1, 2}
    return hg.set_delta(added={3, 4}, removed={1}, tp=int)


@hg.component(recordable_id="keyed-follower-restore")
def _checkpointed_keyed_followers(step: hg.TS[int]) -> Book:
    return hg.mesh_(_count_recovered_mesh_keys, __name__="recovered_keys", __keys__=_mesh_key_changes(step))


def test_restored_key_set_followers_receive_the_first_membership_change(tmp_path):
    persistence = pytest.importorskip("hgraph_persistence")
    store = persistence.ComponentCheckpointStore(tmp_path)
    next_cycle = hg.MIN_ST + hg.MIN_TD
    with hg.GlobalState() as state:
        persistence.configure_component_recovery(store, "keyed-follower-restore", "initial", global_state=state)
        assert eval_node(_checkpointed_keyed_followers, [0], __end_time__=next_cycle) == [{1: 2, 2: 2}]

    with hg.GlobalState() as state:
        persistence.configure_component_recovery(
            store, "keyed-follower-restore", "resumed", "initial", global_state=state
        )
        assert eval_node(
            _checkpointed_keyed_followers, [None, 1],
            __start_time__=next_cycle, __end_time__=next_cycle + 2 * hg.MIN_TD,
        ) == [None, {1: hg.REMOVE, 2: 3, 3: 3, 4: 3}]
