"""Correctness guards for workloads timed by ``benchmarks/scenarios.py``."""

from collections import Counter
import importlib.util
from pathlib import Path

import hgraph as hg
from hgraph import Size, TS, TSD, TSL, compute_node, graph, mesh_, reduce
from hgraph.test import eval_node


_SCENARIOS_PATH = Path(__file__).parents[2] / "benchmarks" / "scenarios.py"
_SPEC = importlib.util.spec_from_file_location("hgraph_benchmark_scenarios", _SCENARIOS_PATH)
bench = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(bench)


def _published(values):
    return [value for value in values if value is not None]


def test_service_pulse_leaves_a_cycle_for_next_cycle_forwarding():
    @graph
    def app(trigger: TS[bool]) -> TS[int]:
        return bench._service_int_pulse(3)

    assert eval_node(
        app,
        [True],
        __end_time__=hg.MIN_ST + 7 * hg.MIN_TD,
    ) == [None, 0, None, 1, None, 2]


def test_benchmark_construct_graph_keeps_each_width_branch_distinct():
    @graph
    def std_app(value: TS[int]) -> TS[int]:
        return bench._wide_chain_std(value, width=3, depth=2)

    @graph
    def py_app(value: TS[int]) -> TS[int]:
        return bench._wide_chain_py(value, width=3, depth=2)

    assert eval_node(std_app, [1, 2]) == [12, 15]
    assert eval_node(py_app, [1, 2]) == [12, 15]


def _request_reply_graph(implementation, path):
    @graph
    def app(value: TS[int]) -> TS[int]:
        hg.register_service(path, implementation)
        return bench._benchmark_request_reply(value, path=path)

    return app


def test_benchmark_request_reply_implementations_process_every_request():
    inputs = [1, None, 2, None, 3]
    assert _published(eval_node(
        _request_reply_graph(bench._benchmark_request_reply_std_impl, "bench_rr_std"),
        inputs,
        __end_time__=hg.MIN_ST + 8 * hg.MIN_TD,
    )) == [4, 6, 8]
    assert _published(eval_node(
        _request_reply_graph(bench._benchmark_request_reply_py_impl, "bench_rr_py"),
        inputs,
        __end_time__=hg.MIN_ST + 8 * hg.MIN_TD,
    )) == [4, 6, 8]


def _subscription_graph(implementation, path):
    @graph
    def app(key: TS[str]) -> TS[int]:
        hg.register_service(path, implementation)
        return bench._benchmark_subscription(key, path=path)

    return app


def test_benchmark_subscription_implementations_process_every_key_change():
    inputs = ["a", None, "bb", None, "ccc"]

    assert _published(eval_node(
        _subscription_graph(bench._benchmark_subscription_std_impl, "bench_sub_std"),
        inputs,
        __end_time__=hg.MIN_ST + 8 * hg.MIN_TD,
    )) == [1, 1, 1]
    assert _published(eval_node(
        _subscription_graph(bench._benchmark_subscription_py_impl, "bench_sub_py"),
        inputs,
        __end_time__=hg.MIN_ST + 8 * hg.MIN_TD,
    )) == [1, 2, 3]


def _service_adaptor_graph(implementation, path):
    @graph
    def app(value: TS[int]) -> TS[int]:
        hg.register_adaptor(path, implementation)
        return bench._benchmark_service_adaptor(value, path=path)

    return app


def test_benchmark_service_adaptor_implementations_process_every_request():
    inputs = [1, None, 2, None, 3]
    assert _published(eval_node(
        _service_adaptor_graph(bench._benchmark_service_adaptor_std_impl, "bench_sa_std"),
        inputs,
        __end_time__=hg.MIN_ST + 8 * hg.MIN_TD,
    )) == [4, 6, 8]
    assert _published(eval_node(
        _service_adaptor_graph(bench._benchmark_service_adaptor_py_impl, "bench_sa_py"),
        inputs,
        __end_time__=hg.MIN_ST + 8 * hg.MIN_TD,
    )) == [4, 6, 8]


def test_benchmark_four_client_request_reply_reaches_every_map_child():
    seen = []

    @compute_node
    def observe(value: TS[int]) -> TS[int]:
        seen.append(value.value)
        return value.value

    @hg.service_impl(interfaces=bench._benchmark_request_reply)
    def implementation(request: TSD[int, TS[int]], path: str) -> TSD[int, TS[int]]:
        return hg.map_(observe, request)

    @graph
    def app(value: TS[int]) -> TS[int]:
        path = "bench_rr_four_clients"
        hg.register_service(path, implementation)
        replies = [bench._benchmark_request_reply(value, path=path) for _ in range(4)]
        return replies[0] + replies[1] + replies[2] + replies[3]

    assert _published(eval_node(
        app,
        [1, None, 2, None, 3],
        __end_time__=hg.MIN_ST + 8 * hg.MIN_TD,
    )) == [4, 8, 12]
    assert Counter(seen) == Counter({1: 4, 2: 4, 3: 4})


def test_benchmark_four_client_service_adaptor_reaches_every_map_child():
    seen = []

    @compute_node
    def observe(value: TS[int]) -> TS[int]:
        seen.append(value.value)
        return value.value

    @hg.service_adaptor_impl(interfaces=bench._benchmark_service_adaptor)
    def implementation(request: TSD[int, TS[int]], path: str) -> TSD[int, TS[int]]:
        return hg.map_(observe, request)

    @graph
    def app(value: TS[int]) -> TS[int]:
        path = "bench_sa_four_clients"
        hg.register_adaptor(path, implementation)
        replies = [
            bench._benchmark_service_adaptor(value + client, path=path)
            for client in range(4)
        ]
        return replies[0] + replies[1] + replies[2] + replies[3]

    assert _published(eval_node(
        app,
        [1, None, 2, None, 3],
        __end_time__=hg.MIN_ST + 8 * hg.MIN_TD,
    )) == [10, 14, 18]
    assert Counter(seen) == Counter({1: 1, 2: 2, 3: 3, 4: 3, 5: 2, 6: 1})


def test_benchmark_duplex_adaptor_variants_forward_every_tick():
    def adaptor_graph(implementation, path):
        @graph
        def app(value: TS[int]) -> TS[int]:
            hg.register_adaptor(path, implementation)
            return bench._benchmark_adaptor(value, path=path)

        return app

    assert eval_node(
        adaptor_graph(bench._benchmark_adaptor_std_impl, "bench_adaptor_std"),
        [1, 2, 3],
    ) == [2, 3, 4]
    assert eval_node(
        adaptor_graph(bench._benchmark_adaptor_py_impl, "bench_adaptor_py"),
        [1, 2, 3],
    ) == [2, 3, 4]


def _source_graph(factory):
    @graph
    def app(trigger: TS[bool]) -> TS[int]:
        return factory()

    return app


def test_benchmark_dense_map_reduce_processes_every_key_and_cycle():
    expected = [0, 20, 28, 36]

    assert eval_node(
        _source_graph(lambda: bench._map_reduce_std(bench._tsd_dense_pulse(3, 4))),
        [True],
        __end_time__=hg.MIN_ST + 5 * hg.MIN_TD,
    ) == expected
    assert eval_node(
        _source_graph(lambda: bench._map_reduce_py(bench._tsd_dense_pulse(3, 4))),
        [True],
        __end_time__=hg.MIN_ST + 5 * hg.MIN_TD,
    ) == expected


def test_benchmark_dense_map_invokes_every_key_on_every_cycle():
    seen = []

    @compute_node
    def observe(value: TS[int]) -> TS[int]:
        seen.append(value.value)
        return value.value

    @graph
    def mapped(tsd: TSD[int, TS[int]]) -> TS[int]:
        return reduce(hg.add_, hg.map_(observe, tsd), 0)

    assert eval_node(
        _source_graph(lambda: mapped(bench._tsd_dense_pulse(3, 4))),
        [True],
        __end_time__=hg.MIN_ST + 5 * hg.MIN_TD,
    ) == [0, 6, 10, 14]
    assert Counter(seen) == Counter({0: 1, 1: 2, 2: 3, 3: 3, 4: 2, 5: 1})


def test_benchmark_sparse_map_reduce_processes_sparse_updates():
    assert eval_node(
        _source_graph(lambda: bench._map_reduce_std(bench._tsd_sparse_pulse(4, 8, 2))),
        [True],
        __end_time__=hg.MIN_ST + 6 * hg.MIN_TD,
    ) == [0, 16, 20, 28, 40]


def test_benchmark_churn_map_reduce_processes_adds_and_removes():
    assert eval_node(
        _source_graph(lambda: bench._map_reduce_std(bench._tsd_churn_pulse(4, 4, 1))),
        [True],
        __end_time__=hg.MIN_ST + 6 * hg.MIN_TD,
    ) == [0, 20, 22, 24, 26]


def test_benchmark_churn_map_invokes_initial_and_new_children():
    seen = []

    @compute_node
    def observe(value: TS[int]) -> TS[int]:
        seen.append(value.value)
        return value.value

    @graph
    def mapped(tsd: TSD[int, TS[int]]) -> TS[int]:
        return reduce(hg.add_, hg.map_(observe, tsd), 0)

    assert eval_node(
        _source_graph(lambda: mapped(bench._tsd_churn_pulse(4, 4, 1))),
        [True],
        __end_time__=hg.MIN_ST + 6 * hg.MIN_TD,
    ) == [0, 6, 7, 8, 9]
    assert Counter(seen) == Counter({0: 1, 1: 2, 2: 2, 3: 2})


def test_benchmark_mesh_processes_dependencies_and_key_churn():
    def meshed():
        source = bench._tsd_churn_pulse(4, 8, 2)
        return reduce(hg.add_, mesh_(bench._mesh_cell, source), 0)

    actual = eval_node(
        _source_graph(meshed),
        [True],
        __end_time__=hg.MIN_ST + 6 * hg.MIN_TD,
    )
    # hg-cpp settles the dependency chain within a cycle; upstream publishes
    # its intermediate settlement. Both traces cover the same churn workload.
    assert actual in (
        [0, 84, 87, 97, 118],
        [0, 28, 85, 81, 68, 83],
    )


def test_benchmark_scheduler_conflates_many_input_notifications():
    calls = []

    @compute_node
    def observe(values: TSL[TS[int], Size[4]]) -> TS[int]:
        calls.append(tuple(value.value for value in values.values()))
        return sum(value.value for value in values.values())

    @graph
    def app(value: TS[int]) -> TS[int]:
        return observe(TSL.from_ts(*(value + offset for offset in range(4))))

    assert eval_node(app, [1, 2, 3]) == [10, 14, 18]
    assert calls == [(1, 2, 3, 4), (2, 3, 4, 5), (3, 4, 5, 6)]


def test_benchmark_python_sink_receives_every_generator_tick():
    seen = []

    @hg.sink_node
    def collect(value: TS[int]):
        seen.append(value.value)

    @graph
    def app(trigger: TS[bool]) -> TS[bool]:
        collect(bench._int_pulse(3))
        return trigger

    assert eval_node(
        app,
        [True],
        __end_time__=hg.MIN_ST + 5 * hg.MIN_TD,
    ) == [True]
    assert seen == [0, 1, 2]


def test_benchmark_tsd_capacity_growth_and_clear_repopulate_do_real_work():
    assert eval_node(
        _source_graph(
            lambda: bench._map_reduce_std(bench._tsd_growing_pulse(3, 2))
        ),
        [True],
        __end_time__=hg.MIN_ST + 5 * hg.MIN_TD,
    ) == [0, 4, 12, 24]
    assert eval_node(
        _source_graph(
            lambda: bench._map_reduce_std(
                bench._tsd_clear_repopulate_pulse(4, 2)
            )
        ),
        [True],
        __end_time__=hg.MIN_ST + 6 * hg.MIN_TD,
    ) == [0, 6, 0, 22, 0]


def test_benchmark_tsd_explicit_keys_follow_key_lifecycle():
    @graph
    def keyed_total(values: TSD[int, TS[int]]) -> TS[int]:
        keyed = hg.map_(bench._key_identity, __keys__=values.key_set)
        return reduce(hg.add_, keyed, 0)

    assert eval_node(
        _source_graph(
            lambda: keyed_total(bench._tsd_churn_pulse(4, 4, 1))
        ),
        [True],
        __end_time__=hg.MIN_ST + 6 * hg.MIN_TD,
    ) == [0, 6, 10, 14, 18]


def test_benchmark_tsd_remove_and_recreate_reuses_key_slots():
    assert eval_node(
        _source_graph(
            lambda: bench._map_reduce_std(
                bench._tsd_reactivate_pulse(4, 2, 1)
            )
        ),
        [True],
        __end_time__=hg.MIN_ST + 6 * hg.MIN_TD,
    ) == [0, 6, 4, 10, 4]


def test_benchmark_tsd_union_map_tracks_both_memberships():
    @graph
    def app(lhs: TSD[int, TS[int]], rhs: TSD[int, TS[int]]) -> TS[int]:
        combined = hg.map_(bench._sum_optional, lhs, rhs)
        return reduce(hg.add_, combined, 0)

    assert eval_node(
        app,
        [{0: 1}, {1: 2}, {0: hg.REMOVE}],
        [{1: 10}, {2: 20}, {1: hg.REMOVE}],
    ) == [11, 33, 22]


def test_benchmark_tsd_intersection_map_tracks_shared_membership():
    @graph
    def app(lhs: TSD[int, TS[int]], rhs: TSD[int, TS[int]]) -> TS[int]:
        combined = hg.map_(
            bench._sum_optional,
            lhs,
            rhs,
            __keys__=lhs.key_set & rhs.key_set,
        )
        return reduce(hg.add_, combined, 0)

    assert eval_node(
        app,
        [{0: 1}, {1: 2}, {0: hg.REMOVE}],
        [{1: 10}, {2: 20}, {1: hg.REMOVE}],
    ) == [0, 12, 0]


def test_benchmark_reduce_combiner_variants_produce_the_same_values():
    @graph
    def graph_combiner(values: TSD[int, TS[int]]) -> TS[int]:
        return reduce(bench._add_graph, values, 0)

    @graph
    def python_combiner(values: TSD[int, TS[int]]) -> TS[int]:
        return reduce(bench._add_py, values, 0)

    inputs = [{0: 1, 1: 2}, {0: 3}, {1: hg.REMOVE}]
    assert eval_node(graph_combiner, inputs) == [3, 5, 3]
    assert eval_node(python_combiner, inputs) == [3, 5, 3]


def test_benchmark_ordered_fixed_tsl_reduce_preserves_left_to_right_order():
    @graph
    def app(values: TSL[TS[int], Size[4]]) -> TS[int]:
        return reduce(
            bench._subtract_graph,
            values,
            hg.const(0, tp=TS[int]),
            is_associative=False,
        )

    assert eval_node(
        app,
        [{0: 1, 1: 2, 2: 3, 3: 4}, {0: 10}],
    ) == [-8, 1]


def test_benchmark_switch_alternates_different_sized_branches():
    @graph
    def app(selector: TS[bool], value: TS[int]) -> TS[int]:
        return hg.switch_(
            selector,
            {False: bench._switch_short, True: bench._switch_deep},
            value=value,
        )

    assert eval_node(app, [False, True, False], [1, 2, 3]) == [2, 14, 4]


def test_benchmark_switch_forwards_keyed_branch_lifecycle():
    @graph
    def app(
        selector: TS[bool], values: TSD[int, TS[int]]
    ) -> TS[int]:
        selected = hg.switch_(
            selector,
            {
                False: bench._switch_tsd_identity,
                True: bench._switch_tsd_double,
            },
            values=values,
        )
        return reduce(hg.add_, selected, 0)

    assert eval_node(
        app,
        [False, True, False],
        [{0: 1, 1: 2}, {0: 3}, {1: hg.REMOVE}],
    ) == [3, 14, 3]


def test_benchmark_bundle_and_window_workloads_publish_updates():
    @graph
    def bundle_app(value: TS[int]) -> TS[int]:
        bundle = hg.TSB[bench.BenchBundle].from_ts(
            fast=value,
            medium=value + 1,
            slow=value + 2,
        )
        return bundle.fast + bundle.medium + bundle.slow

    @graph
    def window_app(value: TS[int]) -> hg.TSW[int]:
        return hg.to_window(value, 3, 1)

    assert eval_node(bundle_app, [1, 2, 3]) == [6, 9, 12]
    assert eval_node(window_app, [1, 2, 3, 4]) == [1, 2, 3, 4]


def test_benchmark_set_churn_keeps_the_requested_live_size():
    @graph
    def app(trigger: TS[bool]) -> TS[int]:
        return hg.len_(bench._tss_churn_pulse(4, 4, 1))

    actual = eval_node(
        app,
        [True],
        __end_time__=hg.MIN_ST + 6 * hg.MIN_TD,
    )
    # hg_cpp deduplicates the unchanged length while upstream republishes
    # it, and a never-valid input stays silent until the first delta
    # (upstream parity, issue #116 family); both traces prove that every
    # add/remove delta preserved four live keys.
    assert actual in ([None, 4], [None, 4, 4, 4, 4])


def test_hg_cpp_benchmark_reduce_without_zero_tracks_cardinality():
    @graph
    def app(values: TSD[int, TS[int]]) -> TS[int]:
        return reduce(bench._add_graph, values)

    assert eval_node(
        app,
        [{}, {0: 4}, {1: 6}, {0: hg.REMOVE}, {1: hg.REMOVE}],
    ) == [None, 4, 10, 6, None]


def test_hg_cpp_benchmark_dynamic_tsl_map_reduce_processes_sparse_updates():
    @graph
    def app(trigger: TS[bool]) -> TS[int]:
        values = bench._dynamic_tsl_sparse_pulse(3, 4, 1)
        return reduce(hg.add_, hg.map_(bench._mapped_std, values), 0)

    assert eval_node(
        app,
        [True],
        __end_time__=hg.MIN_ST + 5 * hg.MIN_TD,
    ) == [0, 20, 28, 36]


def test_benchmark_wiring_scale_workloads_compute_their_totals():
    # keys 0..3 through two layers of the keyed cell (layer offsets 0 and 1
    # on the > 10 branch): 0, 14, 26, 38.
    assert _published(eval_node(
        _source_graph(lambda: bench._layered_map_py(bench._tsd_churn_pulse(1, 4, 0), 2)),
        [True],
        __end_time__=hg.MIN_ST + 3 * hg.MIN_TD,
    ))[-1] == 78
    # site s carries leaf s % 4; overload index multiplies by s + 1: 0 + 2 + 6 + 12.
    assert eval_node(_source_graph(lambda: bench._dispatch_sites(4, 4)), [True])[-1] == 20
    assert eval_node(_source_graph(lambda: bench._overload_sites(4, 4)), [True])[-1] == 20
