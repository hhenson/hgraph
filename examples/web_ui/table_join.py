import gc
import math
import os
import sys
from datetime import timedelta, datetime
from math import floor, sqrt
from random import random, sample, randint

from _socket import gethostname
from frozendict import frozendict
from frozendict.cool import deepfreeze

from hgraph import (
    graph,
    TimeSeriesSchema,
    TSB,
    TSD,
    evaluate_graph,
    GraphConfiguration,
    EvaluationMode,
    TS,
    EvaluationClock,
    feedback,
    compute_node,
    SCHEDULER,
    STATE,
    CompoundScalar,
    const,
    debug_print,
    map_,
    SIGNAL,
    schedule,
    combine,
    last_modified_time,
    convert,
    drop,
    nothing,
    default,
    TSS,
    collapse_keys,
    MIN_DT,
    take,
    collect,
)
from hgraph._operators._flow_control import merge
from hgraph.adaptors.perspective import (
    publish_table_editable,
    publish_table,
    register_perspective_adaptors,
    publish_multitable,
    TableEdits,
)
from hgraph.adaptors.perspective import perspective_web, PerspectiveTablesManager
from hgraph.debug import trace_controller
from hgraph.debug import inspector


class Readings(TimeSeriesSchema):
    value: TS[float]


class Events(TimeSeriesSchema):
    type: TS[str]
    magnitude: TS[int]


class Config(TimeSeriesSchema):
    initial: TS[float]
    randomness: TS[float]
    trend: TS[float]
    frequency: TS[int]


def refdata(count=15):
    return {
        (sensor := "".join(sample("ABCDEFHIGKLMNOPQRSTUVWXYZ", 4))): {
            "initial": random() * 100,
            "randomness": (random() * 40 + 10) / 100,
            "trend": (random() - 0.5) * 0.5,
            "frequency": randint(500, 2000),
        }
        for _ in range(count)
    }


class RandomDataState(CompoundScalar):
    value: float = math.nan


def round_time_up(t: datetime, to: timedelta):
    return (1 + (t - MIN_DT) // to) * to + MIN_DT


@compute_node(all_valid=("config",))
def random_values(
    config: TSB[Config],
    ec: EvaluationClock = None,
    sched: SCHEDULER = None,
    state: STATE[RandomDataState] = None,
) -> TSB[Readings]:
    data = {
        "value": (
            (prev := config.initial.value if state.value is math.nan else state.value)
            + (random() - 0.5) * (config.randomness.value * prev / sqrt(252))
            + config.trend.value / 252
        ),
    }

    state.value = data["value"]

    freq_ms = config.frequency.value
    sched.schedule(
        round_time_up(
            ec.now + timedelta(milliseconds=randint(freq_ms // 2, freq_ms + freq_ms // 2)), timedelta(milliseconds=50)
        ), tag=''
    )
    return data


@compute_node(all_valid=("config",))
def random_events(
    config: TSB[Config],
    ec: EvaluationClock = None,
    sched: SCHEDULER = None,
) -> TSB[Events]:
    data = {
        "type": ["pop", "buzz", "fizz", "bang"][randint(0, 3)],
        "magnitude": floor(randint(0, 100)) if randint(0, 100) > 95 else 0,
    }

    freq_ms = config.frequency.value
    sched.schedule(
        round_time_up(
            ec.now + timedelta(milliseconds=randint(freq_ms // 2, freq_ms + freq_ms // 2)), timedelta(milliseconds=50)
        ), tag=''
    )
    return data if data["magnitude"] else None


@graph
def host_web_server():
    register_perspective_adaptors()
    PerspectiveTablesManager.set_current(
        PerspectiveTablesManager(
            host_server_tables=False,
            table_config_file=[
                os.path.join(os.path.dirname(__file__), "table_join.json"),
                os.path.join(os.path.dirname(__file__), "total_table_join.json"),
            ],
        )
    )
    perspective_web(gethostname(), 8082, layouts_path=os.path.join(os.path.dirname(__file__), "layouts"))

    count = 5

    initial_config = const(deepfreeze(refdata(count)), TSD[str, TSB[Config]])
    config_updates = feedback(TSB[TableEdits[str, TSB[Config]]])
    debug_print("config updates", config_updates())
    config = merge(initial_config, config_updates().edits)
    config = config[config.key_set - default(config_updates().removes, const(frozenset(), TSS[str]))]
    config_updates(publish_table_editable("config", config, index_col_name="sensor", empty_row=True))
    # publish_table("config", initial_config, index_col_name="sensor")

    initial_data = map_(
        lambda key, c: map_(lambda c: random_values(c), __keys__=const(frozenset({"C", "F"}), TSS[str]), c=c),
        take(config, 1),
    )

    initial_data = collapse_keys(initial_data)
    publish_table("data", initial_data, index_col_name="sensor,units")

    initial_events = map_(
        lambda key, c: random_events(c),
        config,
    )
    event_updates = feedback(TSB[TableEdits[str, TSB[Events]]])
    debug_print("event updates", event_updates())
    events = merge(take(initial_events, timedelta(seconds=5)), event_updates().edits)
    # events = event_updates().edits
    events = events[events.key_set - default(event_updates().removes, const(frozenset(), TSS[str]))]
    event_updates(publish_table_editable("events", events, index_col_name="sensor", empty_row=True))

    other_updates = feedback(TSB[TableEdits[str, TS[str]]])
    debug_print("other updates", other_updates())
    # others = merge(others, other_updates().edits)
    others = other_updates().edits
    others = others[others.key_set - default(other_updates().removes, const(frozenset(), TSS[str]))]
    other_updates(publish_table_editable("others", others, index_col_name="sensor", empty_row=True))

    # data_updates = feedback(TSB[TableEdits[tuple[str, str], TSB[Readings]]])
    # debug_print("data updates", data_updates())
    # data = merge(initial_data, data_updates().edits)
    # data = data[data.key_set - default(data_updates().removes, const(frozenset(), TSS[tuple[str, str]]))]
    # data_updates(publish_table_editable("data", data, index_col_name="sensor,units", empty_row=True))

    trace_controller(port=8082)
    inspector(port=8082)


if __name__ == "__main__":
    gc.disable()
    print(f"pid={os.getpid()}")
    evaluate_graph(host_web_server, GraphConfiguration(run_mode=EvaluationMode.REAL_TIME, trace=False))
