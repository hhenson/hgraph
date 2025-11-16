import gc
import math
import os
import sys
from datetime import timedelta, datetime
from math import floor, sqrt
from random import random, sample, randint

from _socket import gethostname
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
    collect,
    TSS,
    default,
    MIN_DT,
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
    sensor: TS[str]
    value: TS[float]
    events: TS[int]


class Config(TimeSeriesSchema):
    initial: TS[float]
    randomness: TS[float]
    trend: TS[float]


def refdata():
    return {
        (sensor := "".join(sample("ABCDEFHIGKLMNOPQRSTUVWXYZ", 4))): {
            "initial": random() * 100,
            "randomness": (random() * 40 + 10) / 100,
            "trend": (random() - 0.5) * 0.5,
        }
        for _ in range(150)
    }


class RandomDataState(CompoundScalar):
    value: float = math.nan


def round_time_up(t: datetime, to: timedelta):
    return (1 + (t - MIN_DT) // to) * to + MIN_DT


@compute_node(all_valid=("config",))
def random_values(
    config: TSB[Config],
    freq_ms: int = 1000,
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
    sched.schedule(
        round_time_up(
            ec.now + timedelta(milliseconds=randint(freq_ms // 2, freq_ms + freq_ms // 2)), timedelta(milliseconds=50)
        ), tag=''
    )
    return data


@compute_node(all_valid=("config",))
def random_events(
    config: TSB[Config],
    freq_ms: int = 1000,
    ec: EvaluationClock = None,
    sched: SCHEDULER = None,
) -> TSB[Readings]:
    data = {
        "events": floor(randint(0, 100)) if randint(0, 100) > 95 else 0,
    }

    sched.schedule(
        round_time_up(
            ec.now + timedelta(milliseconds=randint(freq_ms // 2, freq_ms + freq_ms // 2)), timedelta(milliseconds=50)
        ), tag=''
    )
    return data


@graph
def host_web_server():
    register_perspective_adaptors()
    PerspectiveTablesManager.set_current(PerspectiveTablesManager(host_server_tables=False))
    perspective_web(gethostname(), 8082, layouts_path=os.path.join(os.path.dirname(__file__), "layouts"))

    initial_config = const(deepfreeze(refdata()), TSD[str, TSB[Config]])
    config_updates = feedback(TSB[TableEdits[str, TSB[Config]]])
    debug_print("config updates", config_updates())
    config = merge(initial_config, config_updates().edits)
    config = config[config.key_set - default(config_updates().removes, const(frozenset(), TSS[str]))]
    config_updates(publish_table_editable("config", config, index_col_name="sensor", empty_row=True))

    map_(
        lambda key, c: publish_multitable(
            "data", key, random_values(c, 10000), unique=False, index_col_name="sensor", history=sys.maxsize
        ),
        config,
    )

    map_(
        lambda key, c: publish_multitable(
            "data", key, random_events(c, 10000), unique=False, index_col_name="sensor", history=sys.maxsize
        ),
        config,
    )

    engine_ticks = drop(schedule(timedelta(milliseconds=10000)), 100)
    publish_table(
        "engine_lag",
        convert[TSD]("lag", (wall_clock_time(engine_ticks) - last_modified_time(engine_ticks)).total_seconds),
        index_col_name="measure",
        history=sys.maxsize,
    )

    trace_controller(port=8082)
    inspector(port=8082)


@compute_node
def wall_clock_time(ts: SIGNAL) -> TS[datetime]:
    return datetime.utcnow()


if __name__ == "__main__":
    gc.disable()
    print(f"pid={os.getpid()}")
    evaluate_graph(host_web_server, GraphConfiguration(run_mode=EvaluationMode.REAL_TIME, trace=False))
