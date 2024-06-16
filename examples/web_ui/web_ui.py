import os
import sys
from datetime import timedelta
from math import floor, sqrt
from random import random, sample, randint

from _socket import gethostname
from frozendict.cool import deepfreeze

from hgraph import graph, TimeSeriesSchema, TSB, TSD, run_graph, EvaluationMode, TS, EvaluationClock, \
    feedback, compute_node, SCHEDULER, TSL, STATE, REMOVE_IF_EXISTS, CompoundScalar
from hgraph.nodes import const, debug_print, delay
from hgraph._operators._flow_control import merge
from hgraph.adaptors.perspective._perspective import perspective_web
from hgraph.adaptors.perspective._perspetive_publish import publish_table


class Readings(TimeSeriesSchema):
    sensor: TS[str]
    value: TS[float]
    events: TS[int]


class Config(TimeSeriesSchema):
    initial: TS[float]
    randomness: TS[float]
    trend: TS[float]


def refdata():
    return {(sensor := ''.join(sample("ABCDEFHIGKLMNOPQRSTUVWXYZ", 4))): {
                                                                    'initial': (random() * 100),
                                                                    'randomness': (random() * 40 + 10) / 100,
                                                                    'trend': (random() - 0.5) * 0.5,
                                                                }
        for _ in range(15)
    }


class RandomDataState(CompoundScalar):
    value: dict[str, float] = {}


@compute_node
def random_data(config: TSD[str, TSB[Config]], freq_ms: int = 1000, ec: EvaluationClock = None, sched: SCHEDULER = None, state: STATE[RandomDataState] = None) -> TSD[str, TSB[Readings]]:
    sensors = list(k for k, v in config.items() if v.all_valid)

    data = {t: {
        'value': (prev := state.value.get(t, config[t].initial.value)) + (random() - 0.5) * (config[t].randomness.value * prev / sqrt(252)) + config[t].trend.value / 252,
        'events': floor(randint(0, 100)) if randint(0, 100) > 95 else 0}
    for t in sample(sensors, k=randint(1, len(sensors)))}

    state.value.update({t: d['value'] for t, d in data.items()})
    sched.schedule(ec.now + timedelta(milliseconds=randint(freq_ms // 2, freq_ms + freq_ms // 2)))
    return data


@graph
def host_web_server():
    perspective_web(gethostname(), 8080, layouts_path=os.path.join(os.path.dirname(__file__), 'layouts'))

    initial_config = const(deepfreeze(refdata()), TSD[str, TSB[Config]])
    config_updates = feedback(TSD[str, TSB[Config]])
    debug_print('config updates', config_updates())
    config = merge(initial_config, config_updates())
    config_updates(publish_table('config', config, editable=True, index_col_name='sensor'))

    publish_table('data', random_data(config, 100), index_col_name='sensor', history=sys.maxsize)


if __name__ == '__main__':
    print(f"pid={os.getpid()}")
    run_graph(host_web_server, run_mode=EvaluationMode.REAL_TIME, __trace__=False)
