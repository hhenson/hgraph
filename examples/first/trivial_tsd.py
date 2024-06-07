from hgraph import TS, TSD, graph, map_
from hgraph.nodes import log
from hgraph.test import eval_node


@graph
def trivial_tsd(ts: TSD[str, TS[int]]):
    result = map_(lambda x: x + 1, ts).reduce(lambda x, y: x * y, 1)
    log('result {}', result)


if __name__ == '__main__':
    eval_node(trivial_tsd, [{'one': 1}, {'two': 2}], __trace__={'start': False, 'stop': False, 'eval': False})
