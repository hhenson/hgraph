import hgraph as hg


@hg.graph
def _peer(key: hg.TS[int]) -> hg.TS[int]:
    return hg.mesh_("staged-dependency")[key]


@hg.graph
def _root(key: hg.TS[int], phase: hg.TS[int]) -> hg.TS[int]:
    return _peer(hg.const(1)) + _peer(hg.const(2))


@hg.graph
def _middle_initial(key: hg.TS[int], phase: hg.TS[int]) -> hg.TS[int]:
    return hg.const(10)


@hg.graph
def _middle_expanded(key: hg.TS[int], phase: hg.TS[int]) -> hg.TS[int]:
    return _peer(hg.const(11)) + _peer(hg.const(12))


@hg.graph
def _middle(key: hg.TS[int], phase: hg.TS[int]) -> hg.TS[int]:
    return hg.switch_(
        phase,
        {0: _middle_initial, hg.DEFAULT: _middle_expanded},
        key,
    )


@hg.graph
def _leaf(key: hg.TS[int], phase: hg.TS[int]) -> hg.TS[int]:
    return key + phase


@hg.graph
def _cascade(key: hg.TS[int], phase: hg.TS[int]) -> hg.TS[int]:
    return hg.switch_(
        key,
        {0: _root, 1: _middle, hg.DEFAULT: _leaf},
        phase,
    )


@hg.graph
def _app(keys: hg.TSS[int], phase: hg.TS[int]) -> hg.TSD[int, hg.TS[int]]:
    return hg.mesh_(
        _cascade,
        phase,
        __keys__=keys,
        __key_arg__="key",
        __name__="staged-dependency",
    )


def test_mesh_waits_for_a_paused_dependency_before_settling_its_dependent():
    assert hg.eval_node(
        _app,
        [hg.set_delta(added={0}, tp=int), None],
        [0, 1],
        __elide__=True,
    ) == [
        {0: 12, 1: 10, 2: 2},
        {0: 28, 1: 25, 2: 3, 11: 12, 12: 13},
    ]
