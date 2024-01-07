from hgraph import compute_node, contains_, REF, TSS, SCALAR, TS, STATE, PythonTimeSeriesReference


@compute_node(overloads=contains_)
def tss_contains(ts: REF[TSS[SCALAR]], item: TS[SCALAR], _state: STATE = None) \
        -> REF[TS[bool]]:
    """Perform a time-series contains check of an item in the given time-series set"""
    # If the tss is set then we should de-register the old contains.
    if _state.tss is not None:
        _state.tss.release_contains_ref(_state.item, _state.requester)
    _state.tss = ts.value.output
    _state.item = item.value
    return PythonTimeSeriesReference(
        None if _state.tss is None else _state.tss.get_contains_ref(_state.item, _state.requester))


@tss_contains.start
def _tss_contains_start(_state: STATE):
    _state.requester = object()
    _state.tss = None
    _state.item = None
