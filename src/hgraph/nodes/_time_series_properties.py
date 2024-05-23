from hgraph import TIME_SERIES_TYPE, REF, TS, compute_node


__all__ = ("valid",)


@compute_node(valid=("ts",), active=('ts',))
def valid(ts: REF[TIME_SERIES_TYPE], ts_value: TIME_SERIES_TYPE = None) -> TS[bool]:
    if ts.modified:
        if ts_value.bound:
            ts_value.make_passive()
            ts_value.un_bind_output()

    if not ts.value.valid:
        return False

    if not ts_value.bound:
        ts.value.bind_input(ts_value)
        if ts_value.valid:
            return True

        ts_value.make_active()

    if ts_value.valid:
        ts_value.make_passive()
        return True

    return False
