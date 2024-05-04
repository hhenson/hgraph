from hgraph import TSS, SIZE, TSL, compute_node, TSS_OUT, PythonSetDelta, TIME_SERIES_TYPE, TS, KEYABLE_SCALAR, \
    union_tsl, is_empty, SCALAR

__all__ = tuple()


@compute_node(valid=tuple(), overloads=union_tsl)
def _union_tsl(tsl: TSL[TSS[KEYABLE_SCALAR], SIZE], _output: TSS_OUT[KEYABLE_SCALAR] = None) -> TSS[KEYABLE_SCALAR]:
    tss: TSS[KEYABLE_SCALAR, SIZE]
    to_add: set[KEYABLE_SCALAR] = set()
    to_remove: set[KEYABLE_SCALAR] = set()
    for tss in tsl.modified_values():
        to_add |= tss.added()
        to_remove |= tss.removed()
    if (disputed:=to_add.intersection(to_remove)):
        # These items are marked for addition and removal, so at least some set is hoping to add these items.
        # Thus, overall these are an add, unless they are already added.
        new_items = disputed.intersection(_output.value)
        to_remove -= new_items
    to_remove &= _output.value  # Only remove items that are already in the output.
    if to_remove:
        # Now we need to make sure there are no items that may be duplicated in other inputs.
        for tss in tsl.valid_values():
            to_remove -= to_remove.intersection(tss.value)  # Remove items that exist in an input
            if not to_remove:
                break
    return PythonSetDelta(to_add, to_remove)


@compute_node(overloads=is_empty)
def is_empty_ts(ts: TS[SCALAR]) -> TS[bool]:
    return len(ts.value) == 0
