"""Reasoned model of the TSD set operators (written before scoring).

Rules (runtime spec + the 2026-07-17/2026-09-15 no-change ruling):
 R1  An invalid (never ticked) operand is nil, not empty (TS-2). union accumulates
     over the valid operands (empty is its identity, and its contract is to
     gather); intersection, difference and symmetric difference publish nothing
     until every operand is valid.
 R2  The key set is derived: union, intersection, lhs-minus-rhs, keys held by an
     odd number of operands (n-ary symmetric difference = left fold).
 R3  Each output child FORWARDS one source child: the only operand holding the
     key (difference/xor), lhs (intersection), for union the operand whose child
     ticked most recently (a same-cycle tie goes to the leftmost). A tick of the
     selected source child ticks the output child, equal value or not.
 R4  When a key enters the result, or its selected source changes because an
     operand lost the key, the output child takes the new source's value; that
     is derived, so an equal value is not re-published (no-change ruling) --
     except that a new source whose child ticked this cycle is forwarded (R3).
 R5  The first evaluation that satisfies R1 validates the output: it ticks even
     with an empty result. Afterwards an evaluation whose delta nets to no change
     does not tick.
"""
import json


def _decode(tick):
    if tick is None:
        return None
    return {k: ("REMOVE" if isinstance(v, dict) and v.get("$remove") else v) for k, v in tick.items()}


class Operand:
    def __init__(self):
        self.valid = False
        self.value = {}
        self.time = {}
        self.modified = set()

    def apply(self, tick, t):
        self.modified = set()
        if tick is None:
            return
        self.valid = True
        for k, v in tick.items():
            if v == "REMOVE":
                self.value.pop(k, None); self.time.pop(k, None)
            else:
                self.value[k] = v; self.time[k] = t; self.modified.add(k)


def keys_for(op, ops):
    sets = [set(o.value) for o in ops]
    if op in ("union", "bit_or"):
        return set().union(*sets)
    if op in ("intersection", "bit_and"):
        out = sets[0]
        for s in sets[1:]:
            out = out & s
        return out
    if op in ("difference", "sub_"):
        return sets[0] - sets[1]
    if op in ("symmetric_difference", "bit_xor"):
        out = sets[0]
        for s in sets[1:]:
            out = out ^ s
        return out
    raise ValueError(op)


def select(op, k, ops, t):
    holders = [i for i, o in enumerate(ops) if k in o.value]
    if op in ("intersection", "bit_and", "difference", "sub_"):
        return 0
    if op in ("symmetric_difference", "bit_xor"):
        return holders[0] if len(holders) == 1 else max(holders) if holders else None
    # union: latest tick, ties leftmost
    return min(holders, key=lambda i: (-ops[i].time[k], i))


def run(op, inputs):
    names = sorted(inputs)
    n = max(len(v) for v in inputs.values())
    ops = [Operand() for _ in names]
    out_valid = False
    out = {}
    src = {}
    trace = []
    for t in range(n):
        for name, o in zip(names, ops):
            ticks = inputs[name]
            o.apply(_decode(ticks[t]) if t < len(ticks) else None, t)
        gated = any(o.valid for o in ops) if op in ("union", "bit_or") else all(o.valid for o in ops)
        if not gated:
            trace.append(None)
            continue
        live = [o if o.valid else Operand() for o in ops]
        new_keys = keys_for(op, live)
        delta = {}
        for k in list(out):
            if k not in new_keys:
                delta[k] = {"$remove": True}
                del out[k]; src.pop(k, None)
        for k in new_keys:
            i = select(op, k, live, t)
            source = live[i]
            ticked = k in source.modified
            if ticked:
                out[k] = source.value[k]; delta[k] = source.value[k]
            elif k not in out or src.get(k) != i:
                if out.get(k, object()) != source.value[k]:
                    out[k] = source.value[k]; delta[k] = source.value[k]
            src[k] = i
        if not out_valid:
            out_valid = True
            trace.append({"$map": sorted([[k, v] for k, v in delta.items()])})
        elif delta:
            trace.append({"$map": sorted([[k, v] for k, v in delta.items()])})
        else:
            trace.append(None)
    if all(x is None for x in trace):
        return None
    return trace
