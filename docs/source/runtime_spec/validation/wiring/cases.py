"""Wiring cases, run unchanged against each runtime.

``python cases.py <case>`` wires and runs one case in the interpreter it is
started with and prints its observations as one JSON object. The same file
runs on Python hgraph 0.5.41 and on the C++ runtime's Python surface; it uses
only public wiring API. See ``README.md`` and ``../../cases_wiring.md``.

An observation is a type (normalised: whitespace removed), a trace of values,
or whether the call raised. Nothing here reads an expectation.
"""

from __future__ import annotations

import json
import re
import sys

import hgraph as hg
from hgraph import (
    REF,
    SIZE,
    TIME_SERIES_TYPE,
    TS,
    TSB,
    TSD,
    TSL,
    Size,
    TimeSeriesSchema,
    compute_node,
    graph,
)
from hgraph.test import eval_node

try:  # 0.5.41 keeps tsl_to_tsd in hgraph.nodes; the C++ surface exports both.
    from hgraph.nodes import tsl_to_tsd
except ImportError:  # pragma: no cover
    from hgraph import tsl_to_tsd


def _type(port) -> str:
    return re.sub(r"\s", "", str(port.output_type))


def _items(value) -> str:
    """A TSD value as sorted ``key=value`` text; a reference shows as ``<ref>``."""
    parts = []
    for key, item in sorted(dict(value).items()):
        parts.append(f"{key}={item!r}" if isinstance(item, (int, float, str)) else f"{key}=<ref>")
    return ",".join(parts)


# --- nodes shared by the cases -------------------------------------------------


@compute_node
def _identity(ts: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    return ts.delta_value


@compute_node
def _show_tsd(ts: TIME_SERIES_TYPE) -> TS[str]:
    return _items(ts.value)


@compute_node
def _show(ts: TIME_SERIES_TYPE) -> TS[str]:
    value = ts.value
    return repr(value) if isinstance(value, (int, float, str)) else "<ref>"


@compute_node
def _to_ref(ts: REF[TS[int]]) -> REF[TS[int]]:
    return ts.value


@compute_node
def _holds_reference(ts: REF[TIME_SERIES_TYPE]) -> TS[bool]:
    value = ts.value
    return bool(value.has_output) and not bool(value.is_empty)


@compute_node
def _ref_identity(ts: REF[TIME_SERIES_TYPE]) -> REF[TIME_SERIES_TYPE]:
    return ts.value


# --- cases ------------------------------------------------------------------------


def generic_depth():
    """WIR-7, WIR-8: a generic binds the dereferenced type at every depth."""
    seen = {}

    @graph
    def g(tsl: TSL[TS[int], Size[3]], keys: tuple[str, ...]) -> TS[str]:
        routed = tsl_to_tsd(tsl, keys)
        seen["source"] = _type(routed)
        seen["bound"] = _type(_identity(routed))
        return _show_tsd(routed)

    seen["values"] = eval_node(g, [(1, 2, 3), {1: 30}], ("a", "b", "c"))
    return seen


def generic_top():
    """WIR-7, WIR-8: a top-level reference is followed too."""
    seen = {}

    @graph
    def g(value: TS[int]) -> TS[str]:
        routed = _to_ref(value)
        seen["source"] = _type(routed)
        seen["bound"] = _type(_identity(routed))
        return _show(routed)

    seen["values"] = eval_node(g, [1, 2])
    return seen


def ref_pattern():
    """WIR-10: a REF in the pattern binds the variable beneath it."""
    seen = {}

    @graph
    def g(tsl: TSL[TS[int], Size[3]], keys: tuple[str, ...]) -> TS[bool]:
        routed = tsl_to_tsd(tsl, keys)
        seen["bound_below_ref"] = _type(_ref_identity(routed))
        return _holds_reference(tsl[0])

    seen["receives_reference"] = eval_node(g, [(1, 2, 3)], ("a", "b", "c"))
    return seen


def stated_resolution():
    """WIR-11: a stated resolution keeps its references."""
    seen = {}

    @graph
    def g(tsl: TSL[TS[int], Size[3]], keys: tuple[str, ...], value: TS[int]):
        routed = tsl_to_tsd(tsl, keys)
        nested = _identity[TIME_SERIES_TYPE: TSD[str, REF[TS[int]]]](routed)
        top = _identity[TIME_SERIES_TYPE: REF[TS[int]]](_to_ref(value))
        seen["stated_nested"] = _type(nested)
        seen["stated_top"] = _type(top)
        hg.null_sink(nested)  # a runnable graph needs a sink (Python 0.5.41)
        hg.null_sink(top)

    eval_node(g, [(1, 2, 3)], ("a", "b", "c"), [1])
    return seen


def requested_output():
    """WIR-12: a requested output keeps its references."""
    seen = {}

    @graph
    def g():
        for name, tp in (("nested", TSD[str, REF[TS[int]]]), ("top", REF[TS[int]]), ("plain", TSD[str, TS[int]])):
            port = hg.nothing[tp]()
            seen[name] = _type(port)
            hg.null_sink(port)  # a runnable graph needs a sink (Python 0.5.41)

    eval_node(g)
    return seen


class _RefFields(TimeSeriesSchema):
    routed: REF[TS[int]]
    plain: TS[int]


@compute_node
def _bundle(routed: REF[TS[int]], plain: TS[int]) -> TSB[_RefFields]:
    return {"routed": routed.value, "plain": plain.value}


def _values(fn):
    """Run a case's graph; an evaluation failure is recorded, not a harness error."""
    try:
        return fn()
    except Exception as error:  # noqa: BLE001 - recorded as the observation
        return f"raised {type(error).__name__}"


def projection():
    """WIR-5, WIR-13: a structural projection keeps a REF field."""
    seen = {}

    @graph
    def g(value: TS[int]) -> TS[int]:
        fields = _bundle(value, value)
        seen["bundle"] = _type(fields)
        seen["by_key"] = _type(fields["routed"])
        seen["by_attribute"] = _type(fields.routed)
        try:  # Python 0.5.41 has no getattr_ overload for a bundle
            seen["by_getattr"] = _type(hg.getattr_(fields, "routed"))
        except Exception as error:  # noqa: BLE001 - recorded as the observation
            seen["by_getattr"] = f"raised {type(error).__name__}"
        seen["plain_by_key"] = _type(fields["plain"])
        return fields["routed"]

    seen["values"] = _values(lambda: eval_node(g, [1, 2]))
    return seen


def projection_through_ref():
    """WIR-5: a field of a referenced bundle is a reference, made by a node."""
    seen = {}

    @graph
    def g(condition: TS[bool], value: TS[int]) -> TS[int]:
        routed = hg.if_(condition, value)
        seen["bundle"] = _type(routed)
        field = routed["true"]
        seen["field"] = _type(field)
        return field

    seen["values"] = _values(lambda: eval_node(g, [True, False], [1, 2]))
    return seen


@hg.operator
def _pick(ts: TIME_SERIES_TYPE) -> TS[str]:
    """Which candidate a call selects."""


@compute_node(overloads=_pick)
def _pick_int(ts: TS[int]) -> TS[str]:
    return "int"


@compute_node(overloads=_pick)
def _pick_generic(ts: TIME_SERIES_TYPE) -> TS[str]:
    return "generic"


@compute_node(overloads=_pick)
def _pick_tsl(ts: TSL[TIME_SERIES_TYPE, SIZE]) -> TS[str]:
    return "tsl-generic"


def operator_specificity():
    """WIR-16, WIR-18: the most specific candidate is selected; a REF adds none."""

    @graph
    def g(i: TS[int], f: TS[float], l: TSL[TS[int], Size[2]]) -> TSL[TS[str], Size[4]]:
        return TSL.from_ts(_pick(i), _pick(f), _pick(l), _pick(_to_ref(i)))

    [row] = eval_node(g, [1], [1.0], [(1, 2)])
    return {"selected": [row[i] for i in range(4)]}


@hg.operator
def _tied(ts: TIME_SERIES_TYPE) -> TS[str]:
    """Two equally specific candidates."""


@compute_node(overloads=_tied)
def _tied_a(ts: TS[int]) -> TS[str]:
    return "a"


@compute_node(overloads=_tied)
def _tied_b(ts: TS[int]) -> TS[str]:
    return "b"


@hg.operator
def _only_int(ts: TIME_SERIES_TYPE) -> TS[str]:
    """One concrete candidate."""


@compute_node(overloads=_only_int)
def _only_int_impl(ts: TS[int]) -> TS[str]:
    return "int"


def _raises(fn) -> bool:
    try:
        fn()
    except Exception:  # noqa: BLE001 - the observation is only whether wiring failed
        return True
    return False


def operator_failures():
    """WIR-4, WIR-16: no candidate, or a tie, fails the call."""

    @graph
    def tie(i: TS[int]) -> TS[str]:
        return _tied(i)

    @graph
    def none(s: TS[str]) -> TS[str]:
        return _only_int(s)

    return {
        "ambiguous_raises": _raises(lambda: eval_node(tie, [1])),
        "no_candidate_raises": _raises(lambda: eval_node(none, ["x"])),
    }


@compute_node
def _same(a: TIME_SERIES_TYPE, b: TIME_SERIES_TYPE) -> TS[bool]:
    return True


def repeated_variable():
    """WIR-7, WIR-17: a repeated variable binds once, dereferenced."""

    @graph
    def matched(i: TS[int]) -> TS[bool]:
        return _same(_to_ref(i), i)

    @graph
    def mismatched(i: TS[int], f: TS[float]) -> TS[bool]:
        return _same(i, f)

    return {
        "ref_and_value_values": eval_node(matched, [1]),
        "different_types_raise": _raises(lambda: eval_node(mismatched, [1], [1.0])),
    }


class _Foo(TimeSeriesSchema):
    a: TS[int]


class _Bar(TimeSeriesSchema):
    a: TS[int]


_Unnamed = hg.ts_schema(a=TS[int])


@compute_node
def _make_foo(v: TS[int]) -> TSB[_Foo]:
    return {"a": v.value}


@compute_node
def _make_bar(v: TS[int]) -> TSB[_Bar]:
    return {"a": v.value}


@compute_node
def _make_unnamed(v: TS[int]) -> TSB[_Unnamed]:
    return {"a": v.value}


@compute_node
def _takes_foo(b: TSB[_Foo]) -> TS[int]:
    return b.a.value


@compute_node
def _takes_unnamed(b: TSB[_Unnamed]) -> TS[int]:
    return b.a.value


def _wires(build) -> str:
    """Whether wiring and running ``build`` over one int tick succeeds."""

    @graph
    def g(v: TS[int]):
        hg.null_sink(build(v))

    try:
        eval_node(g, [1])
    except Exception:  # noqa: BLE001 - the observation is only whether wiring failed
        return "fails"
    return "wires"


def bundle_identity():
    """WIR-15: bundle names count only when both bundles are named."""
    return {
        "named_and_unnamed_repeat": _wires(lambda v: _same(_make_foo(v), _make_unnamed(v))),
        "same_name_repeat": _wires(lambda v: _same(_make_foo(v), _make_foo(v))),
        "two_names_repeat": _wires(lambda v: _same(_make_foo(v), _make_bar(v))),
        "named_input_from_unnamed": _wires(lambda v: _takes_foo(_make_unnamed(v))),
        "unnamed_input_from_named": _wires(lambda v: _takes_unnamed(_make_foo(v))),
        "named_input_from_other_name": _wires(lambda v: _takes_foo(_make_bar(v))),
    }


CASES = {
    fn.__name__: fn
    for fn in (
        generic_depth,
        generic_top,
        ref_pattern,
        stated_resolution,
        requested_output,
        projection,
        projection_through_ref,
        operator_specificity,
        operator_failures,
        repeated_variable,
        bundle_identity,
    )
}


def main() -> None:
    name = sys.argv[1]
    try:
        observation = CASES[name]()
    except Exception as error:  # noqa: BLE001 - a harness failure is recorded, not a value
        observation = {"harness_error": f"{type(error).__name__}: {error}"}
    print(json.dumps(observation, sort_keys=True, default=repr))
    sys.stdout.flush()


if __name__ == "__main__":
    main()
