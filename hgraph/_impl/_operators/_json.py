import json
from typing import overload
from hgraph import (
    K,
    OUT,
    TS,
    TS_SCHEMA,
    TSB,
    TSD,
    combine,
    compute_node,
    SCALAR,
    JSON,
    convert,
    getattr_,
    getitem_,
    json_decode,
    json_encode,
    operator,
    Type,
    DEFAULT,
)


@compute_node(overloads=json_decode)
def json_decode_str(ts: TS[str]) -> TS[JSON]:
    return JSON(json.loads(ts.value))


@compute_node(overloads=json_decode)
def json_decode_bytes(ts: TS[bytes]) -> TS[JSON]:
    return JSON(json.loads(ts.value))


@compute_node(overloads=json_encode)
def json_encode_str(ts: TS[JSON], _tp: Type[str] = DEFAULT[SCALAR]) -> TS[str]:
    return json.dumps(ts.value.json)


@compute_node(overloads=json_encode)
def json_encode_bytes(ts: TS[JSON], _tp: Type[bytes] = DEFAULT[SCALAR]) -> TS[bytes]:
    return json.dumps(ts.value.json).encode("utf-8")


@compute_node(overloads=getattr_, requires=lambda m, s: s["attr"] == "str")
def getattr_json_str(ts: TS[JSON], attr: str) -> TS[str]:
    return ts.value.json


@compute_node(overloads=getattr_, requires=lambda m, s: s["attr"] == "bool")
def getattr_json_bool(ts: TS[JSON], attr: str) -> TS[bool]:
    return ts.value.json


@compute_node(overloads=getattr_, requires=lambda m, s: s["attr"] == "int")
def getattr_json_int(ts: TS[JSON], attr: str) -> TS[int]:
    return ts.value.json


@compute_node(overloads=getattr_, requires=lambda m, s: s["attr"] == "float")
def getattr_json_float(ts: TS[JSON], attr: str) -> TS[float]:
    return ts.value.json


@compute_node(overloads=getattr_, requires=lambda m, s: s["attr"] == "obj")
def getattr_json_obj(ts: TS[JSON], attr: str) -> TS[object]:
    return ts.value.json


@compute_node(overloads=getitem_)
def getitem_json_str(ts: TS[JSON], key: str) -> TS[JSON]:
    return JSON(ts.value.json.get(key))


@compute_node(overloads=getitem_)
def getitem_json_int(ts: TS[JSON], key: int) -> TS[JSON]:
    return JSON(ts.value.json[key])


@compute_node(
    overloads=combine,
    all_valid=lambda m, s: ("bundle",) if s["__strict__"] else None,
)
def combine_json(
    tp_out_: Type[TS[JSON]] = DEFAULT[OUT],
    __strict__: bool = True,
    **bundle: TSB[TS_SCHEMA],
) -> TS[JSON]:
    return JSON({k: v if type(v) is not JSON else v.json for k, v in bundle.value.items() if v is not None})


@compute_node(overloads=convert, requires=lambda m, s: m[OUT].py_type == TS[JSON])
def convert_tsd_to_json(
    ts: TSD[K, TS[JSON]],
    to: Type[TS[JSON]] = DEFAULT[OUT],
) -> TS[JSON]:
    return JSON({k: v.value.json for k, v in ts.valid_items()})
