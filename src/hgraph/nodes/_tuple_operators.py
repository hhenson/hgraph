from typing import Type, TypeVar

from hgraph import SCALAR, TS, HgTypeMetaData, WiringContext, MissingInputsError, IncorrectTypeBinding, compute_node, \
    with_signature, TimeSeries, HgTupleFixedScalarType, HgTupleCollectionScalarType, TSL
from hgraph._runtime._operators import getitem_


__all__ = ("TUPLE", "tuple_from_ts")

from hgraph.nodes import flatten_tsl_values

TUPLE = TypeVar("TUPLE", bound=tuple)


def _item_type(tuple_tp: Type[TUPLE], index: int) -> Type:
    if isinstance(tuple_tp, HgTupleFixedScalarType):
        return tuple_tp.element_types[index]
    elif isinstance(tuple_tp, HgTupleCollectionScalarType):
        return tuple_tp.element_type
    raise IncorrectTypeBinding(TUPLE, tuple_tp)


@compute_node(overloads=getitem_, resolvers={SCALAR: lambda mapping, scalars: _item_type(mapping[TUPLE],scalars['key'])})
def getitem_tuple(ts: TS[TUPLE], key: int) -> TS[SCALAR]:
    return ts.value[key]


def tuple_from_ts(cls: Type[TUPLE], *args, all_valid: bool = True) -> TS[TUPLE]:
    cls = HgTypeMetaData.parse_type(cls)
    if isinstance(cls, HgTupleFixedScalarType):
        scalar_schema = cls.element_types
        args_schema = tuple(HgTypeMetaData.parse_value(v) for v in args)

        with WiringContext(current_signature=dict(signature=f"from_ts({str(cls)}, ...)")):
            for i, t in enumerate(scalar_schema):
                if i >= len(args):
                    raise MissingInputsError({i: a for i, a in enumerate(args)})
                if (kt := args_schema[i]) is None:
                    raise MissingInputsError({i: a for i, a in enumerate(args)})
                elif not t.matches(kt if kt.is_scalar else kt.scalar_type()):
                    raise IncorrectTypeBinding(t, args_schema[i])

            @compute_node(valid=None if all_valid else ())
            @with_signature(kwargs={f'_{i}': v for i, v in enumerate(args_schema)}, return_annotation=TS[cls.py_type])
            def from_ts_node(**kwargs):
                return cls.py_type(v if not isinstance(v, TimeSeries) else v.value for v in kwargs.values())

            return from_ts_node(*args)
    elif isinstance(cls, HgTupleCollectionScalarType):
        return flatten_tsl_values(TSL.from_ts(*args), all_valid=all_valid)
    else:
        raise IncorrectTypeBinding(TUPLE, cls)