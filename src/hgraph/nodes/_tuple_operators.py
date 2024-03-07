from typing import Type, TypeVar

from hgraph import SCALAR, TS, HgTypeMetaData, WiringContext, MissingInputsError, IncorrectTypeBinding, compute_node, \
    with_signature, TimeSeries

TUPLE = TypeVar("TUPLE", bound=tuple)


def from_ts(cls: Type[TUPLE], *args) -> TS[TUPLE]:
    scalar_schema = cls.element_types
    args_schema = tuple(HgTypeMetaData.parse_value(v) for v in args)

    with WiringContext(current_signature=dict(signature=f"from_ts({cls.__name__}, ...)")):
        for i, t in enumerate(scalar_schema.items()):
            if i >= len(args):
                raise MissingInputsError({i: a for i, a in enumerate(args)})
            if (kt := args_schema[i]) is None:
                raise MissingInputsError({i: a for i, a in enumerate(args)})
            elif not t.matches(kt if kt.is_scalar else kt.scalar_type()):
                raise IncorrectTypeBinding(t, args_schema[i])

        @compute_node
        @with_signature(args=args_schema, return_annotation=TS[cls])
        def from_ts_node(**args):
            return cls(v if not isinstance(v, TimeSeries) else v.value for v in args)

        return from_ts_node(**args)
