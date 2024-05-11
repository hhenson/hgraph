from typing import Tuple, Type, Callable

from hgraph._types import HgTSTypeMetaData, HgCompoundScalarType, HgTypeMetaData
from hgraph._wiring._wiring_node_class import WiringNodeClass, BaseWiringNodeClass, WiringNodeSignature

__all__ = ('dispatch', 'dispatch_')


def dispatch(fn: Callable = None, *, on: Tuple[str, ...] = None):
    """
    Decorator that converts a graph into a single or multiple dispatch graph. See dispatch_ for more details.
    """

    if fn is None:
        return lambda fn: dispatch(fn, on=on)

    from hgraph import graph, with_signature, AUTO_RESOLVE
    from hgraph._wiring._wiring_node_class._wiring_node_class import OverloadedWiringNodeHelper

    if not isinstance(fn, BaseWiringNodeClass):
        fn: WiringNodeClass = graph(fn)

    @with_signature(
        kwargs={**fn.signature.non_autoresolve_inputs, '__resolution_dict__': HgTypeMetaData.parse_type(object)},
        defaults={'__resolution_dict__': AUTO_RESOLVE},
        return_annotation=fn.signature.output_type)
    def dispatch_(**kwargs):
        if overloads := getattr(dispatch_graph, 'overload_list', None):
            overload_list = overloads.overloads + [(fn, OverloadedWiringNodeHelper._calc_rank(fn.signature))]
            if __resolution_dict__ := kwargs.get('__resolution_dict__', None):
                args = tuple(slice(k, v) for k, v in __resolution_dict__.items())
                overload_list = tuple((o[args], r) for o, r in overload_list)
            return _dispatch_impl(fn.signature, overload_list, __on__=on, **kwargs)
        else:
            from hgraph import CustomMessageWiringError
            raise CustomMessageWiringError(f'{fn.signature} has no overloads to dispatch to')

    dispatch_graph = graph(dispatch_)
    dispatch_graph.skip_overload_check = True
    return dispatch_graph


def dispatch_(overloaded: BaseWiringNodeClass, *args, __on__: Tuple[str, ...] = None, **kwargs):
    """
    Dispatch to the right overload for the types of the arguments at runtime.

    For example if there is a graph or node with a few overloads on a argument that is a type hierarchy this function
    will build a switch that uses the type of the argument to dispatch to the right overload:

    class Pet(CompoundScalar): ...
    class Dog(Pet): ...
    class Cat(Pet): ...

    @graph
    def pet_sound(pet: TS[Pet]) -> TS[str]: ...

    @graph(overloads=pet_sound)
    def pet_sound(pet: TS[Dog]) -> TS[str]: ...

    @graph(overloads=pet_sound)
    def pet_sound(pet: TS[Cat]) -> TS[str]: ...

    @graph
    def make_sound(pet: TS[Pet]) -> TS[str]:
        return dispatch_(pet_sound, pet)

    """
    if overloads := getattr(overloaded, 'overload_list', None):
        return _dispatch_impl(overloaded.signature, overloads.overloads, *args, __on__=__on__, **kwargs)
    else:
        from hgraph import CustomMessageWiringError
        raise CustomMessageWiringError(f'{overloaded.signature} has no overloads to dispatch to')


def _dispatch_impl(signature: WiringNodeSignature, overloads: BaseWiringNodeClass, *args,
                   __on__: Tuple[str, ...] = None, **kwargs):
    from hgraph import CustomMessageWiringError, switch_, TSL, SCALAR, with_signature, graph, nth, compute_node, TS, CompoundScalar, extract_kwargs
    from hgraph.nodes import flatten_tsl_values, downcast_ref

    dispatch_args = {}
    if __on__ is None:
        for k, t in signature.input_types.items():
            if t.is_scalar:
                continue
            if isinstance(t, HgTSTypeMetaData) and isinstance(t.value_scalar_tp, HgCompoundScalarType):
                dispatch_args[k] = t.value_scalar_tp
    else:
        for k in __on__:
            t = signature.input_types[k]
            if t.is_scalar:
                raise CustomMessageWiringError(f'Cannot dispatch on scalar type {t}')
            if isinstance(t, HgTSTypeMetaData) and isinstance(t.value_scalar_tp, HgCompoundScalarType):
                dispatch_args[k] = t.value_scalar_tp
            else:
                raise CustomMessageWiringError(f'Cannot dispatch on type {t}')

    dispatch_map = {}
    for o, _ in overloads:
        o_dispatch_types = {k: t.value_scalar_tp for k, t in o.signature.input_types.items() if k in dispatch_args and isinstance(t, HgTSTypeMetaData)}
        if len(o_dispatch_types) != len(dispatch_args):
            continue  # not a valid overload
        if not all(dispatch_args[k].matches(t) for k, t in o_dispatch_types.items()):
            continue  # not a valid overload

        def make_dispatch_graph(o, dispatch_types):
            @graph
            @with_signature(kwargs=signature.non_autoresolve_inputs, return_annotation=o.signature.output_type)
            def dispatch(**kwargs):
                kw = {k: downcast_ref(dispatch_types[k].py_type, v) if k in dispatch_types else v for k, v in kwargs.items()}
                return o(**kw)
            return dispatch

        from hgraph import create_input_stub
        stub_args = {k: create_input_stub(k, HgTSTypeMetaData(v), False) for k, v in o_dispatch_types.items()}
        stub_args.update({k: v for k, v in kwargs.items() if k not in dispatch_args and k in o.signature.args})

        from hgraph import RequirementsNotMetWiringError
        try:
            o.resolve_signature(**stub_args)
        except RequirementsNotMetWiringError:
            continue

        key = tuple(t.py_type for t in o_dispatch_types.values())
        if len(o_dispatch_types) == 1:
            key = key[0]

        dispatch_map[key] = make_dispatch_graph(o, o_dispatch_types)

    kwargs = extract_kwargs(signature, *args, **kwargs)  # process args and kwargs in kwargs so we can build a key

    if len(dispatch_args) == 1:
        @compute_node
        def adjust_dispatch_key(key: TS[Type[CompoundScalar]], available_keys: Tuple[Type[CompoundScalar], ...]) -> TS[Type[CompoundScalar]]:
            if key.value in available_keys:
                return key.value
            else:
                candidates = []
                for a_key in available_keys:
                    if issubclass(key.value, a_key):
                        candidates.append((a_key, a_key.__mro__.index(CompoundScalar)))
                if not candidates:
                    raise RuntimeError(f"No suitable overload found for {key.value}")
                if len(candidates) > 1 and candidates[0][1] == candidates[1][1]:
                    raise RuntimeError(f"Ambiguous dispatch for {key.value}")
                return candidates[0][0]

        from hgraph import type_
        key = adjust_dispatch_key(type_(kwargs[nth(dispatch_args.keys(), 0)]), tuple(dispatch_map.keys()))
    else:
        @compute_node
        def adjust_dispatch_keys(key: TS[Tuple[Type[CompoundScalar], ...]],
                                 available_keys: Tuple[Tuple[Type[CompoundScalar], ...], ...]) \
                -> TS[Tuple[Type[CompoundScalar], ...]]:
            if key.value in available_keys:
                return key.value
            else:
                candidates = []
                for a_keys in available_keys:
                    if all(issubclass(k, ak) for ak, k in zip(a_keys, key.value)):
                        candidates.append((a_keys, sum(ak.__mro__.index(CompoundScalar) for ak in a_keys)))
                if not candidates:
                    raise RuntimeError(f"No suitable overload found for {key.value}")
                candidates.sort(key=lambda x: x[1], reverse=True)
                if len(candidates) > 1 and candidates[0][1] == candidates[1][1]:
                    raise RuntimeError(f"Ambiguous dispatch for {key.value}")
                return candidates[0][0]

        from hgraph import type_
        key = adjust_dispatch_keys(flatten_tsl_values[SCALAR: Type[CompoundScalar]](TSL.from_ts(*[type_(kwargs[k]) for k in dispatch_args]), all_valid=True), tuple(dispatch_map.keys()))

    return switch_(dispatch_map, **kwargs, key=key)  # AB: should rename key to __key__ in the switch_ signature?
