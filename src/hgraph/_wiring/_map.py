from itertools import chain
from typing import Callable, cast, TYPE_CHECKING, List

from frozendict import frozendict

from hgraph._types._scalar_types import SCALAR, STATE, Size
from hgraph._types._time_series_types import TIME_SERIES_TYPE
from hgraph._types._ts_meta_data import HgTSTypeMetaData
from hgraph._types._ts_type import TS
from hgraph._types._ts_type_var_meta_data import HgTsTypeVarTypeMetaData
from hgraph._types._time_series_meta_data import HgTimeSeriesTypeMetaData
from hgraph._types._tsd_meta_data import HgTSDTypeMetaData
from hgraph._types._tsd_type import TSD
from hgraph._types._type_meta_data import HgTypeMetaData
from hgraph._types._tsl_meta_data import HgTSLTypeMetaData
from hgraph._wiring._map_wiring_node import TsdMapWiringNodeClass, TsdMapWiringSignature, TslMapWiringSignature, \
    TslMapWiringNodeClass
from hgraph._wiring._wiring import WiringNodeClass
from hgraph._wiring._wiring_node_signature import WiringNodeSignature, WiringNodeType
from hgraph._wiring._wiring_port import WiringPort
from hgraph._wiring._wiring import extract_kwargs
from hgraph._wiring._wiring_context import WiringContext
from hgraph._wiring._wiring_errors import CustomMessageWiringError
from hgraph._wiring._wiring_errors import NoTimeSeriesInputsError
from hgraph._wiring._wiring_utils import stub_wiring_port, as_reference

if TYPE_CHECKING:
    from hgraph._types._scalar_type_meta_data import HgAtomicType

__all__ = ("map_", "pass_through", "no_key", "KEYS_ARG")

KEYS_ARG = '__keys__'
_KEY_ARG = "__key_arg__"


def pass_through(tsd: TSD[SCALAR, TIME_SERIES_TYPE]) -> TSD[SCALAR, TIME_SERIES_TYPE]:
    """
    Marks the TSD input as a pass through value. This will ensure the TSD is not included in the key mapping in the
    tsd_map function. This is useful when the function takes a template type and the TSD has the same SCALAR type as
    the implied keys for the tsd_map function.
    """
    # noinspection PyTypeChecker
    return _PassthroughMarker(tsd)


def no_key(tsd: TSD[SCALAR, TIME_SERIES_TYPE]) -> TSD[SCALAR, TIME_SERIES_TYPE]:
    """
    Marks the TSD input as not contributing to the keys of the tsd_map function.
    This is useful when the input TSD is likely to be larger than the desired keys to process.
    This is only required if no keys are supplied to the tsd_map function.
    """
    # noinspection PyTypeChecker
    return _NoKeyMarker(tsd)


def map_(func: Callable, *args, **kwargs):
    """
    This is a simple wrapper that makes it easier to use the map without having to think about the inputs too much.
    This will attempt to infer which of the map functions are suitable to make use of based on the inputs provided.
    It will then delegate to the appropriate map function.
    """
    if not isinstance(func, WiringNodeClass):
        raise RuntimeError(f"The supplied function is not a graph or node function: '{func.__name__}'")
    with WiringContext(current_signature=STATE(signature=f"map_('{func.signature.signature}', ...)")):
        if len(args) + len(kwargs) == 0:
            raise NoTimeSeriesInputsError()
        signature: WiringNodeSignature = func.signature
        map_wiring_node, calling_kwargs = _build_map_wiring_node_and_inputs(func, signature, *args, **kwargs)
        return map_wiring_node(**calling_kwargs)


class _MappingMarker:

    def __init__(self, value: TSD[SCALAR, TIME_SERIES_TYPE]):
        assert isinstance(value, WiringPort), "Marker must wrap a valid time-series input."
        self.value = value

    @property
    def output_type(self):
        return self.value.output_type


class _PassthroughMarker(_MappingMarker):
    ...


class _NoKeyMarker(_MappingMarker):
    ...


def _build_map_wiring_node_and_inputs(
        fn: Callable, signature: WiringNodeSignature, *args, __keys__=None, __key_arg__=None,
        **kwargs) -> tuple[WiringNodeClass, dict[str, WiringPort | SCALAR]]:
    """
    Build the maps wiring signature. This will process the inputs looking to work out which are multiplexed inputs,
    which are pass-through, etc. It will perform basic validation that will ensure the signature of the mapped function
    and the inputs provided are compatible.
    """
    # 1. See if the first argument of the signature is a key argument.
    #    A key argument has a name of either 'key' (for TSD) or 'ndx' (for TSL)
    #    The key is a TS[SCALAR] for TSD and TS[int] for TSL.
    input_has_key_arg, input_key_name, input_key_tp = _extract_map_fn_key_arg_and_type(signature, __key_arg__)

    # 2. Now we can safely extract the kwargs.
    kwargs_ = extract_kwargs(signature, *args, _ensure_match=False, _args_offset=1 if input_has_key_arg else 0,
                             **kwargs)

    # 3. Split out the inputs into multiplexed, no_key, pass_through and direct and key_tp
    multiplex_args, no_key_args, pass_through_args, _, map_type, key_tp_ = _split_inputs(signature, kwargs_, __keys__)

    # 4. If the key is present, make sure the extracted key type matches what we found in the multiplexed inputs.
    if map_type == "TSL":
        tp = HgTSTypeMetaData.parse(TS[int])
    else:
        tp = key_tp_
    if input_has_key_arg and not input_key_tp.matches(tp):
        raise CustomMessageWiringError(
            f"The ndx argument '{signature.args[0]}: {input_key_tp}' does not match '{tp}'")
    input_key_tp = tp

    # 5. Extract provided key signature
    #    We use the output_type of wiring ports, but for scalar values, they must take the form of the underlying
    #    function signature, so we just use from that signature.
    input_types = {k: v.output_type if isinstance(v, (WiringPort, _MappingMarker)) else signature.input_types[k] for
                   k, v in kwargs_.items()}

    # 6. Create the wiring nodes for the map function.
    match map_type:
        case "TSD":
            if __keys__ is not None:
                kwargs_[KEYS_ARG] = __keys__
            else:
                from hgraph.nodes import union_
                __keys__ = union_(*tuple(kwargs_[k].key_set for k in multiplex_args if k not in no_key_args))
                kwargs_[KEYS_ARG] = __keys__
            input_types = input_types | {KEYS_ARG: __keys__.output_type}
            map_wiring_node = _create_tsd_map_wiring_node(fn, kwargs_, input_types, multiplex_args, no_key_args,
                                                          input_key_tp, input_key_name if input_has_key_arg else None)
        case "TSL":
            from hgraph._types._scalar_type_meta_data import HgAtomicType
            map_wiring_node = _create_tsl_map_signature(fn, kwargs_, input_types, multiplex_args,
                                                        HgAtomicType.parse(key_tp_),
                                                        input_key_name if input_has_key_arg else None)
        case _:
            raise CustomMessageWiringError(f"Unable to determine map type for given inputs: {kwargs_}")

    # 7. Clean the inputs (eliminate the marker wrappers)
    for arg in chain(pass_through_args, no_key_args):
        kwargs_[arg] = kwargs_[arg].value  # Unwrap the marker inputs.

    return map_wiring_node, kwargs_


def _extract_map_fn_key_arg_and_type(signature: WiringNodeSignature, __key_arg__) \
        -> tuple[bool, str | None, HgTSTypeMetaData | None]:
    """
    Attempt to detect if the mapping fn has a key argument and if so, what is the type of the key is.
    """
    input_has_key_arg = False
    input_key_tp = None
    input_key_name = __key_arg__
    if input_key_name == "":
        # If the user supplied an emtpy string for _key_arg, interpret as ignore any input named as key / ndx
        # and that no key arg is present.
        return False, None, None
    if input_key_name:
        input_has_key_arg = True
        if input_key_name != signature.args[0]:
            raise CustomMessageWiringError(
                f"The key argument '{input_key_name}' is not the first argument of the function: '{signature.signature}'")
        input_key_tp = signature.input_types[input_key_name]
    elif signature.args[0] in ('key', 'ndx'):
        from hgraph._types._time_series_meta_data import HgTimeSeriesTypeMetaData
        input_key_tp = signature.input_types[signature.args[0]]
        match_tp = None
        match signature.args[0]:
            case 'key':
                input_key_name = 'key'
                match_tp = HgTimeSeriesTypeMetaData.parse(TS[SCALAR])
            case 'ndx':
                input_key_name = 'ndx'
                match_tp = HgTimeSeriesTypeMetaData.parse(TS[int])
        if not (input_has_key_arg := (match_tp and signature.input_types[signature.args[0]].matches(match_tp))):
            if match_tp:
                raise CustomMessageWiringError(
                    f"The key argument '{signature.args[0]}: {signature.input_types[signature.args[0]]}' "
                    f"does not match the expected type: '{match_tp}'")

    return input_has_key_arg, input_key_name, cast(HgTSTypeMetaData, input_key_tp)


def _split_inputs(signature: WiringNodeSignature, kwargs_, tsd_keys) \
        -> tuple[frozenset[str], frozenset[str], frozenset[str], frozenset[str], str, HgTimeSeriesTypeMetaData]:
    # multiplex, no_key passthrough, direct, tp, key_tp
    """
    Splits out the inputs into three groups:
    #. multiplex_args: These are the inputs that need to be de-multiplexed.
    #. no_key_args: These are the inputs that are marked as pass through or no key.
    #. pass_through_args: These are the inputs that are marked as pass through.
    #. direct_args: These are the inputs that match the signature of the underlying signature.

    This will also validate that the inputs are correctly typed if requested to do so, for the map_ function
    it is useful to by-pass some of the checks are it is really only interested in guessing the correct map type.

    Key type is only present if validate_type is True.
    """
    if non_ts_inputs := [arg for arg in kwargs_ if not isinstance(kwargs_[arg], (WiringPort, _MappingMarker))]:
        if not all(k in signature.scalar_inputs for k in non_ts_inputs):
            raise CustomMessageWiringError(
                f" The following args are not time-series inputs, but should be: {non_ts_inputs}")

    marker_args = frozenset(arg for arg in kwargs_ if isinstance(kwargs_[arg], _MappingMarker))
    pass_through_args = frozenset(arg for arg in marker_args if isinstance(kwargs_[arg], _PassthroughMarker))
    no_key_args = frozenset(arg for arg in marker_args if arg not in pass_through_args)

    _validate_pass_through(signature, kwargs_, pass_through_args)  # Ensure the pass through args are correctly typed.

    input_types = {k: v.output_type for k, v in kwargs_.items() if k not in non_ts_inputs}

    direct_args = frozenset(
        k for k, v in input_types.items() if k not in marker_args and signature.input_types[k].matches(v) if
        (type(signature.input_types[k]) is not HgTsTypeVarTypeMetaData and  # All time-series value match this!
         type(v) not in (HgTSLTypeMetaData, HgTSDTypeMetaData)))  # So if it is possibly not direct, don't mark it direct

    multiplex_args = frozenset(
        k for k, v in input_types.items() \
        if k not in pass_through_args and \
        k not in direct_args and \
        type(v) in (HgTSDTypeMetaData, HgTSLTypeMetaData)
    )

    _validate_multiplex_types(signature, kwargs_, multiplex_args, no_key_args)

    if not tsd_keys and (len(no_key_args) + len(multiplex_args) == 0):
        raise CustomMessageWiringError("No multiplexed inputs found")

    if len(multiplex_args) + len(direct_args) + len(pass_through_args) + len(non_ts_inputs) != len(kwargs_):
        raise CustomMessageWiringError(
            f"Unable to determine how to split inputs with args:\n {kwargs_}")

    if is_tsl := any(isinstance(v, HgTSLTypeMetaData) for v in input_types.values()):
        if not all(isinstance(input_types[k], HgTSLTypeMetaData) for k in multiplex_args):
            raise CustomMessageWiringError("Not all multiplexed inputs are of type TSL or TSD")

    if is_tsl:
        key_tp = _extract_tsl_size(kwargs_, multiplex_args, no_key_args)
    else:
        key_tp = _validate_tsd_keys(kwargs_, multiplex_args, no_key_args, tsd_keys)

    return (multiplex_args, no_key_args, pass_through_args, direct_args, "TSL" if is_tsl else "TSD", key_tp if is_tsl
    else HgTSTypeMetaData(key_tp))


def _prepare_stub_inputs(
        kwargs_: dict[str, WiringPort | SCALAR],
        input_types: dict[str, HgTypeMetaData],
        multiplex_args: frozenset[str],
        no_key_args: frozenset[str],
        input_key_tp: HgTSTypeMetaData,
        input_key_name: str | None
):
    call_kwargs = {}
    for key, arg in input_types.items():
        if key in multiplex_args or key in no_key_args:
            arg: HgTSDTypeMetaData | HgTSLTypeMetaData
            call_kwargs[key] = stub_wiring_port(arg.value_tp)
        elif key == KEYS_ARG:
            continue
        elif arg.is_scalar:
            call_kwargs[key] = kwargs_[key]
        else:
            call_kwargs[key] = stub_wiring_port(arg)
    if input_key_name:
        call_kwargs[input_key_name] = stub_wiring_port(input_key_tp)
    return call_kwargs


def _create_tsd_map_wiring_node(
        fn: WiringNodeClass,
        kwargs_: dict[str, WiringPort | SCALAR],
        input_types: dict[str, HgTypeMetaData],
        multiplex_args: frozenset[str],
        no_key_args: frozenset[str],
        input_key_tp: HgTSTypeMetaData,
        input_key_name: str | None
) -> TsdMapWiringNodeClass:
    from hgraph._types._ref_meta_data import HgREFTypeMetaData

    # Resolve the mapped function signature
    stub_inputs = _prepare_stub_inputs(kwargs_, input_types, multiplex_args, no_key_args, input_key_tp, input_key_name)
    resolved_signature = fn.resolve_signature(**stub_inputs)

    reference_inputs = frozendict(
        {k: as_reference(v, k in multiplex_args) if isinstance(v, HgTimeSeriesTypeMetaData) and k != KEYS_ARG else v for
         k, v in input_types.items()})

    # NOTE: The wrapper node does not need to sets it valid and tick to that of the underlying node, it just
    #       needs to ensure that it gets notified when the key sets tick. Likewise with validity.
    map_signature = TsdMapWiringSignature(
        node_type=WiringNodeType.COMPUTE_NODE if resolved_signature.output_type else WiringNodeType.SINK_NODE,
        name="map",
        # All actual inputs are encoded in the input_types, so we just need to add the keys if present.
        args=tuple(input_types.keys()),
        defaults=frozendict(),  # Defaults would have already been applied.
        input_types=reference_inputs,
        output_type=HgTSDTypeMetaData(input_key_tp.value_scalar_tp, HgREFTypeMetaData(resolved_signature.output_type)) \
            if resolved_signature.output_type else None,
        src_location=resolved_signature.src_location,  # TODO: Figure out something better for this.
        active_inputs=None,  # We will follow a copy approach to transfer the inputs to inner graphs
        valid_inputs=frozenset({KEYS_ARG, }),  # We have constructed the map so that the key are is always present.
        all_valid_inputs=None,
        unresolved_args=frozenset(),
        time_series_args=frozenset(k for k, v in input_types.items() if not v.is_scalar),
        uses_scheduler=False,
        label=f"map('{resolved_signature.signature}', {', '.join(input_types.keys())})",
        map_fn_signature=resolved_signature,
        key_tp=input_key_tp.value_scalar_tp,
        key_arg=input_key_name,
        multiplexed_args=multiplex_args,
    )
    wiring_node = TsdMapWiringNodeClass(map_signature, fn)
    return wiring_node


def _create_tsl_map_signature(
        fn: WiringNodeClass,
        kwargs_: dict[str, WiringPort | SCALAR],
        input_types: dict[str, HgTypeMetaData],
        multiplex_args: frozenset[str],
        size_tp: "HgAtomicType",
        input_key_name: str | None
):
    from hgraph._types._ref_meta_data import HgREFTypeMetaData

    # Resolve the mapped function signature
    stub_inputs = _prepare_stub_inputs(kwargs_, input_types, multiplex_args, frozenset(),
                                       HgTSTypeMetaData.parse(TS[int]),
                                       input_key_name)
    resolved_signature = fn.resolve_signature(**stub_inputs)

    reference_inputs = frozendict(
        {k: as_reference(v, k in multiplex_args) if isinstance(v, HgTimeSeriesTypeMetaData) else v for
         k, v in input_types.items()})

    map_signature = TslMapWiringSignature(
        node_type=WiringNodeType.COMPUTE_NODE if resolved_signature.output_type else WiringNodeType.SINK_NODE,
        name="map",
        # All actual inputs are encoded in the input_types, so we just need to add the keys if present.
        args=tuple(input_types.keys()),
        defaults=frozendict(),  # Defaults would have already been applied.
        input_types=frozendict(reference_inputs),
        output_type=HgTSLTypeMetaData(HgREFTypeMetaData(resolved_signature.output_type),
                                      size_tp) if resolved_signature.output_type else None,
        src_location=resolved_signature.src_location,  # TODO: Figure out something better for this.
        active_inputs=frozenset(),
        valid_inputs=frozenset(),
        all_valid_inputs=None,
        unresolved_args=frozenset(),
        time_series_args=frozenset(k for k, v in input_types.items() if not v.is_scalar),
        uses_scheduler=False,
        label=f"map('{resolved_signature.signature}', {', '.join(input_types.keys())})",
        map_fn_signature=resolved_signature,
        size_tp=size_tp,
        key_arg=input_key_name,
        multiplexed_args=multiplex_args,
    )
    wiring_node = TslMapWiringNodeClass(map_signature, fn)
    return wiring_node


def _validate_tsd_keys(kwargs_, multiplex_args, no_key_args, tsd_keys):
    """
    Ensure all the multiplexed inputs use the same input key.
    """
    types = set(kwargs_[arg].output_type.key_tp for arg in chain(multiplex_args, no_key_args))
    if tsd_keys:
        types.add(tsd_keys.output_type.value_scalar_tp)
    if len(types) > 1:
        raise CustomMessageWiringError(
            f"The TSD multiplexed inputs have different key types: {types}")
    return next(iter(types))


def _validate_pass_through(signature: WiringNodeSignature, kwargs_, pass_through_args):
    """
    Validates that the pass-through inputs are valid.
    """
    for arg in pass_through_args:
        if isinstance(pt_type := kwargs_[arg], _PassthroughMarker):
            if not (in_type := signature.input_types[arg]).matches(pt_type.output_type):
                raise CustomMessageWiringError(
                    f"The input '{arg}: {pt_type.output_type}' is marked as pass_through,"
                    f"but is not compatible with the input type: {in_type}")


def _extract_tsl_size(kwargs_: dict[str, WiringPort], multiplex_args, marker_args) -> type[Size]:
    """
    With a TSL multiplexed input, we need to determine the size of the output. This is done by looking at all the inputs
    that could be multiplexed.
    """
    sizes: List[type[Size]] = [
        cast(type[Size], cast(HgTSLTypeMetaData, kwargs_[arg].output_type).size_tp.py_type) for arg in
        chain(multiplex_args, (m_arg for m_arg in marker_args if not isinstance(kwargs_[m_arg], _PassthroughMarker)))]
    size: type[Size] = Size
    for sz in sizes:
        if sz.FIXED_SIZE:
            if size.FIXED_SIZE:
                size = size if size.SIZE < sz.SIZE else sz
            else:
                size = sz
    return size


def _validate_multiplex_types(signature: WiringNodeSignature, kwargs_, multiplex_args, no_key_args):
    """
    Validates that the multiplexed inputs are valid.
    """
    for arg in chain(multiplex_args, no_key_args):
        if not (in_type := signature.input_types[arg]).matches((m_type := kwargs_[arg].output_type).value_tp):
            raise CustomMessageWiringError(
                f"The input '{arg}: {m_type}' is a multiplexed type, "
                f"but its '{m_type.value_tp}' is not compatible with the input type: {in_type}")