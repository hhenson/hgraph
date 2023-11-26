from dataclasses import dataclass
from itertools import chain
from typing import Callable, cast

__all__ = ("map_", "pass_through", "no_key", "reduce")

from frozendict import frozendict

from hg._types._ref_meta_data import HgREFTypeMetaData
from hg._wiring._wiring import WiringNodeType
from hg._wiring._wiring_errors import NoTimeSeriesInputsError
from hg._wiring._wiring_context import WiringContext
from hg._wiring._wiring import WiringNodeSignature, WiringPort, HgTSLTypeMetaData, HgScalarTypeMetaData, \
    WiringNodeClass, TsdMapWiringNodeClass
from hg._types._type_meta_data import HgTypeMetaData
from hg._types._time_series_types import TIME_SERIES_TYPE, TIME_SERIES_TYPE_1
from hg._types._tsd_meta_data import HgTSDTypeMetaData
from hg._types._tsd_type import TSD
from hg._types._tsl_type import TSL
from hg._types._ts_type import TS
from hg._types._scalar_types import SIZE, SCALAR_1, SCALAR, STATE, Size
from hg._types._scalar_type_meta_data import HgAtomicType
from hg._types._ts_meta_data import HgTSTypeMetaData
from hg._types._ts_type_var_meta_data import HgTimeSeriesTypeMetaData
from hg._wiring._wiring import extract_kwargs
from hg._wiring._wiring_errors import CustomMessageWiringError

_INDEX = "__index__"
_KEYS = '__keys__'
_KEY_ARG = "__key_arg__"


def pass_through(tsd: TSD[SCALAR, TIME_SERIES_TYPE]) -> TSD[SCALAR, TIME_SERIES_TYPE]:
    """
    Marks the TSD input as a pass through value. This will ensure the TSD is not included in the key mapping in the
    tsd_map function. This is useful when the function takes a template type and the TSD has the same SCALAR type as
    the implied keys for the tsd_map function.
    """
    return _PassthroughMarker(tsd)


def no_key(tsd: TSD[SCALAR, TIME_SERIES_TYPE]) -> TSD[SCALAR, TIME_SERIES_TYPE]:
    """
    Marks the TSD input as not contributing to the keys of the tsd_map function.
    This is useful when the input TSD is likely to be larger than the desired keys to process.
    This is only required if no keys are supplied to the tsd_map function.
    """
    return _NoKeyMarker(tsd)


def map_(func: Callable, *args, **kwargs):
    """
    This is a simple wrapper that makes it easier to use the map without having to think about the inputs too much.
    This will attempt to infer which of the map functions are suitable to make use of based on the inputs provided.
    It will then delegate to the appropriate map function.

    This can fail to correct detect the type of map to use, as such it is still possible to use the named
    map functions (tsd_map and tsl_map) directly. For more information about these see the documentation
    for the individual types.

    """
    if not isinstance(func, WiringNodeClass):
        raise RuntimeError(f"The supplied function is not a graph or node function: '{func.__name__}'")
    with WiringContext(current_signature=STATE(current_signature=f"map_('{func.signature.signature}', ...)")):
        if len(args) + len(kwargs) == 0:
            raise NoTimeSeriesInputsError()
        signature: WiringNodeSignature = func.signature
        map_wiring_node, calling_kwargs = _build_map_wiring_node_and_inputs(func, signature, *args, **kwargs)
        return map_wiring_node(**calling_kwargs)


def reduce(func: Callable[[TIME_SERIES_TYPE, TIME_SERIES_TYPE_1], TIME_SERIES_TYPE],
           ts: TSD[SCALAR, TIME_SERIES_TYPE_1] | TSL[TIME_SERIES_TYPE_1, SIZE],
           zero: SCALAR_1, is_associated: bool = True) -> TIME_SERIES_TYPE:
    """
    Reduce the input time-series collection into a single time-series value.
    The zero must be compatible with the TIME_SERIES_TYPE value and be constructable as const(zero, TIME_SERIES_TYPE).
    If the function is associative, then TIME_SERIES_TYPE must be the same as TIME_SERIES_TYPE_1.
    When the function is associative the reduce will perform a tree reduction, otherwise it will perform a linear
    reduction. The tree reduction is much faster on change.

    By definition the reduce function over a TSD must be commutative in the sense that the order of the inputs is
    not guaranteed. Only a TSL supports non-commutative reduce functions.

    Example [TSD]:
        tsd: TSD[str, TS[int]] = ...
        out = reduce(add_, tsd, 0)
        >> tsd <- {'a': [1], 'b': [4], 'c': [7]}
        >> out -> 12

    Example [TSL]:
        tsl: TSL[TS[int], SIZE] = ...
        out = reduce(add_, tsl, 0)
        >> tsl <- ([1], [2], [3], [4], [5])
        >> out -> 15
    """


@dataclass(frozen=True)
class TsdMapWiringSignature(WiringNodeSignature):
    map_fn_signature: WiringNodeSignature = None
    key_tp: HgScalarTypeMetaData = None
    key_arg: str | None = None  # The arg name of the key in the map function is there is one
    keyable_args: frozenset[str] | None = None  # When keys is not present, these are the inputs to key from.
    multiplexed_args: frozenset[str] | None = None  # The inputs that need to be de-multiplexed.


@dataclass(frozen=True)
class TslMapWiringSignature(WiringNodeSignature):
    map_fn_signature: WiringNodeSignature = None
    size_tp: HgAtomicType = None
    key_arg: str | None = None
    multiplexed_args: frozenset[str] | None = None  # The inputs that need to be de-multiplexed.


class _MappingMarker:

    def __init__(self, value: TSD[SCALAR, TIME_SERIES_TYPE]):
        assert isinstance(value, WiringPort), "Marker must wrap a valid time-series input."
        self.value = value

    @property
    def output_type(self):
        return self.value.output_type


class _PassthroughMarker:
    ...


class _NoKeyMarker:
    ...


def _build_map_wiring_node_and_inputs(
        fn: Callable, signature: WiringNodeSignature, *args, __keys__=None, __index__=None, __key_arg__=None,
        **kwargs) -> tuple[WiringNodeClass, dict[str, WiringPort | SCALAR]]:
    """
    Build the maps wiring signature. This will process the inputs looking to work out which are multiplexed inputs,
    which are pass-through, etc. It will perform basic validation that will ensure the signature of the mapped function
    and the inputs provided are compatible.
    """
    # 1. See if the first argument of the signature is a key argument.
    #    A key argument has a name of either 'key' (for TSD) or 'ndx' (for TSL)
    #    The key is a TS[SCALAR] for TSD and TS[int] for TSL.
    input_has_key_arg, input_key_name, input_key_tp = _extract_map_fn_key_arg_and_type(fn, signature, kwargs,
                                                                                       __key_arg__)

    # 2. Now we can safely extract the kwargs.
    kwargs_ = extract_kwargs(signature, *args, _ensure_match=False, _args_offset=1 if input_has_key_arg else 0,
                             **kwargs)

    # 3. Split out the inputs into multiplexed, no_key, pass_through and direct and key_tp
    multiplex_args, no_key_args, pass_through_args, direct_args, map_type, key_tp_ = _split_inputs(signature, kwargs_
                                                                                                   )
    # 4. If the key is present, make sure the extracted key type matches what we found in the multiplexed inputs.
    if map_type == "TSL":
        tp = HgTSTypeMetaData.parse(TS[int])
    else:
        tp = key_tp_
    if input_has_key_arg:
        if not input_key_tp.matches(tp):
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
                kwargs_[_KEYS] = __keys__
                input_types = input_types | {_KEYS: __keys__.output_type}
            map_wiring_node = _create_tsd_map_wiring_node(fn, kwargs_, input_types, multiplex_args, no_key_args,
                                                          input_key_tp, input_key_name if input_has_key_arg else None)
        case "TSL":
            if __index__ is not None:
                input_types = input_types | {_INDEX: __index__.output_type}
                kwargs_[_INDEX] = __index__
            map_wiring_node = _create_tsl_map_signature(fn, kwargs_, input_types, multiplex_args,
                                                        HgAtomicType.parse(key_tp_),
                                                        input_key_name if input_has_key_arg else None)
        case _:
            raise CustomMessageWiringError(f"Unable to determine map type for given inputs: {kwargs_}")

    # 7. Clean the inputs (eliminate the marker wrappers)
    for arg in chain(pass_through_args, no_key_args):
        kwargs_[arg] = kwargs_[arg].value  # Unwrap the marker inputs.

    return map_wiring_node, kwargs_


def _extract_map_fn_key_arg_and_type(fn: Callable, signature: WiringNodeSignature, kwargs, __key_arg__) \
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
        from hg._types._time_series_meta_data import HgTimeSeriesTypeMetaData
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


def _split_inputs(signature: WiringNodeSignature, kwargs_) \
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
        raise CustomMessageWiringError(
            f" The following args are not time-series inputs, but should be: {non_ts_inputs}")

    marker_args = frozenset(arg for arg in kwargs_ if isinstance(kwargs_[arg], _MappingMarker))
    pass_through_args = frozenset(arg for arg in marker_args if isinstance(kwargs_[arg], _PassthroughMarker))
    no_key_args = frozenset(arg for arg in marker_args if arg not in pass_through)

    _validate_pass_through(signature, kwargs_, pass_through_args)  # Ensure the pass through args are correctly typed.

    input_types = {k: v.output_type for k, v in kwargs_.items()}

    direct_args = frozenset(
        k for k, v in input_types.items() if k not in marker_args and signature.input_types[k].matches(v))

    multiplex_args = frozenset(
        k for k, v in input_types.items() \
        if k not in pass_through_args and \
        k not in direct_args and \
        type(v) in (HgTSDTypeMetaData, HgTSLTypeMetaData)
    )

    _validate_multiplex_types(signature, kwargs_, multiplex_args, no_key_args)

    if len(no_key_args) + len(multiplex_args) == 0:
        raise CustomMessageWiringError(f"No multiplexed inputs found")

    if len(multiplex_args) + len(direct_args) + len(pass_through_args) != len(kwargs_):
        raise CustomMessageWiringError(
            f"Unable to determine how to split inputs with args:\n {kwargs_}")

    if is_tsl := any(isinstance(v, HgTSLTypeMetaData) for v in input_types.values()):
        if not all(isinstance(input_types[k], HgTSLTypeMetaData) for k in multiplex_args):
            raise CustomMessageWiringError(
                f"Not all multiplexed inputs are of type TSL or TSD")

    if is_tsl:
        key_tp = _extract_tsl_size(kwargs_, multiplex_args, no_key_args)
    else:
        key_tp = _validate_tsd_keys(kwargs_, multiplex_args, no_key_args)

    return (multiplex_args, no_key_args, pass_through_args, direct_args, "TSL" if is_tsl else "TSD", key_tp if is_tsl
    else HgTSTypeMetaData(key_tp))


@dataclass(frozen=True)
class StubWiringPort(WiringPort):
    _value_tp: HgTypeMetaData = None

    @property
    def output_type(self) -> HgTypeMetaData:
        return self._value_tp

    @property
    def rank(self) -> int:
        return 1


def stub_wiring_port(value_tp: HgTimeSeriesTypeMetaData) -> WiringPort:
    return StubWiringPort(node_instance=None, _value_tp=value_tp)


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
            call_kwargs[key] = stub_wiring_port(arg.value_tp)
        elif key in (_KEYS, _INDEX):
            continue
        elif arg.is_scalar:
            call_kwargs[key] = kwargs_[key]
        else:
            call_kwargs[key] = stub_wiring_port(arg)
    if input_key_name:
        call_kwargs[input_key_name] = stub_wiring_port(input_key_tp)
    return call_kwargs


def _as_reference(tp_: HgTimeSeriesTypeMetaData, is_multiplexed: bool) -> HgTypeMetaData:
    if is_multiplexed:
        # If multiplexed type, we want references to the values not the whole output.
        if type(tp_) is HgTSDTypeMetaData:
            tp_: HgTSDTypeMetaData
            return HgTSDTypeMetaData(tp_.key_tp, HgREFTypeMetaData(tp_.value_tp))
        elif type(tp_) is HgTSLTypeMetaData:
            tp_: HgTSLTypeMetaData
            return HgTSLTypeMetaData(HgREFTypeMetaData(tp_.value_tp), tp_.size_tp)
        else:
            raise CustomMessageWiringError(f"Unable to create reference for multiplexed type: {tp_}")
    else:
        return HgREFTypeMetaData(tp_)


def _create_tsd_map_wiring_node(
        fn: WiringNodeClass,
        kwargs_: dict[str, WiringPort | SCALAR],
        input_types: dict[str, HgTypeMetaData],
        multiplex_args: frozenset[str],
        no_key_args: frozenset[str],
        input_key_tp: HgTSTypeMetaData,
        input_key_name: str | None
) -> TsdMapWiringNodeClass:
    # Resolve the mapped function signature
    stub_inputs = _prepare_stub_inputs(kwargs_, input_types, multiplex_args, no_key_args, input_key_tp, input_key_name)
    resolved_signature = fn.resolve_signature(**stub_inputs)

    reference_inputs = frozendict(
        {k: _as_reference(v, k in multiplex_args) if isinstance(v, HgTimeSeriesTypeMetaData) and k != _KEYS else v for
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
        active_inputs=frozenset({_KEYS, }) if (has_keys := _KEYS in input_types) else multiplex_args,
        valid_inputs=frozenset({_KEYS, }) if has_keys else frozenset(),
        unresolved_args=frozenset(),
        time_series_args=frozenset(k for k, v in input_types.items() if not v.is_scalar),
        uses_scheduler=False,
        label=f"map('{resolved_signature.signature}', {', '.join(input_types.keys())})",
        map_fn_signature=resolved_signature,
        key_tp=input_key_tp.value_scalar_tp,
        key_arg=input_key_name,
        keyable_args=frozenset(arg for arg in multiplex_args if arg not in no_key_args) if not has_keys else None,
        multiplexed_args=multiplex_args,
    )
    wiring_node = TsdMapWiringNodeClass(map_signature, fn)
    return wiring_node


def _create_tsl_map_signature(
        fn: WiringNodeClass,
        kwargs_: dict[str, WiringPort | SCALAR],
        input_types: dict[str, HgTypeMetaData],
        multiplex_args: frozenset[str],
        size_tp: HgAtomicType,
        input_key_name: str | None
):
    # Resolve the mapped function signature
    stub_inputs = _prepare_stub_inputs(kwargs_, input_types, multiplex_args, [], HgTSTypeMetaData.parse(TS[int]),
                                       input_key_name)
    resolved_signature = fn.resolve_signature(**stub_inputs)

    reference_inputs = frozendict(
        {k: _as_reference(v, k in multiplex_args) if isinstance(v, HgTimeSeriesTypeMetaData) and k != _INDEX else v for
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
        active_inputs=frozenset({_INDEX, }) if (has_keys := _INDEX in input_types) else multiplex_args,
        valid_inputs=frozenset({_INDEX, }) if has_keys else tuple(),
        unresolved_args=frozenset(),
        time_series_args=frozenset(k for k, v in input_types.items() if not v.is_scalar),
        uses_scheduler=False,
        label=f"map('{resolved_signature.signature}', {', '.join(input_types.keys())})",
        map_fn_signature=resolved_signature,
        size_tp=size_tp,
        key_arg=input_key_name,
        multiplexed_args=multiplex_args,
    )
    wiring_node = TsdMapWiringNodeClass(map_signature, fn)
    return wiring_node


def _validate_tsd_keys(kwargs_, multiplex_args, no_key_args):
    """
    Ensure all the multiplexed inputs use the same input key.
    """
    types = set(kwargs_[arg].output_type.key_tp for arg in chain(multiplex_args, no_key_args))
    if len(types) > 1:
        raise CustomMessageWiringError(
            f"The TSD multiplexed inputs have different key types: {types}")
    return next(iter(types))


def _validate_pass_through(signature: WiringNodeSignature, kwargs_, pass_through_args):
    """
    Validates that the pass through inputs are valid.
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
    sizes: [type[Size]] = [cast(HgTSLTypeMetaData, kwargs_[arg].output_type).size_tp.py_type for arg in
                           chain(multiplex_args,
                                 (m_arg for m_arg in marker_args if
                                  not isinstance(kwargs_[m_arg], _PassthroughMarker)))]
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
