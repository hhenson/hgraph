from dataclasses import dataclass
from itertools import chain
from typing import Callable, Optional, cast
from collections.abc import Set

__all__ = ("tsd_map", "pass_through", "no_key", "tsl_map", "tsd_reduce", "tsl_reduce")

from frozendict import frozendict

from hg._types._ts_meta_data import HgTSTypeMetaData
from hg._wiring._wiring_errors import NoTimeSeriesInputsError
from hg._wiring._wiring_context import WiringContext
from hg._wiring._wiring import WiringNodeSignature, WiringPort, HgTSLTypeMetaData, HgScalarTypeMetaData, \
    WiringNodeClass, TsdMapWiringNodeClass
from hg._types._tss_meta_data import HgTSSTypeMetaData
from hg._types._time_series_types import TIME_SERIES_TYPE
from hg._types._tsd_meta_data import HgTSDTypeMetaData
from hg._types._tsd_type import TSD
from hg._types._tsl_type import TSL
from hg._types._ts_type import TS
from hg._types._scalar_types import SIZE, SCALAR_1, SCALAR, STATE, Size
from hg._types._tss_type import TSS
from hg._wiring._wiring import extract_kwargs
from hg._wiring._wiring_errors import CustomMessageWiringError


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


@dataclass(frozen=True)
class TsdMapWiringSignature(WiringNodeSignature):
    map_fn_signature: WiringNodeSignature = None
    key_tp: HgScalarTypeMetaData = None
    key_arg: Optional[str] = None  # The arg name of the key in the map function is there is one
    key_simulated: Optional[
        Set[str]] = None  # If simulated then the list of input arguments to use when simulating the key is here
    pass_through: Optional[Set[str]] = None  # The inputs that do not need to be de-multiplexed.


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
        # We need to extract keys, we may be missing the key field, this will be computed and supplied if required.
        kwargs_ = extract_kwargs(signature, *args, _ensure_match=False, _allow_missing_count=1, **kwargs)
        _, _, _, _, map_type = _split_inputs(signature, kwargs_, validate_type=False)
        match map_type:
            case "TSD":
                return tsd_map(func, *args, **kwargs)
            case "TSL":
                return tsl_map(func, *args, **kwargs)
            case _:
                raise CustomMessageWiringError(f"Unable to determine map type for given inputs: {kwargs_}")


def _split_inputs(signature: WiringNodeSignature, kwargs_, validate_type: bool = False) \
        -> tuple[frozenset[str], frozenset[str], frozenset[str], frozenset[str], str, HgScalarTypeMetaData]:
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
    if non_time_inputs := [arg for arg in kwargs_ if not isinstance(kwargs_[arg], (WiringPort, _MappingMarker))]:
        raise CustomMessageWiringError(
            f" The following args are not time-series inputs, but should be: {non_time_inputs}")

    marker_args = frozenset(arg for arg in kwargs_ if isinstance(kwargs_[arg], _MappingMarker))
    pass_through_args = frozenset(arg for arg in marker_args if isinstance(kwargs_[arg], _PassthroughMarker))
    no_key_args = frozenset(arg for arg in marker_args if arg not in pass_through)

    if validate_type:
        _validate_pass_through(signature, kwargs_,
                               pass_through_args)  # Ensure the pass through args are correctly typed.

    input_types = {k: v.output_type for k, v in kwargs_.items()}

    direct_args = frozenset(k for k, v in input_types if k not in marker_args and signature.input_types[k].matches(v))

    multiplex_args = frozenset(
        k for k, v in input_types \
        if k not in marker_args and \
        k not in direct_args and \
        type(v) in (HgTSDTypeMetaData, HgTSLTypeMetaData)
    )

    if validate_type:
        _validate_multiplex_types(signature, kwargs_, multiplex_args, no_key_args)

    if len(marker_args) + len(multiplex_args) == 0:
        raise CustomMessageWiringError(f"No multiplexed inputs found")

    if len(multiplex_args) + len(direct_args) + len(marker_args) != len(kwargs_):
        raise CustomMessageWiringError(
            f"Unable to determine how to split inputs with args:\n {kwargs_}")

    if is_tsl := any(isinstance(v, HgTSLTypeMetaData) for v in input_types.values()):
        if not all(isinstance(v, HgTSLTypeMetaData) for v in input_types.values()):
            raise CustomMessageWiringError(
                f"Not all multiplexed inputs are of type TSL or TSD")

    key_tp = None
    if validate_type:
        if is_tsl:
            _validate_tsd_keys(kwargs_, multiplex_args, no_key_args)
            key_tp = kwargs_[next(iter(chain(multiplex_args, no_key_args)))].output_type.key_tp
        else:
            key_tp = _extract_tsl_size(kwargs_, multiplex_args, no_key_args)

    return multiplex_args, no_key_args, pass_through_args, direct_args, "TSL" if is_tsl else "TSD", key_tp


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


def _validate_multiplex_types(signature: WiringNodeSignature, kwargs_, multiplex_args, no_key_args):
    """
    Validates that the multiplexed inputs are valid.
    """
    for arg in chain(multiplex_args, no_key_args):
        if not (in_type := signature.input_types[arg]).matches((m_type := kwargs_[arg].output_type).value_tp):
            raise CustomMessageWiringError(
                f"The input '{arg}: {m_type}' is a multiplexed type, "
                f"but its '{m_type.value_tp}' is not compatible with the input type: {in_type}")


def _validate_tsd_keys(kwargs_, multiplex_args, no_key_args):
    """
    Ensure all the multiplexed inputs use the same input key.
    """
    types = set(kwargs_[arg].output_type.key_tp for arg in chain(multiplex_args, no_key_args))
    if len(types) > 1:
        raise CustomMessageWiringError(
            f"The TSD multiplexed inputs have different key types: {types}")


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


def tsd_map(func: Callable[[...], TIME_SERIES_TYPE], *args, keys: Optional[TSS[SCALAR]] = None, **kwargs) \
        -> TSD[SCALAR, TIME_SERIES_TYPE]:
    """
    The ability to demultiplex a stream of TSD values and process the associated time-series values against the func
    provided. The func can take a first argument of type TS[SCALAR] which will be mapped to the key of the TSD's been
    de-multiplexed. If the first argument is not of type TS[SCALAR] or the name of the first argument matches kwargs
    supplied (or the count of *args + **kwargs is equivalent to th number of arguments of the function) then the key
    will not be mapped into the first argument.

    If the keys kwarg is not supplied:
    #. The keys type is inferred from the types of the TSDs supplied for the function's inputs (that are not marked as
       pass through).
    #. The keys will be taken as the union of the keys of the TSDs supplied, that no not marked as pass through or
       no_key.

    Example:
        lhs: TSD[str, int] = ...
        rhs: TSD[str, int] = ...
        out = tsd_map(add_, lhs, rhs)
    """
    if not isinstance(func, WiringNodeClass):
        raise RuntimeError(f"The supplied function is not a graph or node function: '{func.__name__}'")
    with WiringContext(current_signature=STATE(current_signature=f"tsd_map('{func.signature.signature}', ...)")):
        if len(args) + len(kwargs) == 0:
            raise NoTimeSeriesInputsError()
        signature: WiringNodeSignature = func.signature
        # We need to extract keys, we may be missing the key field, this will be computed and supplied if required.
        kwargs_ = extract_kwargs(signature, *args, _ensure_match=False, _allow_missing_count=1, **kwargs)
        multiplex_args, no_key_args, pass_through_args, direct_args, map_type, key_tp = _split_inputs(signature,
                                                                                                      kwargs_)
        if map_type != "tsd":
            raise CustomMessageWiringError(f"The multiplexed inputs are not suitable for tsd_map'")
        keys = kwargs.get("keys")
        if keys is not None:
            if not isinstance(keys, WiringPort):
                raise CustomMessageWiringError(
                    f"The 'keys' argument supplied has a value that is not a time-series input: '{keys}'")
            if not isinstance(keys_tp := keys.output_type, (HgTSSTypeMetaData, HgTSDTypeMetaData)):
                raise RuntimeError(f"The supplied 'keys' argument is not a TSS/TSD type, got: '{keys_tp}'")
            key_tp_ = keys_tp.value_scalar_tp if keys_tp is HgTSSTypeMetaData else keys_tp.key_tp
            if not key_tp_.matches(key_tp):
                raise CustomMessageWiringError(
                    f"The 'keys' argument supplied has a type that does not match the expected type: '{key_tp}'")
            key_simulated = None
        else:
            key_simulated = multiplex_args

        if len(kwargs_) == len(signature.input_types) - 1:
            # Assume the missing input must be an input for the key.
            key_arg = next(iter(signature.input_types.keys() - kwargs_.keys()))
            if not (s_key_tp := signature.input_types[key_arg]).matches(HgTSTypeMetaData(key_tp)):
                raise CustomMessageWiringError(
                    f"The key argument '{key_arg}: {s_key_tp}' does not match the expected type: 'TS[{key_tp}]'")
        else:
            key_arg = None

        # For all time-series inputs, use the provided input values, for scalars use the function signature.
        input_types = {k: v.output_type if isinstance(v, (WiringPort, _MappingMarker)) else signature.input_types[k] for
                       k, v in kwargs_.items()}
        # TODO: Need to resolve scalars if they are not resolved in the signature.

        map_signature = TsdMapWiringSignature(
            node_type=signature.node_type,
            name="map",
            args=tuple('keys') + signature.args if keys else signature.args,
            input_types=input_types,
            output_type=HgTSDTypeMetaData(key_tp, signature.output_type) if signature.output_type else None,
            src_location=signature.src_location,  # TODO: Figure out something better for this.
            active_inputs=frozenset('keys') | (
                signature.active_inputs if signature.active_inputs else signature.time_series_args) \
                if not key_simulated else signature.active_inputs,
            valid_inputs=frozenset('keys') | (
                signature.valid_inputs if signature.valid_inputs else signature.time_series_args) \
                if not key_simulated else signature.valid_inputs,
            unresolved_args=tuple(),
            time_series_args=tuple('keys') + signature.time_series_args \
                if not key_simulated else signature.time_series_args,
            uses_scheduler=False,
            label=signature.label if signature.label else f"map('{func.signature.signature}', ...)",
            map_fn_signature=func.signature,
            key_tp=key_tp,
            key_arg=key_arg,
            key_simulated=key_simulated,
            pass_through=pass_through_args
        )
        wiring_node = TsdMapWiringNodeClass(map_signature, None)
        calling_kwargs = {k: kwargs[k].value if k in pass_through_args or k in no_key_args else kwargs[k] for k in
                          kwargs}
        if keys:
            calling_kwargs["keys"] = keys

        return wiring_node(**calling_kwargs)


def tsl_map(func: Callable[[...], TIME_SERIES_TYPE], *args,
            index: Optional[TSL[TS[bool], SIZE] | TS[tuple[int, ...]]] = None, **kwargs) -> TSL[TIME_SERIES_TYPE, SIZE]:
    """
    The ability to demultiplex a stream of TSL values and apply the de-multiplexed time-series values against the func
    provided. In this case the inputs of the functions should either be TSL values with the associated time-series
    being the same as the associated input or be the type of the input. This will attempt to determine the expected
    size of the output either using the index kwarg or by using the size of the input TSLs.

    If any of the inputs are fixed sized, then the output will be the smallest fixed size. If the index is fixed size
    then all TSL inputs must be at least that size. If the index is not fixed size, but any of the inputs are fixed,
    the output will still be set to the size of the smallest input.
    """
    if not isinstance(func, WiringNodeClass):
        raise RuntimeError(f"The supplied function is not a graph or node function: '{func.__name__}'")
    with WiringContext(current_signature=STATE(current_signature=f"tsl_map('{func.signature.signature}', ...)")):
        if len(args) + len(kwargs) == 0:
            raise NoTimeSeriesInputsError()
        signature: WiringNodeSignature = func.signature
        # We need to extract keys, we may be missing the key field, this will be computed and supplied if required.
        kwargs_ = extract_kwargs(signature, *args, _ensure_match=False, _allow_missing_count=1, **kwargs)


def tsd_reduce(func: Callable[[TIME_SERIES_TYPE, TIME_SERIES_TYPE], TIME_SERIES_TYPE],
               tsd: TSD[SCALAR, TIME_SERIES_TYPE], zero: SCALAR_1) \
        -> TIME_SERIES_TYPE:
    """
    The ability to reduce a stream of TSD values and process the associated time-series values against the func
    provided. The entries of the tsd as reduced one with another until a single value is produced. If only one
    value is present, the assumption is that the value is the desired output. If no values are present, the zero
    value is used as the return value. The func must be associative and commutative as order of evaluation is
    not assured.

    Example:
        tsd: TSD[str, TS[int]] = ...
        out = tsd_reduce(add_, tsd, 0)
    """


def tsl_reduce(func: Callable[[TIME_SERIES_TYPE, TIME_SERIES_TYPE], TIME_SERIES_TYPE], tsl: TSL[TIME_SERIES_TYPE, SIZE],
               zero: SCALAR_1, is_associated: bool = True) -> TIME_SERIES_TYPE:
    """
    The ability to reduce a stream of TSL values and process the associated time-series values against the func.
    The structure of computation is dependent on the is_associated flag. If False, then the time-series values
    are processed in order. If True, then the time-series values are processed in a tree structure.

    Example:
        tsl: TSL[TS[int], SIZE] = ...
        out = tsl_reduce(add_, tsl, 0)
        >> tsl <- [1, 2, 3, 4, 5]
        >> out <- 15
    """
