"""Service/adaptor decorators, impl registration and the wiring-scope
``context``."""
import inspect
import itertools
import typing

import _hgraph

from .._types import (wiring_signature_of as _wiring_signature_of,
                      _GenericTsExpr, _TsExpr, _TypeVarSentinel,
                      TSB, TimeSeriesSchema, _pattern_of,
                      _type_var_is_scalar, _type_var_name, _value_type)
from ._core import (
    WiringError,
    WiringPort,
    _current_wiring,
    _unwrap,
    _wiring_stack,
    wire,
)
from ._graph import _wrap_graph_fn
from ._markers import _INJECTABLE_MARKERS
from ._node import _PyNode, _warn_deprecated
from ._resolution import (_apply_resolvers, _python_value_for_binding,
                          _resolution_binding)


_TS_ANNOTATIONS = (_TsExpr, _GenericTsExpr)
_SERVICE_ADAPTOR_CLIENT_TOKENS = itertools.count()
_CLIENT_CONFIGS = {}
_CLIENT_CONFIG_CLEANUPS = set()
_PARAMETERIZED_SERVICE_ADAPTOR_REGISTRATIONS = {}
_SERVICE_ADAPTOR_SPLIT_PATHS = {}
_SERVICE_IMPLEMENTATION_PATH_STACK = []


class _ClientConfigRecord(typing.NamedTuple):
    values: dict
    logical_path: str
    variant_path: str


class _ParameterizedServiceAdaptorRegistration(typing.NamedTuple):
    implementation: object
    config: dict
    paths: set


class _PreparedAdaptorClient(typing.NamedTuple):
    stub: object
    logical_path: str
    concrete_path: str
    request: object


class _ServiceAdaptorSplitPath(typing.NamedTuple):
    request_id: object
    logical_path: str
    concrete_path: str


def _is_ts_annotation(annotation):
    return (
        isinstance(annotation, _TS_ANNOTATIONS)
        or (
            isinstance(annotation, (_TypeVarSentinel, typing.TypeVar))
            and not _type_var_is_scalar(annotation)
        )
    )


def _is_resolution_annotation(annotation):
    """A ``type[T]`` generic parameter is resolved by wiring, not supplied
    independently by adaptor clients as scalar configuration."""
    args = typing.get_args(annotation)
    return (
        typing.get_origin(annotation) is type
        and bool(args)
        and isinstance(args[0], (_TypeVarSentinel, typing.TypeVar))
    )


_FLAVOUR_LABELS = {
    "reference": "reference service",
    "subscription": "subscription service",
    "request_reply": "request/reply service",
    "adaptor": "adaptor",
    "service_adaptor": "service adaptor",
}


def _flavour_label(flavour):
    """Human-readable name for a flavour, for diagnostics.

    ``flavour`` alone reads oddly for services ("reference 'x' clients"), and
    these messages became service-visible when client scalar options were
    lifted onto the service surface (RFC 0011 step 1).
    """
    return _FLAVOUR_LABELS.get(flavour, flavour.replace("_", " "))


def _client_config_values(stub, bound):
    return {
        parameter.name: bound.arguments[parameter.name]
        for parameter in stub._signature.parameters.values()
        if parameter.name != "path"
        and not _is_ts_annotation(parameter.annotation)
        and not _is_resolution_annotation(parameter.annotation)
        and parameter.annotation not in _INJECTABLE_MARKERS
    }


def _has_client_config(stub):
    return any(
        parameter.name != "path"
        and not _is_ts_annotation(parameter.annotation)
        and not _is_resolution_annotation(parameter.annotation)
        and parameter.annotation not in _INJECTABLE_MARKERS
        for parameter in stub._signature.parameters.values()
    )


def _client_state_key(identity, stub, path):
    return (
        identity, stub.flavour, stub.__name__,
        stub._specialization, path,
    )


def _retain_client_state_cleanup(wiring, identity):
    if identity in _CLIENT_CONFIG_CLEANUPS:
        return

    def clear_configs():
        # The C++ Wiring may outlive this module: at interpreter shutdown
        # module globals are cleared to None, and the cleanup still fires.
        # Nothing is left to release at that point, so bail out quietly
        # rather than raising out of a destructor.
        if _CLIENT_CONFIGS is None or _CLIENT_CONFIG_CLEANUPS is None:
            return
        for existing in tuple(_CLIENT_CONFIGS):
            if existing[0] == identity:
                del _CLIENT_CONFIGS[existing]
        if _PARAMETERIZED_SERVICE_ADAPTOR_REGISTRATIONS is not None:
            for existing in tuple(_PARAMETERIZED_SERVICE_ADAPTOR_REGISTRATIONS):
                if existing[0] == identity:
                    del _PARAMETERIZED_SERVICE_ADAPTOR_REGISTRATIONS[existing]
        if _SERVICE_ADAPTOR_SPLIT_PATHS is not None:
            for existing in tuple(_SERVICE_ADAPTOR_SPLIT_PATHS):
                if existing[0] == identity:
                    del _SERVICE_ADAPTOR_SPLIT_PATHS[existing]
        _CLIENT_CONFIG_CLEANUPS.discard(identity)

    # The C++ Wiring owns this callback. A temporary borrowed PyWiring can
    # disappear before build_services(), but its underlying Wiring retains
    # both the config and cleanup for its actual lifetime.
    wiring._retain_cleanup(clear_configs)
    _CLIENT_CONFIG_CLEANUPS.add(identity)


def _record_client_config(stub, path, bound, *, variant_path=None):
    """Record the wiring-time scalar options shared by one service or
    adaptor path. Flavour-neutral unless ``variant_path`` is supplied for a
    service adaptor: released hgraph materializes one native adaptor instance
    per distinct scalar configuration at the same logical path."""
    config = _client_config_values(stub, bound)
    wiring = _wiring_stack[0] if _wiring_stack else _current_wiring()
    identity = wiring.identity()
    _retain_client_state_cleanup(wiring, identity)

    if variant_path is not None and config:
        logical_path = path
        # Variant identity belongs to the logical service-adaptor path, not to
        # one interface stub. A multi-interface implementation must therefore
        # route equal configurations to the same concrete path even when its
        # interfaces are wired in different orders.
        variants = {}
        for existing, record in _CLIENT_CONFIGS.items():
            if (
                existing[0] == identity
                and existing[1] == "service_adaptor"
                and record.logical_path == logical_path
                and record.variant_path == variant_path
            ):
                variants.setdefault(existing[4], record)
        for concrete_path, record in variants.items():
            if record.values == config:
                key = _client_state_key(identity, stub, concrete_path)
                previous = _CLIENT_CONFIGS.setdefault(
                    key,
                    _ClientConfigRecord(config, logical_path, variant_path),
                )
                if previous.values != config:
                    raise WiringError(
                        f"service adaptor '{stub.__name__}' clients at path "
                        f"{logical_path!r} disagree on wiring-time options")
                _materialize_parameterized_service_adaptor(
                    stub, variant_path, concrete_path, wiring)
                return concrete_path

        # The qualifier is an internal native path identity. Values stay in
        # the Python-owned wiring record, so arbitrary valid scalar options do
        # not need a lossy string serialization. Equal configurations reuse
        # the same concrete C++ path and therefore the same implementation.
        variant_index = len(variants)
        concrete_path = f"{variant_path}[__config__={variant_index}]"
        while concrete_path in variants:
            variant_index += 1
            concrete_path = f"{variant_path}[__config__={variant_index}]"
        key = _client_state_key(identity, stub, concrete_path)
        _CLIENT_CONFIGS[key] = _ClientConfigRecord(
            config, logical_path, variant_path)
        _materialize_parameterized_service_adaptor(
            stub, variant_path, concrete_path, wiring)
        return concrete_path

    key = _client_state_key(identity, stub, path)
    previous = _CLIENT_CONFIGS.setdefault(
        key, _ClientConfigRecord(config, path, path)).values
    if previous != config:
        differences = sorted(
            name for name in previous.keys() | config.keys()
            if previous.get(name) != config.get(name)
        )
        raise WiringError(
            f"{_flavour_label(stub.flavour)} '{stub.__name__}' clients at "
            f"path {path!r} disagree on wiring-time option(s) {differences!r}")
    # A generic service adaptor's native path carries its type-specialization
    # suffix even when it has no scalar configuration. ``path`` is the
    # suffix-free lookup key used by the implementation binding.
    return variant_path if variant_path is not None else path


def _client_config_record(stub, path):
    wiring = _wiring_stack[0] if _wiring_stack else _current_wiring()
    return _CLIENT_CONFIGS.get(
        _client_state_key(wiring.identity(), stub, path))


def _resolved_client_path(stub, path):
    """Return the config key and concrete native client path."""
    resolved = stub._resolved_path(path) if path else stub._default_path
    resolved = resolved or f"{stub.__name__}_default"
    config_path = resolved
    specialization = getattr(stub, "_specialization", "")
    suffix = f"[{specialization}]" if specialization else ""
    if suffix and config_path.endswith(suffix):
        config_path = config_path[:-len(suffix)]
    return config_path, resolved


def _split_client_path_key(identity, stub, request_id):
    raw_request_id = _unwrap(request_id)
    return (
        identity, stub.flavour, stub.__name__, stub._specialization,
        id(raw_request_id),
    ), raw_request_id


def _remember_service_adaptor_split_path(
    stub, request_id, logical_path, concrete_path,
):
    wiring = _wiring_stack[0] if _wiring_stack else _current_wiring()
    identity = wiring.identity()
    _retain_client_state_cleanup(wiring, identity)
    key, raw_request_id = _split_client_path_key(identity, stub, request_id)
    record = _ServiceAdaptorSplitPath(
        raw_request_id, logical_path, concrete_path)
    previous = _SERVICE_ADAPTOR_SPLIT_PATHS.setdefault(key, record)
    if previous.request_id is not raw_request_id \
            or previous.logical_path != logical_path \
            or previous.concrete_path != concrete_path:
        raise WiringError(
            f"service adaptor '{stub.__name__}' request id is already bound "
            f"to path {previous.logical_path!r}")


def _service_adaptor_split_path(stub, request_id, logical_path, fallback):
    wiring = _wiring_stack[0] if _wiring_stack else _current_wiring()
    key, raw_request_id = _split_client_path_key(
        wiring.identity(), stub, request_id)
    record = _SERVICE_ADAPTOR_SPLIT_PATHS.get(key)
    if record is None or record.request_id is not raw_request_id:
        return fallback
    if record.logical_path != logical_path:
        raise WiringError(
            f"service adaptor '{stub.__name__}' request id was bound at "
            f"path {record.logical_path!r}, not {logical_path!r}")
    return record.concrete_path


def _resolved_service_path(stub, path):
    if path is None:
        path = getattr(stub, "_default_path", "")
    if hasattr(stub, "is_full_path") and stub.is_full_path(path):
        return path
    resolver = getattr(stub, "_resolved_path", None)
    return resolver(path) if resolver is not None else path


def _service_implementation_path_key(stub, path):
    return (
        stub.flavour, stub.__name__,
        getattr(stub, "_specialization", ""), path,
    )


def _resolved_implementation_path(stub, path):
    resolved = _resolved_service_path(stub, path)
    key = _service_implementation_path_key(stub, resolved)
    for bindings in reversed(_SERVICE_IMPLEMENTATION_PATH_STACK):
        concrete = bindings.get(key)
        if concrete is not None:
            return concrete
    return resolved


def _apply_service_resolvers(resolution, resolvers, scalar_values=None):
    if resolution is None:
        resolution = _hgraph.ResolutionScope()
    return _apply_resolvers(resolution, resolvers, scalar_values)


def _specialization_label(resolution):
    segments = []
    for name, value in resolution.bindings.items():
        label = value.name if isinstance(value, _hgraph.ValueType) else repr(value)
        segments.append(f"{name}={label}")
    return ",".join(sorted(segments))


def _specialization(item, owner, signature, resolvers=None):
    items = item if isinstance(item, tuple) else (item,)
    resolution = _hgraph.ResolutionScope()
    variables = {}
    for binding in items:
        if not isinstance(binding, slice) or binding.step is not None:
            raise TypeError(
                f"{owner} specialization requires TYPEVAR: concrete entries")
        variable, concrete = binding.start, binding.stop
        if not isinstance(variable, (_TypeVarSentinel, typing.TypeVar)):
            raise TypeError(f"{owner} specialization key is not a type variable")
        name = _type_var_name(variable)
        variables[name] = variable
        if _type_var_is_scalar(variable):
            meta = _value_type(concrete)
            constraints = tuple(getattr(variable, "__constraints__", ()))
            if constraints and all(meta != _value_type(constraint) for constraint in constraints):
                allowed = ", ".join(getattr(value, "__name__", repr(value)) for value in constraints)
                raise TypeError(f"{name} must be one of {allowed}, got {meta.name}")
            resolution.bind_scalar(name, meta)
        else:
            if isinstance(concrete, _TsExpr):
                meta = concrete.handle
            elif isinstance(concrete, type) and issubclass(concrete, TimeSeriesSchema):
                meta = TSB[concrete].handle
            else:
                meta = concrete
            resolution.bind_ts(name, meta)
    resolution = _resolve_service_signature(
        signature, resolvers, resolution=resolution)
    return resolution, _specialization_label(resolution), variables


def _service_type_variables(signature):
    """Return public Python TypeVars retained by a service signature."""
    found = {}

    def visit(annotation):
        if isinstance(annotation, (_TypeVarSentinel, typing.TypeVar)):
            found.setdefault(_type_var_name(annotation), annotation)
            return
        if isinstance(annotation, _GenericTsExpr):
            for variable in annotation.variables:
                visit(variable)
            return
        for argument in typing.get_args(annotation):
            visit(argument)

    for parameter in signature.parameters.values():
        visit(parameter.annotation)
    visit(signature.return_annotation)
    return tuple(found.values())


def _copy_service_resolution(resolution):
    """Return a call-local copy of a native service resolution scope."""
    copied = _hgraph.ResolutionScope()
    if resolution is None:
        return copied
    for name, value in resolution.bindings.items():
        if isinstance(value, _hgraph.TsType):
            copied.bind_ts(name, value)
        elif isinstance(value, _hgraph.ValueType):
            copied.bind_scalar(name, value)
        elif isinstance(value, int) and not isinstance(value, bool):
            copied.bind_size(name, value)
        else:  # pragma: no cover - ResolutionScope exposes only these kinds
            raise TypeError(
                f"unsupported service resolution binding {name}={value!r}")
    return copied


def _service_needs_resolution(stub):
    """Whether transport or type-only interface variables remain unresolved.

    A concrete transport descriptor is not sufficient: ``type[T]`` parameters
    can still require call-time scalar values before a resolver can bind them.
    Keep that distinction in one predicate for clients and registrations.
    """
    if getattr(stub, "descriptor", None) is None:
        return True
    signature = getattr(stub, "_signature", None)
    if signature is None:
        return False
    resolution = getattr(stub, "_resolution", None)
    return any(
        resolution is None
        or not resolution.is_resolved(_type_var_name(variable))
        for variable in _service_type_variables(signature)
    )


def _inferred_specialization(fn, request_annotation, request, resolvers=None):
    resolution = _hgraph.ResolutionScope()
    actual = _unwrap(request).ts_type
    if not resolution.match(_pattern_of(request_annotation), actual):
        raise TypeError(
            f"generic adaptor '{fn.__name__}' request does not match its type pattern")
    resolution = _resolve_service_signature(
        _wiring_signature_of(fn)[0], resolvers,
        resolution=resolution)
    specialization = _specialization_label(resolution)
    if not specialization:
        raise TypeError(
            f"generic adaptor '{fn.__name__}' could not infer its type specialization")
    return resolution, specialization


def _resolve_annotation(annotation, resolution):
    if isinstance(annotation, _TsExpr):
        return annotation.handle
    if isinstance(annotation, (_GenericTsExpr, _TypeVarSentinel, typing.TypeVar)) and resolution is not None:
        return resolution.resolve_ts(_pattern_of(annotation))
    return None


def _apply_service_defaults(signature, resolution):
    """Seed type-valued interface defaults after request matching.

    Generic service signatures use defaults such as
    ``tp: Type[TIME_SERIES_TYPE] = TS[KEYABLE_SCALAR]``.  The right hand
    side becomes concrete only after another argument has bound
    ``KEYABLE_SCALAR``.
    """
    if resolution is None:
        return None
    import typing

    from .._types import AUTO_RESOLVE

    for parameter in signature.parameters.values():
        args = typing.get_args(parameter.annotation)
        if typing.get_origin(parameter.annotation) is not type or not args:
            continue
        sentinel = args[0]
        if not isinstance(sentinel, _TypeVarSentinel):
            continue
        name = _type_var_name(sentinel)
        if resolution.is_resolved(name):
            continue
        default = parameter.default
        if default in (inspect.Parameter.empty, AUTO_RESOLVE):
            continue
        if isinstance(default, _GenericTsExpr):
            resolved = resolution.resolve_ts(_pattern_of(default))
            if resolved is not None:
                resolution.bind_ts(name, resolved)
        elif isinstance(default, (_TypeVarSentinel, typing.TypeVar)):
            default_name = _type_var_name(default)
            if (resolved := resolution.find_scalar(default_name)) is not None:
                resolution.bind_scalar(name, resolved)
            elif (resolved := resolution.find_ts(default_name)) is not None:
                resolution.bind_ts(name, resolved)
            elif (resolved := resolution.find_size(default_name)) is not None:
                resolution.bind_size(name, resolved)
        else:
            _PyNode._bind_resolved(resolution, name, default)
    return resolution


def _service_scalar_values(signature, bound):
    """Return the bound wiring-time scalars available to resolvers.

    Time-series arguments may still be raw Python values at this point, so
    select by the declared interface annotation rather than by runtime value.
    This matches graph/node resolution: supplied values and applied defaults
    are both visible by their declared parameter name.
    """
    return {
        parameter.name: bound.arguments[parameter.name]
        for parameter in signature.parameters.values()
        if parameter.name in bound.arguments
        and not _is_ts_annotation(parameter.annotation)
        and parameter.annotation not in _INJECTABLE_MARKERS
    }


def _resolve_service_signature(
    signature,
    resolvers,
    *,
    resolution=None,
    request_params=(),
    requests=(),
    scalar_values=None,
    owner="service or adaptor",
):
    """Apply the common interface resolution order in one place.

    Explicit bindings or request inference run first, type-valued defaults run
    second, and only then do unresolved user resolvers receive the bound scalar
    values. This is the same ordering used by nodes, graphs, and operators.
    """
    if resolution is None:
        resolution = _hgraph.ResolutionScope()
    for parameter, request in zip(request_params, requests):
        if not resolution.match(
                _pattern_of(parameter.annotation), _unwrap(request).ts_type):
            raise TypeError(
                f"generic {owner} request does not match its type pattern")
    _apply_service_defaults(signature, resolution)
    return _apply_service_resolvers(resolution, resolvers, scalar_values)


class context:
    """Publish a port as a named context for the wiring scope within:
    ``with hg.context("name", port): ...``; consume with
    ``hg.context.get("name")`` / test with ``hg.context.has("name")``.
    Same-wiring only (the design record's semantics)."""

    def __init__(self, name, port):
        self._name, self._port = name, port

    def __enter__(self):
        _hgraph.push_context(_current_wiring(), self._name, _unwrap(self._port))
        return self

    def __exit__(self, *exc):
        _hgraph.pop_context()
        return False

    @staticmethod
    def get(name):
        return WiringPort(_hgraph.get_context(_current_wiring(), name))

    @staticmethod
    def has(name):
        return _hgraph.has_context(_current_wiring(), name)


class _GetContext:
    """hgraph's ``get_context`` free function: ``get_context[TS[str]]("name")``
    or ``get_context("name")``. The subscript documents the expected type -
    this runtime's contexts are name-based (recorded deviation), so the
    published port carries its own type and the subscript is not needed for
    resolution."""

    __slots__ = ("_tp",)

    def __init__(self, tp=None):
        self._tp = tp

    def __getitem__(self, tp):
        return _GetContext(tp)

    def __call__(self, name, tp_=None, required=False):
        if context.has(name):
            return context.get(name)

        # ``with port as name`` is the Python spelling of a published
        # context. It predates the explicit C++ context helper, but both must
        # resolve to the same source, including while a nested service graph
        # is being compiled.
        expected = tp_ or self._tp
        published = None
        if expected is not None:
            from .._types import _ContextExpr, _TsExpr, TS
            from ._core import _resolve_context

            ts_type = expected if isinstance(expected, _TsExpr) else TS[expected]
            published = _resolve_context(_ContextExpr(ts_type), name)
        else:
            from ._core import _context_name_of, _published_contexts

            for port, _, frame, _ in reversed(_published_contexts):
                if _context_name_of(port, frame) == name:
                    published = port
                    break
        if published is None:
            if required:
                from ._core import WiringError

                raise WiringError(f"Context variable for {name} is required but not found")
            return None
        return published


get_context = _GetContext()


_SERVICE_RESOLUTIONS = {}


def _remember_service_resolution(stub):
    if stub.descriptor is None:
        return
    resolution = getattr(stub, "_resolution", None)
    variables = dict(getattr(stub, "_resolution_variables", {}))
    variables.update({
        _type_var_name(variable): variable
        for variable in _service_type_variables(stub._signature)
    })
    _SERVICE_RESOLUTIONS[(stub.flavour, stub.__name__, stub._specialization)] = (
        {variables.get(name, name): value
         for name, value in resolution.bindings.items()}
        if resolution is not None else {}
    )


class _ServiceStub:
    """A service interface stub (hgraph's service decorators): calling it
    wires a CLIENT; register_service registers an implementation."""

    def __init__(self, fn, flavour, *, resolution=None, specialization="",
                 resolvers=None, deprecated=False, pending_registrations=None,
                 registered_resolutions=None, resolution_variables=None):
        self.fn = fn
        self.__name__ = fn.__name__
        self.flavour = flavour
        self._specialization = specialization
        self._resolution_variables = resolution_variables or {}
        self._resolvers = dict(resolvers) if resolvers else None
        self._deprecated = deprecated
        self._pending_registrations = (
            pending_registrations if pending_registrations is not None else []
        )
        self._registered_resolutions = (
            registered_resolutions if registered_resolutions is not None else []
        )
        self._signature, self._default_type_var = _wiring_signature_of(fn)
        self._request_params = tuple(
            p for p in self._signature.parameters.values()
            if _is_ts_annotation(p.annotation)
        )
        self._request_annotation = (
            self._request_params[0].annotation if self._request_params else None
        )
        # Interface construction is descriptive. Resolution happens only once
        # explicit bindings, inferred request types, and call scalars are
        # available; running a resolver here gives services a different order
        # from nodes, graphs, and adaptors.
        self._resolution = (
            resolution if resolution is not None
            else _hgraph.ResolutionScope()
        )
        out = self._signature.return_annotation
        reply_less = (
            flavour == "request_reply"
            and out in (inspect.Signature.empty, None)
        )
        if not reply_less and not _is_ts_annotation(out):
            raise TypeError(f"@{flavour}_service '{self.__name__}' requires a time-series return annotation")
        path_param = self._signature.parameters.get("path")
        default_path = (
            path_param.default
            if path_param is not None and isinstance(path_param.default, str)
            else f"{fn.__name__}_default"
        )
        if specialization:
            default_path = f"{default_path}[{specialization}]"
        self._default_path = default_path
        kwargs = {
            "name": fn.__name__,
            "flavour": flavour,
            "default_path": default_path,
            "specialization": specialization,
        }

        if flavour == "reference":
            kwargs["output"] = _resolve_annotation(out, self._resolution)
        elif flavour == "subscription":
            if len(self._request_params) != 1:
                raise TypeError(f"@subscription_service '{self.__name__}' needs a TS[key] parameter")
            kwargs["key_ts"] = _resolve_annotation(self._request_params[0].annotation, self._resolution)
            kwargs["value"] = _resolve_annotation(out, self._resolution)
        elif flavour == "request_reply":
            if not self._request_params:
                raise TypeError(f"@request_reply_service '{self.__name__}' needs a request parameter")
            request_fields = [
                (parameter.name, _resolve_annotation(parameter.annotation, self._resolution))
                for parameter in self._request_params
            ]
            if all(field_type is not None for _, field_type in request_fields):
                kwargs["request"] = (
                    request_fields[0][1]
                    if len(request_fields) == 1
                    else _hgraph.un_named_tsb_type(request_fields)
                )
            else:
                kwargs["request"] = None
            if not reply_less:
                kwargs["response"] = _resolve_annotation(out, self._resolution)
        unresolved = [name for name, value in kwargs.items()
                      if name in {"output", "key_ts", "value", "request", "response"}
                      and value is None]
        self._request_type = kwargs.get("request")
        self.descriptor = None if unresolved else _hgraph.service_descriptor(**kwargs)
        _remember_service_resolution(self)

    def __getitem__(self, item):
        if not _service_needs_resolution(self) and not self._specialization:
            raise TypeError(f"service '{self.__name__}' is not generic")

        items = item if isinstance(item, tuple) else (item,)
        if not all(isinstance(binding, slice) for binding in items):
            variables = [
                variable for variable in _service_type_variables(self._signature)
                if self._resolution is None
                or _type_var_name(variable) not in self._resolution.bindings
            ]
            if len(variables) != 1 or len(items) != 1:
                raise TypeError(
                    f"service '{self.__name__}' cannot infer which type variable "
                    f"to bind; use TYPEVAR: concrete")
            item = slice(variables[0], items[0])

        resolution, specialization, variables = _specialization(
            item, f"service '{self.__name__}'", self._signature,
            self._resolvers)
        result = _ServiceStub(
            self.fn, self.flavour, resolution=resolution,
            specialization=specialization, resolvers=self._resolvers,
            deprecated=self._deprecated,
            pending_registrations=self._pending_registrations,
            registered_resolutions=self._registered_resolutions,
            resolution_variables=variables)
        if _service_needs_resolution(result):
            raise TypeError(
                f"service '{self.__name__}' specialization leaves an unresolved type")
        return result

    def _require_descriptor(self):
        if self.descriptor is None:
            raise TypeError(
                f"generic service '{self.__name__}' must be specialized, for example "
                f"{self.__name__}[NUMBER:int]")
        return self.descriptor

    def _resolved_path(self, path):
        if not path or not self._specialization:
            return path
        suffix = f"[{self._specialization}]"
        return path if path.endswith(suffix) else f"{path}{suffix}"

    def default_path(self):
        """Return the default user path declared by this service interface."""
        return self._default_path

    @property
    def implementation_arity(self):
        return len(self._request_params)

    def _bind_call(self, args, kwargs):
        kwargs = dict(kwargs)
        external_path = kwargs.pop("path", "") if "path" not in self._signature.parameters else None
        bound = self._signature.bind(*args, **kwargs)
        bound.apply_defaults()
        path = (
            bound.arguments.get("path")
            if "path" in self._signature.parameters
            else external_path
        )
        if path is None:
            path = ""
        if not isinstance(path, str):
            raise TypeError(f"service '{self.__name__}' path must be a string")
        # ``bound`` carries the client's wiring-time scalar options; the caller
        # records them so the registered implementation can read them back
        # (RFC 0011 step 1 - the same channel adaptor clients have always had).
        return path, [bound.arguments[p.name] for p in self._request_params], bound

    def __call__(self, *args, **kwargs):
        _warn_deprecated(self.__name__, self._deprecated)
        path, requests, bound = self._bind_call(args, kwargs)
        if requests:
            from ._node import _lift_time_series_argument

            requests = [
                request
                if isinstance(request, WiringPort)
                else _lift_time_series_argument(request, parameter.annotation)
                for parameter, request in zip(self._request_params, requests)
            ]
        w = _current_wiring()
        stub = self
        scalar_values = _service_scalar_values(self._signature, bound)
        if _service_needs_resolution(self):
            resolution = _resolve_service_signature(
                self._signature,
                self._resolvers,
                resolution=_copy_service_resolution(self._resolution),
                request_params=self._request_params,
                requests=requests,
                scalar_values=scalar_values,
                owner=f"service '{self.__name__}'",
            )
            _expand_pending_resolution(
                self, resolution, w, scalar_values=scalar_values)
            resolution = _registered_service_resolution(self, w, path, resolution)
            specialization = _specialization_label(resolution)
            stub = _ServiceStub(
                self.fn, self.flavour, resolution=resolution,
                specialization=specialization, resolvers=self._resolvers,
                deprecated=self._deprecated,
                pending_registrations=self._pending_registrations,
                registered_resolutions=self._registered_resolutions)
            if _service_needs_resolution(stub):
                raise TypeError(
                    f"generic service '{self.__name__}' has unresolved type variables")
        elif self._registered_resolutions:
            resolution = _registered_service_resolution(
                self, w, path, self._resolution)
            specialization = _specialization_label(resolution)
            stub = _ServiceStub(
                self.fn, self.flavour, resolution=resolution,
                specialization=specialization, resolvers=self._resolvers,
                deprecated=self._deprecated,
                pending_registrations=self._pending_registrations,
                registered_resolutions=self._registered_resolutions)
        _materialize_pending_registrations(
            self, stub._resolution, w, scalar_values=scalar_values)
        # Record the client's wiring-time scalar options against the resolved
        # path, exactly as adaptor clients do. The key carries the flavour, and
        # ``_bind_registered_impl`` reads it back through ``_client_config``.
        config_path, _ = _resolved_client_path(stub, path)
        _record_client_config(stub, config_path, bound)
        request = None
        if len(requests) == 1:
            request = _unwrap(requests[0])
        elif requests:
            fields = {
                parameter.name: _unwrap(value)
                for parameter, value in zip(stub._request_params, requests)
            }
            request = _hgraph.tsb_port(stub._request_type, fields)
        port = _hgraph.service_client(w, stub._require_descriptor(), stub._resolved_path(path),
                                      request)
        return None if port is None else WiringPort(port)

    def wire_impl_inputs_stub(self, path=""):
        """hgraph parity: the interface inputs inside a service impl."""
        return get_service_inputs(path, self)

    def wire_impl_out_stub(self, path, out):
        """hgraph parity: publish this interface's output inside an impl."""
        set_service_output(path, self, out)

    def register_impl(self, path, implementation, resolution_dict=None, **kwargs):
        """Register an implementation through this interface specialization."""
        if not isinstance(implementation, _ServiceImpl):
            raise WiringError("register_impl requires an @service_impl-decorated implementation")
        if _service_needs_resolution(self):
            _queue_service_registration(path, implementation, kwargs, (self,))
            return
        resolved = _implementation_for_stub(implementation, self)
        _register_resolved_service(path, resolved, kwargs)


def reference_service(fn=None, resolvers=None):
    """Declare a service that publishes one shared reference output.

    The interface callable's return annotation defines the shared output type;
    calling the decorated stub wires a client to the registered service.

    :param fn: Interface callable to decorate. Omit it when configuring the
        decorator.
    :param resolvers: Mapping of type variables to wiring-time resolver
        callables.
    :return: A reference-service decorator or interface stub.
    """
    if fn is None:
        return lambda f: _ServiceStub(f, "reference", resolvers=resolvers)
    return _ServiceStub(fn, "reference", resolvers=resolvers)


def subscription_service(fn=None, resolvers=None):
    """Declare a keyed subscription service.

    The first time-series parameter is the subscription key and the return
    annotation is the per-key output. Calling the decorated stub wires a
    client subscription.

    :param fn: Interface callable to decorate. Omit it when configuring the
        decorator.
    :param resolvers: Mapping of type variables to wiring-time resolver
        callables.
    :return: A subscription-service decorator or interface stub.
    """
    if fn is None:
        return lambda f: _ServiceStub(f, "subscription", resolvers=resolvers)
    return _ServiceStub(fn, "subscription", resolvers=resolvers)


def request_reply_service(fn=None, resolvers=None):
    """Declare a request/reply service.

    The time-series parameters define the request and the return annotation
    defines the response. Calling the decorated stub wires one client request.

    :param fn: Interface callable to decorate. Omit it when configuring the
        decorator.
    :param resolvers: Mapping of type variables to wiring-time resolver
        callables.
    :return: A request/reply-service decorator or interface stub.
    """
    if fn is None:
        return lambda f: _ServiceStub(f, "request_reply", resolvers=resolvers)
    return _ServiceStub(fn, "request_reply", resolvers=resolvers)


class _AdaptorClientStub:
    def _specialized_stub(self, resolution, specialization):
        return type(self)(
            self.fn, resolution=resolution, specialization=specialization,
            resolvers=self._resolvers)

    def _materialize_client_registration(self, stub, scalar_values=None):
        del stub

    def _prepare_client_request(self, args, kwargs):
        kwargs = dict(kwargs)
        has_path_parameter = "path" in self._signature.parameters
        external_path = kwargs.pop("path", "") if not has_path_parameter else None
        if has_path_parameter and "path" in kwargs and args and not isinstance(args[0], str):
            external_path = kwargs.pop("path")
            request_signature = self._signature.replace(parameters=[
                parameter for parameter in self._signature.parameters.values()
                if parameter.name != "path"
            ])
            bound = request_signature.bind(*args, **kwargs)
        else:
            bound = self._signature.bind(*args, **kwargs)
        bound.apply_defaults()
        path = bound.arguments.get("path", external_path) if has_path_parameter else external_path
        if path is None:
            path = ""
        if not isinstance(path, str):
            raise TypeError(f"{_flavour_label(self.flavour)} '{self.__name__}' path must be a string")

        from ._node import _lift_time_series_argument
        requests = [
            value if isinstance(value, WiringPort)
            else wire("nothing", output_type=parameter.annotation)
            if value is None
            else _lift_time_series_argument(value, parameter.annotation)
            for parameter in self._request_params
            for value in (bound.arguments[parameter.name],)
        ]
        stub = self
        scalar_values = _service_scalar_values(self._signature, bound)
        if _service_needs_resolution(self):
            resolution = _resolve_service_signature(
                self._signature,
                self._resolvers,
                resolution=_copy_service_resolution(self._resolution),
                request_params=self._request_params,
                requests=requests,
                scalar_values=scalar_values,
                owner=f"{_flavour_label(self.flavour)} '{self.__name__}'",
            )
            specialization = _specialization_label(resolution)
            stub = self._specialized_stub(resolution, specialization)
            if _service_needs_resolution(stub):
                raise TypeError(
                    f"generic {_flavour_label(self.flavour)} '{self.__name__}' "
                    "has unresolved type variables")
        self._materialize_client_registration(stub, scalar_values)
        config_path, path = _resolved_client_path(stub, path)
        configured_path = _record_client_config(
            stub,
            config_path,
            bound,
            variant_path=path if stub.flavour == "service_adaptor" else None,
        )
        if stub.flavour == "service_adaptor":
            path = configured_path
        request = (
            None if not requests else requests[0]
            if len(requests) == 1 else WiringPort(_hgraph.tsb_port(
                stub._request_type,
                {parameter.name: _unwrap(value)
                 for parameter, value in zip(stub._request_params, requests)})))
        return _PreparedAdaptorClient(stub, config_path, path, request)


class _AdaptorStub(_AdaptorClientStub):
    """@adaptor: an adaptor interface stub - the first TS parameter is the
    graph-side input (optional), the return annotation the graph-side output
    (optional). Calling the stub wires a CLIENT."""

    def __init__(self, fn, *, resolution=None, specialization="",
                 resolvers=None, pending_registrations=None,
                 registered_resolutions=None, resolution_variables=None):
        self.fn = fn
        self.__name__ = fn.__name__
        self.flavour = "adaptor"
        self._specialization = specialization
        self._resolution_variables = resolution_variables or {}
        self._resolution = resolution
        self._resolvers = dict(resolvers) if resolvers else None
        self._pending_registrations = (
            pending_registrations if pending_registrations is not None else [])
        self._registered_resolutions = (
            registered_resolutions if registered_resolutions is not None else [])
        sig, self._default_type_var = _wiring_signature_of(fn)
        self.__signature__ = sig
        params = [p for p in sig.parameters.values() if _is_ts_annotation(p.annotation)]
        self._signature = sig
        self._request_params = tuple(params)
        out = sig.return_annotation
        path_param = sig.parameters.get("path")
        default_path = (
            path_param.default
            if path_param is not None and isinstance(path_param.default, str)
            else f"{fn.__name__}_default"
        )
        if specialization:
            default_path = f"{default_path}[{specialization}]"
        self._default_path = default_path
        kwargs = {
            "name": fn.__name__,
            "flavour": "adaptor",
            "default_path": default_path,
            "specialization": specialization,
        }
        if params:
            request_fields = [
                (parameter.name, _resolve_annotation(parameter.annotation, resolution))
                for parameter in params
            ]
            kwargs["request"] = (
                request_fields[0][1]
                if len(request_fields) == 1
                else _hgraph.un_named_tsb_type(request_fields)
                if all(field_type is not None for _, field_type in request_fields)
                else None)
        self._request_type = kwargs.get("request")
        if _is_ts_annotation(out):
            kwargs["output"] = _resolve_annotation(out, resolution)
        unresolved = [
            name for name in ("request", "output")
            if name in kwargs and kwargs[name] is None
        ]
        self.descriptor = None if unresolved else _hgraph.service_descriptor(**kwargs)
        _remember_service_resolution(self)

    def __getitem__(self, item):
        if not _service_needs_resolution(self) and not self._specialization:
            raise TypeError(f"adaptor '{self.__name__}' is not generic")
        resolution, specialization, variables = _specialization(
            item, f"adaptor '{self.__name__}'", self._signature,
            self._resolvers)
        result = _AdaptorStub(
            self.fn, resolution=resolution, specialization=specialization,
            resolvers=self._resolvers,
            pending_registrations=self._pending_registrations,
            registered_resolutions=self._registered_resolutions,
            resolution_variables=variables)
        if _service_needs_resolution(result):
            raise TypeError(
                f"adaptor '{self.__name__}' specialization leaves an unresolved type")
        return result

    def _require_descriptor(self):
        if self.descriptor is None:
            raise TypeError(f"generic adaptor '{self.__name__}' must be specialized")
        return self.descriptor

    def _resolved_path(self, path):
        if not path or not self._specialization:
            return path
        suffix = f"[{self._specialization}]"
        return path if path.endswith(suffix) else f"{path}{suffix}"

    def is_full_path(self, path):
        return isinstance(path, str) and path.startswith("adaptor://")

    def path_from_full_path(self, path):
        if not self.is_full_path(path):
            return path
        suffix = f"/{self.__name__}"
        if not path.endswith(suffix):
            raise ValueError(f"invalid adaptor identity {path!r}")
        user_path = path[len("adaptor://"):-len(suffix)]
        specialization = f"[{self._specialization}]" if self._specialization else ""
        if specialization and user_path.endswith(specialization):
            user_path = user_path[:-len(specialization)]
        return user_path

    @property
    def implementation_arity(self):
        return len(self._request_params)

    def _specialized_stub(self, resolution, specialization):
        return _AdaptorStub(
            self.fn, resolution=resolution, specialization=specialization,
            resolvers=self._resolvers,
            pending_registrations=self._pending_registrations,
            registered_resolutions=self._registered_resolutions)

    def _materialize_client_registration(self, stub, scalar_values=None):
        _materialize_pending_registrations(
            self, stub._resolution, _current_wiring(),
            scalar_values=scalar_values)

    def __call__(self, *args, **kwargs):
        prepared = self._prepare_client_request(args, kwargs)
        port = _hgraph.adaptor_client(
            _current_wiring(), prepared.stub._require_descriptor(),
            prepared.concrete_path,
            None if prepared.request is None else _unwrap(prepared.request))
        return None if port is None else WiringPort(port)


def adaptor(fn=None, resolvers=None):
    """Declare an adaptor interface used to cross a graph boundary.

    Calling the decorated stub wires its request and response through the
    implementation registered for the selected path.

    :param fn: Interface callable to decorate. Omit it when configuring the
        decorator.
    :param resolvers: Mapping of type variables to wiring-time resolver
        callables.
    :return: An adaptor decorator or interface stub.
    """
    if fn is None:
        return lambda f: _AdaptorStub(f, resolvers=resolvers)
    return _AdaptorStub(fn, resolvers=resolvers)


class _ServiceAdaptorStub(_AdaptorClientStub):
    """A C++ service-adaptor contract with Python structural bundling.

    Multiple Python request parameters become one unnamed ``TSB`` request,
    matching a C++ interface whose ``input_schema`` is a ``TSB<...>``.
    """

    def __init__(self, fn, *, resolution=None, specialization="",
                 resolvers=None, pending_registrations=None,
                 registered_resolutions=None, resolution_variables=None):
        self.fn = fn
        self.__name__ = fn.__name__
        self.flavour = "service_adaptor"
        self._specialization = specialization
        self._resolution_variables = resolution_variables or {}
        self._resolution = resolution
        self._resolvers = dict(resolvers) if resolvers else None
        self._pending_registrations = (
            pending_registrations if pending_registrations is not None else [])
        self._registered_resolutions = (
            registered_resolutions if registered_resolutions is not None else [])
        sig, self._default_type_var = _wiring_signature_of(fn)
        self.__signature__ = sig
        params = [p for p in sig.parameters.values() if _is_ts_annotation(p.annotation)]
        if not params:
            raise TypeError(
                f"@service_adaptor '{self.__name__}' requires at least one time-series request parameter"
            )
        self._has_output = _is_ts_annotation(sig.return_annotation)
        if sig.return_annotation not in (inspect.Signature.empty, None) and not self._has_output:
            raise TypeError(
                f"@service_adaptor '{self.__name__}' return annotation must be a "
                "time-series type or None")
        self._signature = sig
        self._request_params = tuple(params)
        path_param = sig.parameters.get("path")
        default_path = (
            path_param.default
            if path_param is not None and isinstance(path_param.default, str)
            else ""
        )
        if specialization:
            default_path = f"{default_path}[{specialization}]"
        self._default_path = default_path
        request_fields = [
            (parameter.name, _resolve_annotation(parameter.annotation, resolution))
            for parameter in params
        ]
        request = (
            request_fields[0][1]
            if len(request_fields) == 1
            else _hgraph.un_named_tsb_type(request_fields)
            if all(field_type is not None for _, field_type in request_fields)
            else None
        )
        output = (
            _resolve_annotation(sig.return_annotation, resolution)
            if self._has_output else None)
        self._request_type = request
        self.descriptor = None if request is None or (self._has_output and output is None) else _hgraph.service_descriptor(
            name=fn.__name__, flavour="service_adaptor",
            request=request, output=output,
            default_path=default_path, specialization=specialization)
        _remember_service_resolution(self)

    def __getitem__(self, item):
        if not _service_needs_resolution(self) and not self._specialization:
            raise TypeError(f"service adaptor '{self.__name__}' is not generic")
        items = item if isinstance(item, tuple) else (item,)
        if not all(isinstance(binding, slice) for binding in items):
            variables = [
                variable for variable in _service_type_variables(self._signature)
                if self._resolution is None
                or _type_var_name(variable) not in self._resolution.bindings
            ]
            if len(variables) != 1 or len(items) != 1:
                raise TypeError(
                    f"service adaptor '{self.__name__}' cannot infer which type variable "
                    f"to bind; use TYPEVAR: concrete")
            item = slice(variables[0], items[0])
        resolution, specialization, variables = _specialization(
            item, f"service adaptor '{self.__name__}'", self._signature,
            self._resolvers)
        result = _ServiceAdaptorStub(
            self.fn, resolution=resolution, specialization=specialization,
            resolvers=self._resolvers,
            pending_registrations=self._pending_registrations,
            registered_resolutions=self._registered_resolutions,
            resolution_variables=variables)
        if _service_needs_resolution(result):
            raise TypeError(
                f"service adaptor '{self.__name__}' specialization leaves an unresolved type")
        return result

    def _require_descriptor(self):
        if self.descriptor is None:
            raise TypeError(
                f"generic service adaptor '{self.__name__}' must be specialized")
        return self.descriptor

    def _resolved_path(self, path):
        if not path or not self._specialization:
            return path
        suffix = f"[{self._specialization}]"
        return path if path.endswith(suffix) else f"{path}{suffix}"

    @property
    def implementation_arity(self):
        return len(self._request_params)

    def _specialized_stub(self, resolution, specialization):
        return _ServiceAdaptorStub(
            self.fn, resolution=resolution, specialization=specialization,
            resolvers=self._resolvers,
            pending_registrations=self._pending_registrations,
            registered_resolutions=self._registered_resolutions)

    def _materialize_client_registration(self, stub, scalar_values=None):
        _materialize_pending_registrations(
            self, stub._resolution, _current_wiring(),
            scalar_values=scalar_values)

    @staticmethod
    def _client_request_id(path, request):
        del path, request
        return wire("request_id", next(_SERVICE_ADAPTOR_CLIENT_TOKENS))

    def from_graph(self, *args, __request_id__=None, **kwargs):
        prepared = self._prepare_client_request(args, kwargs)
        request_id = (
            self._client_request_id(
                prepared.concrete_path, prepared.request)
            if __request_id__ is None else __request_id__)
        _remember_service_adaptor_split_path(
            prepared.stub, request_id, prepared.logical_path,
            prepared.concrete_path)
        _hgraph.service_adaptor_client_from_graph(
            _current_wiring(), prepared.stub._require_descriptor(),
            prepared.concrete_path,
            _unwrap(prepared.request), _unwrap(request_id))
        return request_id

    def to_graph(self, *, path="", __request_id__, __no_ts_inputs__=False):
        del __no_ts_inputs__  # compatibility with the old explicit client API
        stub = self
        if not stub._has_output:
            raise TypeError(
                f"sink-only service adaptor '{self.__name__}' has no to_graph output")
        if stub.descriptor is None:
            raise TypeError(
                f"generic service adaptor '{self.__name__}' must be specialized")
        logical_path, concrete_path = _resolved_client_path(stub, path)
        concrete_path = _service_adaptor_split_path(
            stub, __request_id__, logical_path, concrete_path)
        return WiringPort(_hgraph.service_adaptor_client_to_graph(
            _current_wiring(), stub._require_descriptor(), concrete_path,
            _unwrap(__request_id__)))

    def __call__(self, *args, **kwargs):
        prepared = self._prepare_client_request(args, kwargs)
        request_id = self._client_request_id(
            prepared.concrete_path, prepared.request)
        _hgraph.service_adaptor_client_from_graph(
            _current_wiring(), prepared.stub._require_descriptor(),
            prepared.concrete_path,
            _unwrap(prepared.request), _unwrap(request_id))
        if not prepared.stub._has_output:
            return None
        return WiringPort(_hgraph.service_adaptor_client_to_graph(
            _current_wiring(), prepared.stub._require_descriptor(),
            prepared.concrete_path,
            _unwrap(request_id)))


class _AdaptorImplGroup:
    """One public registration token backed by concrete generic specializations."""

    def __init__(self, *implementations):
        self.implementations = implementations

    def _register_adaptor(self, path, *, resolution_dict=None, **kwargs):
        for implementation in self.implementations:
            concrete_path = path
            if concrete_path is None:
                interface = implementation.interfaces[0]
                concrete_path = interface._default_path
            register_adaptor(
                concrete_path,
                implementation,
                resolution_dict=resolution_dict,
                **kwargs,
            )

    def wire_impl_inputs_stub(self, path=""):
        return _ServiceInputs(impl_input(self, self._resolved_path(path)))

    def wire_impl_out_stub(self, path, out):
        impl_output(self, out, self._resolved_path(path))


def service_adaptor(fn=None, resolvers=None):
    """Declare a keyed service-adaptor interface.

    A service adaptor preserves per-client request/response identity while the
    registered implementation processes clients as a group.

    :param fn: Interface callable to decorate. Omit it when configuring the
        decorator.
    :param resolvers: Mapping of type variables to wiring-time resolver
        callables.
    :return: A service-adaptor decorator or interface stub.
    """
    if fn is None:
        return lambda f: _ServiceAdaptorStub(f, resolvers=resolvers)
    return _ServiceAdaptorStub(fn, resolvers=resolvers)


def from_graph(stub, path=""):
    """Impl-side: the client input of ``stub`` (inside a registered impl).

    Works for every flavour that has a client input. For services this is the
    same operation as ``impl_input``/``get_service_inputs`` under the adaptor
    spelling (RFC 0011 step 3); a reference service has no input and raises.
    """
    descriptor = stub._require_descriptor() if hasattr(stub, "_require_descriptor") else stub.descriptor
    path = _resolved_implementation_path(stub, path)
    if stub.flavour == "service_adaptor":
        return WiringPort(_hgraph.service_adaptor_from_graph(
            _current_wiring(), descriptor, path))
    if stub.flavour == "adaptor":
        return WiringPort(_hgraph.adaptor_from_graph(_current_wiring(), descriptor, path))
    if stub.flavour == "reference":
        raise WiringError(
            f"reference service '{stub.__name__}' has no client input to read")
    return WiringPort(_hgraph.service_impl_input(_current_wiring(), descriptor, path))


def to_graph(stub, out, path=""):
    """Impl-side: publish ``stub``'s output back to clients.

    Works for every flavour that has an output. For services this is the same
    operation as ``impl_output``/``set_service_output`` under the adaptor
    spelling (RFC 0011 step 3) - the underlying wiring is identical.
    """
    descriptor = stub._require_descriptor() if hasattr(stub, "_require_descriptor") else stub.descriptor
    path = _resolved_implementation_path(stub, path)
    if stub.flavour == "service_adaptor":
        _hgraph.service_adaptor_to_graph(
            _current_wiring(), descriptor, path, out=_unwrap(out))
        return
    if stub.flavour == "adaptor":
        _hgraph.adaptor_to_graph(_current_wiring(), descriptor, path, out=_unwrap(out))
        return
    _hgraph.service_impl_output(
        _current_wiring(), descriptor, path, out=_unwrap(out))


def register_adaptor(path, implementation, resolution_dict=None, **kwargs):
    """Bind an adaptor or service-adaptor implementation to a path.

    Call this while wiring the application graph.

    :param path: Client-visible adaptor path.
    :param implementation: Callable decorated with :func:`adaptor_impl` or
        :func:`service_adaptor_impl`.
    :param resolution_dict: Concrete type-variable bindings for a generic
        interface.
    :param kwargs: Wiring-time scalar or time-series configuration passed to
        the implementation.
    """
    registration = getattr(implementation, "_register_adaptor", None)
    if registration is not None:
        registration(path, resolution_dict=resolution_dict, **kwargs)
        return
    if isinstance(implementation, _AdaptorImplGroup):
        for concrete in implementation.implementations:
            register_adaptor(
                path, concrete, resolution_dict=resolution_dict, **kwargs)
        return
    if not isinstance(implementation, _ServiceImpl):
        raise WiringError("register_adaptor requires an @adaptor_impl-decorated implementation")
    unresolved = tuple(
        stub for stub in implementation.interfaces
        if _service_needs_resolution(stub))
    if unresolved and not resolution_dict:
        _queue_service_registration(
            path, implementation, kwargs, unresolved,
            registrar=_register_resolved_adaptor)
        return
    implementation = _resolve_registered_implementation(
        implementation, resolution_dict, "register_adaptor")
    _register_resolved_adaptor(path, implementation, kwargs)


def _parameterized_service_adaptor_key(wiring, stub, path):
    return _client_state_key(wiring.identity(), stub, path)


def _materialize_parameterized_service_adaptor(
    stub, base_path, concrete_path, wiring,
):
    registration = _PARAMETERIZED_SERVICE_ADAPTOR_REGISTRATIONS.get(
        _parameterized_service_adaptor_key(wiring, stub, base_path))
    if registration is None or concrete_path in registration.paths:
        return
    registration.paths.add(concrete_path)
    try:
        _register_resolved_adaptor(
            concrete_path,
            registration.implementation,
            registration.config,
            wiring=wiring,
            _parameterize=False,
        )
    except Exception:
        registration.paths.remove(concrete_path)
        raise


def _register_parameterized_service_adaptor(
    path, implementation, config, stubs, wiring,
):
    identity = wiring.identity()
    _retain_client_state_cleanup(wiring, identity)
    stubs = tuple(stubs)
    keys = tuple(
        _parameterized_service_adaptor_key(wiring, stub, path)
        for stub in stubs)
    if any(
        key in _PARAMETERIZED_SERVICE_ADAPTOR_REGISTRATIONS
        for key in keys
    ):
        raise WiringError(
            f"duplicate service adaptor implementation at path {path!r}")
    registration = _ParameterizedServiceAdaptorRegistration(
        implementation, dict(config), set())
    for key in keys:
        _PARAMETERIZED_SERVICE_ADAPTOR_REGISTRATIONS[key] = registration

    # A multi-interface implementation is one native implementation candidate
    # at every concrete variant path. Each interface key points at this shared
    # registration, and ``registration.paths`` de-duplicates materialization
    # when several interfaces select the same scalar configuration.
    for stub, key in zip(stubs, keys):
        prefix = key[:4]
        for existing, record in tuple(_CLIENT_CONFIGS.items()):
            if existing[:4] == prefix and record.variant_path == path:
                _materialize_parameterized_service_adaptor(
                    stub, path, existing[4], wiring)


def _register_resolved_adaptor(
    path, implementation, kwargs, wiring=None, *, _parameterize=True,
):
    wiring = wiring or _current_wiring()
    default_fallback = path is None
    parameterized_stubs = tuple(
        stub for stub in implementation.interfaces
        if stub.flavour == "service_adaptor" and _has_client_config(stub)
    )
    if _parameterize and not default_fallback and parameterized_stubs:
        resolved_paths = {
            _resolved_service_path(stub, path)
            for stub in implementation.interfaces
        }
        if len(resolved_paths) != 1:
            raise WiringError(
                "multi-interface adaptors require one shared type specialization")
        resolved_path = resolved_paths.pop()
        _register_parameterized_service_adaptor(
            resolved_path,
            implementation,
            kwargs,
            parameterized_stubs,
            wiring,
        )
        # Interfaces without client scalar configuration retain their ordinary
        # base-path registration. Parameterized service-adaptor interfaces are
        # additionally materialized as clients select concrete variants.
        if len(parameterized_stubs) != len(implementation.interfaces):
            _register_resolved_adaptor(
                path, implementation, kwargs, wiring=wiring,
                _parameterize=False)
        return
    implementation_inputs, scalar_kwargs = _registration_inputs(
        implementation, kwargs)
    if not implementation.interfaces:
        impl_fn = _bind_registered_impl(implementation, path, scalar_kwargs)
        _hgraph.register_unbound_adaptor_impl(
            wiring, _wrap_graph_fn(impl_fn),
            [_unwrap(port) for port in implementation_inputs])
        return
    flavours = {stub.flavour for stub in implementation.interfaces}
    if not flavours <= {"adaptor", "service_adaptor"}:
        raise WiringError("register_adaptor requires adaptor interfaces")
    if len(implementation.interfaces) > 1:
        resolved_paths = {
            _resolved_service_path(stub, path) for stub in implementation.interfaces
        }
        if len(resolved_paths) != 1:
            raise WiringError("multi-interface adaptors require one shared type specialization")
        resolved_path = resolved_paths.pop()
        impl_fn = _bind_registered_impl(implementation, path, scalar_kwargs)
        _hgraph.register_multi_service_impl(
            wiring, [stub.descriptor for stub in implementation.interfaces], resolved_path,
            _wrap_graph_fn(impl_fn), [_unwrap(port) for port in implementation_inputs],
            default_fallback=default_fallback)
        return
    stub = implementation.interfaces[0]
    resolved_path = _resolved_service_path(stub, path)
    user_path = getattr(stub, "_default_path", "") if path is None else path
    impl_fn = _bind_registered_impl(implementation, user_path, scalar_kwargs)
    if stub.flavour == "service_adaptor":
        _hgraph.register_service_adaptor_impl(
            wiring, stub.descriptor, resolved_path, _wrap_graph_fn(impl_fn),
            default_fallback=default_fallback)
    else:
        _hgraph.register_adaptor_impl(
            wiring, stub.descriptor, resolved_path, _wrap_graph_fn(impl_fn),
            automatic=not implementation.manual_adaptor,
            inputs=[_unwrap(port) for port in implementation_inputs],
            default_fallback=default_fallback)


def _registration_inputs(implementation, config):
    """Split time-series values out of a registration's keyword configuration.

    Two shapes supply implementation inputs at registration time:

    * a MANUAL adaptor, whose every time-series parameter is supplied by the
      registration rather than by the interface; and
    * a service (or automatic adaptor) declaring time-series parameters BEYOND
      the ones its interface supplies - the surplus is registration
      configuration (RFC 0011 step 2).

    Returns ``(ports, remaining_scalar_config)``.
    """
    config = dict(config)
    if implementation.manual_adaptor:
        wanted = implementation.ts_parameters
        described = f"manual adaptor implementation '{implementation.__name__}'"
    else:
        wanted = implementation.registration_ts_parameters
        described = f"implementation '{implementation.__name__}'"
    if not wanted:
        return (), config
    inputs = []
    from ._node import _lift_time_series_argument
    for parameter in wanted:
        if parameter.name not in config:
            raise WiringError(
                f"{described} requires time-series configuration '{parameter.name}'")
        value = config.pop(parameter.name)
        if not isinstance(value, WiringPort):
            value = _lift_time_series_argument(value, parameter.annotation)
        inputs.append(value)
    return tuple(inputs), config


def adaptor_impl(fn=None, *, interfaces=None, resolvers=None,
                 deprecated=False):
    """Declare an implementation of one or more adaptor interfaces.

    The implementation takes no interface ports directly; it uses
    ``from_graph`` and ``to_graph`` to consume requests and publish responses.

    :param fn: Implementation graph to decorate. Omit it when configuring the
        decorator.
    :param interfaces: Adaptor interface stub, or iterable of supported stubs.
    :param resolvers: Mapping of type variables to wiring-time resolver
        callables.
    :param deprecated: ``True`` or a message string to emit a deprecation
        warning when the implementation is wired.
    :return: An adaptor-implementation decorator or decorated implementation.
    """
    if fn is None:
        return lambda f: _ServiceImpl(
            f, interfaces, resolvers=resolvers, deprecated=deprecated)
    return _ServiceImpl(
        fn, interfaces, resolvers=resolvers, deprecated=deprecated)


def service_adaptor_impl(fn=None, *, interfaces=None, resolvers=None,
                         label=None, deprecated=False):
    """Declare an implementation of one or more service adaptors.

    A single-interface implementation consumes and returns dictionaries keyed
    by the native client identifier.

    :param fn: Implementation graph to decorate. Omit it when configuring the
        decorator.
    :param interfaces: Service-adaptor stub, or iterable of supported stubs.
    :param resolvers: Mapping of type variables to wiring-time resolver
        callables.
    :param label: Diagnostic label used in the wired graph.
    :param deprecated: ``True`` or a message string to emit a deprecation
        warning when the implementation is wired.
    :return: A service-adaptor-implementation decorator or decorated
        implementation.
    """
    if fn is None:
        return lambda f: _ServiceImpl(
            f, interfaces, resolvers=resolvers, label=label,
            deprecated=deprecated)
    return _ServiceImpl(
        fn, interfaces, resolvers=resolvers, label=label,
        deprecated=deprecated)


_FLAVOUR_TS_ARITY = {
    "reference": 0,
    "subscription": 1,
    "request_reply": 1,
    "adaptor": 0,
    "service_adaptor": 1,
}


def _validate_interface_implementation_signature(implementation, stub):
    actual_by_name = {
        parameter.name: parameter
        for parameter in implementation.ts_parameters}
    expected_names = tuple(parameter.name for parameter in stub._request_params)
    if tuple(actual_by_name) != expected_names:
        raise TypeError(
            f"@service_impl '{implementation.__name__}': automatic adaptor inputs "
            f"must be {expected_names!r}, found {tuple(actual_by_name)!r}")

    def matches(expected, actual):
        expected_type = _resolve_annotation(expected, stub._resolution)
        actual_type = _resolve_annotation(actual, stub._resolution)
        if expected_type is not None or actual_type is not None:
            return expected_type == actual_type
        return repr(_pattern_of(expected)) == repr(_pattern_of(actual))

    for parameter in stub._request_params:
        if not matches(
                parameter.annotation,
                actual_by_name[parameter.name].annotation):
            raise TypeError(
                f"@service_impl '{implementation.__name__}': input "
                f"'{parameter.name}' does not match the adaptor interface")

    expected_output = stub._signature.return_annotation
    actual_output = implementation.signature.return_annotation
    expected_has_output = _is_ts_annotation(expected_output)
    actual_has_output = _is_ts_annotation(actual_output)
    if expected_has_output != actual_has_output or (
            expected_has_output and not matches(expected_output, actual_output)):
        raise TypeError(
            f"@service_impl '{implementation.__name__}': output does not match "
            "the adaptor interface")


class _ServiceImpl:
    """@service_impl: an implementation declaring WHICH interfaces it
    supports - validated at decoration (signature shape per flavour) and
    used at registration (hgraph parity)."""

    def __init__(self, fn, interfaces, *, resolvers=None, label=None,
                 deprecated=False):
        self.fn = fn
        self.__name__ = fn.__name__
        self.resolvers = dict(resolvers) if resolvers else None
        self.label = label
        self.deprecated = deprecated
        self._bound_impl_cache = {}
        if interfaces is None:
            raise TypeError(f"@service_impl '{self.__name__}' requires interfaces=")
        interfaces_were_sequence = isinstance(interfaces, (tuple, list))
        if not interfaces_were_sequence:
            interfaces = (interfaces,)
        self.interfaces = tuple(self._resolve(stub) for stub in interfaces)
        # Sequence spelling is the legacy opt-in for manual ADAPTOR
        # implementations only. Upstream also spells ordinary one-interface
        # service implementations as ``interfaces=(service,)``; treating that
        # request port as registration configuration drops the transport.
        self.manual_adaptor = (
            interfaces_were_sequence
            and all(stub.flavour == "adaptor" for stub in self.interfaces)
        )
        self.target = getattr(fn, "fn", fn)   # unwrap @graph/@compute_node wrappers
        self.signature = inspect.signature(self.target, eval_str=True)
        self.ts_parameters = tuple(
            p for p in self.signature.parameters.values()
            if p.name != "path"
            and (_is_ts_annotation(p.annotation) or p.annotation is inspect.Signature.empty)
        )
        ts_names = {parameter.name for parameter in self.ts_parameters}
        self.configuration_parameters = tuple(
            parameter for parameter in self.signature.parameters.values()
            if parameter.name != "path"
            and parameter.name not in ts_names
            and parameter.annotation not in _INJECTABLE_MARKERS)
        ts_params = self.ts_parameters
        self.registration_ts_parameters = ()
        if len(self.interfaces) > 1:
            # Multi-interface implementations take NO wired inputs: they
            # fetch each interface's input via impl_input and publish via
            # impl_output inside the body.
            if ts_params and not all(stub.flavour == "adaptor" for stub in self.interfaces):
                raise TypeError(
                    f"@service_impl '{self.__name__}': a multi-interface implementation takes no "
                    "time-series parameters (use impl_input/impl_output)")
        else:
            for stub in self.interfaces:
                expected = getattr(
                    stub, "implementation_arity", _FLAVOUR_TS_ARITY[stub.flavour]
                )
                if stub.flavour == "adaptor" and self.manual_adaptor:
                    continue
                if stub.flavour == "adaptor":
                    _validate_interface_implementation_signature(self, stub)
                if len(ts_params) < expected:
                    raise TypeError(
                        f"@service_impl '{self.__name__}': a {stub.flavour} implementation takes "
                        f"{expected} time-series parameter(s), found {len(ts_params)}"
                    )
                # Time-series parameters beyond the interface's own are
                # supplied at registration (RFC 0011 step 2). Adaptors express
                # the same thing through manual_adaptor.
                if not self.manual_adaptor:
                    self.registration_ts_parameters = ts_params[expected:]

    @staticmethod
    def _resolve(stub):
        if isinstance(stub, str):
            descriptor = _hgraph.find_service(stub)
            if descriptor is None:
                raise WiringError(f"no service interface named '{stub}'")

            class _CppStub:
                pass

            resolved = _CppStub()
            resolved.descriptor = descriptor
            resolved.flavour = descriptor.flavour
            return resolved
        if not isinstance(stub, (_ServiceStub, _AdaptorStub, _ServiceAdaptorStub)):
            raise TypeError(f"@service_impl interfaces must be service stubs, got {stub!r}")
        return stub


def service_impl(fn=None, *, interfaces=None, resolvers=None,
                 deprecated=False):
    """Declare an implementation of one or more service interfaces.

    Register the result with :func:`register_service`. Interfaces may be
    Python stubs or the names of C++-defined interfaces, allowing a Python
    implementation to satisfy a native public contract.

    :param fn: Implementation graph to decorate. Omit it when configuring the
        decorator.
    :param interfaces: Service interface stub/name, or iterable of supported
        stubs/names.
    :param resolvers: Mapping of type variables to wiring-time resolver
        callables.
    :param deprecated: ``True`` or a message string to emit a deprecation
        warning when the implementation is wired.
    :return: A service-implementation decorator or decorated implementation.
    """
    if fn is None:
        return lambda f: _ServiceImpl(
            f, interfaces, resolvers=resolvers, deprecated=deprecated)
    return _ServiceImpl(
        fn, interfaces, resolvers=resolvers, deprecated=deprecated)


class _ServiceInputs:
    """hgraph's get_service_inputs result: the interface inputs, exposed as
    ``.ts`` (the single input time-series)."""

    __slots__ = ("ts", "_fields")

    def __init__(self, ts, fields=None):
        self.ts = ts
        self._fields = fields or {}

    def __getattr__(self, name):
        try:
            return self._fields[name]
        except KeyError:
            raise AttributeError(name) from None


def _split_service_requests(stub, packed):
    if stub.flavour not in {"request_reply", "adaptor", "service_adaptor"} \
            or stub.implementation_arity == 1:
        return [packed]
    return [wire("getattr_", packed, parameter.name) for parameter in stub._request_params]


def _bind_registered_impl(implementation, path, config):
    """Bind path/config while leaving only native service ports in the signature."""
    impl_fn = implementation.fn
    from ._core import _published_contexts
    registration_contexts = tuple(_published_contexts)
    signature = implementation.signature
    parameters = list(signature.parameters.values())
    stub = implementation.interfaces[0] if len(implementation.interfaces) == 1 else None
    expected_ports = 0 if stub is None else getattr(
        stub, "implementation_arity", _FLAVOUR_TS_ARITY[stub.flavour]
    )
    port_parameters = list(implementation.ts_parameters)
    # Registration-supplied inputs are still ports the bound function
    # receives - they arrive appended to the flavour's transport input rather
    # than from the transport itself (RFC 0011 step 2).
    registration_count = len(implementation.registration_ts_parameters)
    manual_adaptor = bool(
        implementation.manual_adaptor
        and (stub is None or stub.flavour == "adaptor"))
    if manual_adaptor:
        expected_ports = len(port_parameters)
        native_ports = len(port_parameters)
        transport_ports = native_ports
    else:
        transport_ports = (
            0 if stub is None or stub.flavour == "reference"
            or (stub.flavour == "adaptor" and expected_ports == 0) else 1
        )
        native_ports = transport_ports + registration_count
    if len(port_parameters) != expected_ports + registration_count:
        raise WiringError(
            f"implementation '{implementation.__name__}' requires {expected_ports} native service input(s)"
        )
    port_names = {param.name for param in port_parameters}
    scalar_parameters = list(implementation.configuration_parameters)
    scalar_names = {param.name for param in scalar_parameters}
    unknown = set(config) - scalar_names
    if unknown:
        raise WiringError(
            f"implementation '{implementation.__name__}' has no scalar configuration {sorted(unknown)!r}"
        )
    from .._types import AUTO_RESOLVE

    resolved_config = dict(config)
    resolution = getattr(
        stub, "_resolution", getattr(implementation, "_resolution", None)
    )
    for param in scalar_parameters:
        if param.name in resolved_config or param.default is not AUTO_RESOLVE or resolution is None:
            continue
        import typing

        args = typing.get_args(param.annotation)
        sentinel = args[0] if args else None
        if not isinstance(sentinel, _TypeVarSentinel):
            continue
        binding = _resolution_binding(resolution, sentinel)
        if binding is not None:
            resolved_config[param.name] = _python_value_for_binding(
                sentinel, binding)

    cache_key = None
    if not registration_contexts:
        candidate = (
            implementation.interfaces,
            path,
            tuple(sorted(resolved_config.items())),
        )
        try:
            hash(candidate)
        except TypeError:
            pass
        else:
            cached = implementation._bound_impl_cache.get(candidate)
            if cached is not None:
                return cached
            cache_key = candidate

    def bound(*ports):
        if len(ports) != native_ports:
            raise WiringError(
                f"implementation '{implementation.__name__}' received {len(ports)} native service inputs"
            )
        # The transport port(s) come first, then the registration inputs.
        transport = list(ports[:len(ports) - registration_count]) if registration_count else list(ports)
        extras = list(ports[len(ports) - registration_count:]) if registration_count else []
        user_ports = (
            _split_service_requests(stub, transport[0])
            if not manual_adaptor and transport_ports and expected_ports > 1
            else transport
        ) + extras
        arguments = dict(zip((param.name for param in port_parameters), user_ports))
        effective_path = path
        matched_stub = stub
        materialized_path = _current_wiring().service_materialization_path()
        if materialized_path:
            _, _, qualified = materialized_path.partition("://")
            matched_stub = next(
                (candidate for candidate in implementation.interfaces
                 if qualified.endswith(f"/{candidate.__name__}")),
                stub,
            )
            if matched_stub is not None:
                suffix = f"/{matched_stub.__name__}"
                effective_path = qualified[:-len(suffix)]
        concrete_path = effective_path
        if not materialized_path:
            candidates = (
                (matched_stub,) if matched_stub is not None
                else implementation.interfaces
            )
            parameterized = any(
                (record := _client_config_record(candidate, effective_path))
                is not None
                and effective_path.startswith(
                    f"{record.variant_path}[__config__=")
                for candidate in candidates
            )
            if not parameterized:
                resolved_paths = {
                    _resolved_service_path(candidate, effective_path)
                    for candidate in candidates
                }
                if len(resolved_paths) == 1:
                    concrete_path = resolved_paths.pop()
        if matched_stub is not None:
            specialization = getattr(matched_stub, "_specialization", "")
            typed_suffix = f"[{specialization}]" if specialization else ""
            record = _client_config_record(matched_stub, effective_path)
            if record is None and typed_suffix \
                    and effective_path.endswith(typed_suffix):
                effective_path = effective_path[:-len(typed_suffix)]
                record = _client_config_record(matched_stub, effective_path)
            if record is None:
                client_config = {}
            else:
                client_config = record.values
                effective_path = record.logical_path
            if typed_suffix and effective_path.endswith(typed_suffix):
                effective_path = effective_path[:-len(typed_suffix)]
        else:
            # A MULTI-interface implementation has no single stub, and
            # service_materialization_path() is only exposed while
            # materializing default-fallback candidates - so an exact
            # multi-interface registration would otherwise see no client
            # configuration at all and silently substitute its own defaults.
            # Merge across every interface the implementation provides,
            # rejecting interfaces that disagree at the same path.
            client_config = {}
            sources = {}
            logical_paths = set()
            for candidate in implementation.interfaces:
                record = _client_config_record(candidate, effective_path)
                if record is None:
                    continue
                logical_paths.add(record.logical_path)
                for name, value in record.values.items():
                    if name in client_config and client_config[name] != value:
                        raise WiringError(
                            f"implementation '{implementation.__name__}' clients at path "
                            f"{effective_path!r} disagree on wiring-time option {name!r}: "
                            f"{_flavour_label(sources[name].flavour)} "
                            f"'{sources[name].__name__}' says {client_config[name]!r}, "
                            f"{_flavour_label(candidate.flavour)} "
                            f"'{candidate.__name__}' says {value!r}")
                    client_config[name] = value
                    sources[name] = candidate
            if len(logical_paths) > 1:
                raise WiringError(
                    f"implementation '{implementation.__name__}' combines clients "
                    f"from different logical paths {sorted(logical_paths)!r}")
            if logical_paths:
                effective_path = logical_paths.pop()
        if any(param.name == "path" for param in parameters):
            arguments["path"] = effective_path
        for param in scalar_parameters:
            configured = resolved_config.get(param.name, inspect.Parameter.empty)
            client_value = client_config.get(param.name, inspect.Parameter.empty)
            if configured is not inspect.Parameter.empty:
                if (client_value is not inspect.Parameter.empty
                        and client_value != configured):
                    raise WiringError(
                        f"implementation '{implementation.__name__}' option "
                        f"'{param.name}' conflicts with clients at path "
                        f"{effective_path!r}")
                arguments[param.name] = configured
            elif client_value is not inspect.Parameter.empty:
                arguments[param.name] = client_value
            elif param.default is inspect.Parameter.empty:
                raise WiringError(
                    f"implementation '{implementation.__name__}' requires scalar "
                    f"configuration '{param.name}'")
            else:
                arguments[param.name] = param.default
        from contextlib import ExitStack
        wiring = _wiring_stack[0] if _wiring_stack else _current_wiring()
        contexts = _SERVICE_BUILD_CONTEXTS.get(wiring, ())
        existing_context_count = len(_published_contexts)
        implementation_paths = {
            _service_implementation_path_key(
                candidate,
                _resolved_service_path(candidate, effective_path),
            ): concrete_path
            for candidate in implementation.interfaces
        }
        _SERVICE_IMPLEMENTATION_PATH_STACK.append(implementation_paths)
        _published_contexts.extend(registration_contexts)
        try:
            if not contexts:
                return impl_fn(**arguments)
            with ExitStack() as stack:
                for context, context_name in contexts:
                    if isinstance(context, WiringPort):
                        from types import SimpleNamespace
                        frame = SimpleNamespace(
                            f_locals={context_name: context} if context_name else {})
                        _published_contexts.append(
                            (context, _unwrap(context).ts_type, frame, wiring))
                    else:
                        stack.enter_context(context)
                return impl_fn(**arguments)
        finally:
            popped_paths = _SERVICE_IMPLEMENTATION_PATH_STACK.pop()
            assert popped_paths is implementation_paths
            del _published_contexts[existing_context_count:]

    bound.__name__ = implementation.__name__
    bound.__signature__ = inspect.Signature(
        parameters=[
            inspect.Parameter(f"requests_{index}", inspect.Parameter.POSITIONAL_OR_KEYWORD)
            for index in range(native_ports)
        ],
        return_annotation=signature.return_annotation,
    )
    if implementation.resolvers or implementation.deprecated or implementation.label:
        from ._graph import _GraphFn

        result = _GraphFn(
            bound,
            resolvers=implementation.resolvers,
            label=implementation.label,
            deprecated=implementation.deprecated,
        )
    else:
        result = bound
    if cache_key is not None:
        implementation._bound_impl_cache[cache_key] = result
    return result


def _resolve_registered_implementation(implementation, resolution_dict, operation):
    """Apply an upstream ``resolution_dict`` to unresolved interface stubs.

    The result is a shallow call-local implementation token. The decorated
    object remains reusable for other concrete specializations.
    """
    unresolved = [
        stub for stub in implementation.interfaces
        if _service_needs_resolution(stub)
    ]
    if not unresolved:
        return implementation
    if not resolution_dict:
        names = ", ".join(stub.__name__ for stub in unresolved)
        raise WiringError(
            f"{operation} requires resolution_dict for generic interface(s): {names}")
    entries = tuple(slice(key, value) for key, value in resolution_dict.items())
    import copy

    resolved = copy.copy(implementation)
    resolved.interfaces = tuple(
        stub[entries] if _service_needs_resolution(stub) else stub
        for stub in implementation.interfaces
    )
    return resolved


class _PendingServiceRegistration:
    __slots__ = (
        "wiring_identity", "path", "implementation", "config", "owners", "registrar",
        "identity")

    def __init__(self, wiring, path, implementation, config, owners, registrar):
        self.wiring_identity = wiring.identity()
        self.path = path
        self.implementation = implementation
        self.config = config
        self.owners = owners
        self.registrar = registrar
        self.identity = id(self)


def _implementation_for_stub(implementation, concrete_stub):
    """Replace the matching generic interface with a concrete specialization."""
    import copy

    resolved = copy.copy(implementation)
    replaced = False
    interfaces = []
    for interface in implementation.interfaces:
        if (isinstance(interface, _ServiceStub)
                and interface.fn is concrete_stub.fn):
            interfaces.append(concrete_stub)
            replaced = True
        else:
            interfaces.append(interface)
    if not replaced:
        raise WiringError(
            f"implementation '{implementation.__name__}' does not implement "
            f"service '{concrete_stub.__name__}'")
    resolved.interfaces = tuple(interfaces)
    return resolved


def _specialize_registered_implementation(
    implementation, resolution, scalar_values=None,
):
    import copy

    participants = (_ServiceStub, _AdaptorStub, _ServiceAdaptorStub)
    for stub in implementation.interfaces:
        if isinstance(stub, participants):
            _resolve_service_signature(
                stub._signature,
                stub._resolvers,
                resolution=resolution,
                scalar_values=scalar_values,
            )
    specialization = _specialization_label(resolution)
    resolved = copy.copy(implementation)
    resolved._resolution = resolution
    interfaces = []
    for stub in implementation.interfaces:
        if not isinstance(stub, participants):
            if getattr(stub, "descriptor", None) is None:
                raise WiringError(
                    f"generic registration is not supported for "
                    f"{stub.flavour} '{stub.__name__}'")
            interfaces.append(stub)
            continue

        needs_concrete = (
            _service_needs_resolution(stub)
            or (len(implementation.interfaces) > 1 and bool(specialization))
        )
        if not needs_concrete:
            interfaces.append(stub)
            continue
        if isinstance(stub, _AdaptorStub):
            concrete = _AdaptorStub(
                stub.fn, resolution=resolution, specialization=specialization,
                resolvers=stub._resolvers,
                pending_registrations=stub._pending_registrations,
                registered_resolutions=stub._registered_resolutions)
        elif isinstance(stub, _ServiceAdaptorStub):
            concrete = _ServiceAdaptorStub(
                stub.fn, resolution=resolution, specialization=specialization,
                resolvers=stub._resolvers,
                pending_registrations=stub._pending_registrations,
                registered_resolutions=stub._registered_resolutions)
        else:
            concrete = _ServiceStub(
                stub.fn, stub.flavour, resolution=resolution,
                specialization=specialization, resolvers=stub._resolvers,
                deprecated=stub._deprecated,
                pending_registrations=stub._pending_registrations,
                registered_resolutions=stub._registered_resolutions)
        if _service_needs_resolution(concrete):
            raise WiringError(
                f"service '{stub.__name__}' remains unresolved after request inference")
        interfaces.append(concrete)
    resolved.interfaces = tuple(interfaces)
    return resolved


def _queue_service_registration(path, implementation, config, owners=None,
                                registrar=None):
    unresolved = tuple(
        stub for stub in implementation.interfaces
        if _service_needs_resolution(stub)
    )
    triggers = tuple(owners or unresolved)
    if not triggers:
        raise WiringError("cannot defer a registration without a generic service interface")
    participants = tuple(
        stub for stub in implementation.interfaces
        if isinstance(stub, (_ServiceStub, _AdaptorStub, _ServiceAdaptorStub))
    )
    wiring = _current_wiring()
    root_wiring = _wiring_stack[0] if _wiring_stack else wiring
    wiring_identity = root_wiring.identity()
    pending = _PendingServiceRegistration(
        root_wiring, path, implementation, dict(config), participants,
        registrar or _register_resolved_service)
    for stub in participants:
        stub._registered_resolutions[:] = [
            registered
            for registered in stub._registered_resolutions
            if registered[0] == wiring_identity
        ]
    for stub in triggers:
        stub._pending_registrations.append(pending)

    def clear_registration():
        for stub in participants:
            stub._pending_registrations[:] = [
                existing for existing in stub._pending_registrations
                if existing.wiring_identity != wiring_identity
            ]
            stub._registered_resolutions[:] = [
                registered for registered in stub._registered_resolutions
                if registered[0] != wiring_identity
            ]

    root_wiring._retain_cleanup(clear_registration)


def _materialize_pending_registrations(
    owner, resolution, wiring, scalar_values=None,
):
    if resolution is None:
        return
    root_wiring = _wiring_stack[0] if _wiring_stack else wiring
    wiring_identity = root_wiring.identity()
    for pending in tuple(owner._pending_registrations):
        if pending.wiring_identity != wiring_identity:
            continue
        registered = (wiring_identity, pending.path, resolution, pending.identity)
        if all(
                any(
                    registered_identity == wiring_identity
                    and path == pending.path
                    and existing.bindings == resolution.bindings
                    and registration_identity == pending.identity
                    for registered_identity, path, existing, registration_identity
                    in stub._registered_resolutions)
                for stub in pending.owners):
            continue
        resolved = _specialize_registered_implementation(
            pending.implementation, resolution, scalar_values)
        newly_registered = []
        for stub in pending.owners:
            if not any(
                    registered_identity == wiring_identity
                    and path == pending.path
                    and existing.bindings == resolution.bindings
                    and registration_identity == pending.identity
                    for registered_identity, path, existing, registration_identity
                    in stub._registered_resolutions):
                stub._registered_resolutions.append(registered)
                newly_registered.append(stub)
        try:
            pending.registrar(
                pending.path, resolved, pending.config, wiring=root_wiring)
        except Exception:
            for stub in newly_registered:
                stub._registered_resolutions.remove(registered)
            raise


def _registered_service_resolution(owner, wiring, path, requested):
    """Recover the full specialization shared by a multi-service registration.

    An individual interface can mention only a subset of the type variables
    that determine the shared implementation path.  Match that local result
    against registrations on the same path so sibling clients address the
    implementation specialization selected by the first client.
    """
    root_wiring = _wiring_stack[0] if _wiring_stack else wiring
    wiring_identity = root_wiring.identity()
    requested_bindings = requested.bindings
    candidates = []
    for registered_identity, registered_path, resolution, _ in owner._registered_resolutions:
        if registered_identity != wiring_identity or registered_path != path:
            continue
        bindings = resolution.bindings
        if all(
                name in bindings and bindings[name] == value
                for name, value in requested_bindings.items()):
            candidates.append(resolution)
    if not candidates:
        return requested

    labels = {_specialization_label(candidate) for candidate in candidates}
    if len(labels) != 1:
        raise WiringError(
            f"service '{owner.__name__}' has multiple registered specializations "
            f"matching path '{path}' and bindings "
            f"'{_specialization_label(requested)}'")
    return candidates[0]


def _expand_pending_resolution(
    owner, resolution, wiring, scalar_values=None,
):
    root_wiring = _wiring_stack[0] if _wiring_stack else wiring
    wiring_identity = root_wiring.identity()
    for pending in tuple(owner._pending_registrations):
        if pending.wiring_identity != wiring_identity:
            continue
        for stub in pending.implementation.interfaces:
            if not isinstance(stub, _ServiceStub):
                continue
            _resolve_service_signature(
                stub._signature,
                stub._resolvers,
                resolution=resolution,
                scalar_values=scalar_values,
            )


def _registered_stub_for_path(stub, path, wiring):
    if not isinstance(stub, _ServiceStub) or not stub._registered_resolutions:
        return stub
    resolution = _registered_service_resolution(
        stub, wiring, path, stub._resolution)
    specialization = _specialization_label(resolution)
    return _ServiceStub(
        stub.fn, stub.flavour, resolution=resolution,
        specialization=specialization, resolvers=stub._resolvers,
        deprecated=stub._deprecated,
        pending_registrations=stub._pending_registrations,
        registered_resolutions=stub._registered_resolutions,
    )


def get_service_inputs(path, stub):
    """hgraph parity: the interface's inputs inside a service impl."""
    wiring = _current_wiring()
    stub = _registered_stub_for_path(stub, path, wiring)
    descriptor = stub._require_descriptor() if hasattr(stub, "_require_descriptor") else stub.descriptor
    packed = WiringPort(_hgraph.service_impl_input(
        wiring, descriptor, _resolved_implementation_path(stub, path)))
    values = _split_service_requests(stub, packed)
    fields = {
        parameter.name: value
        for parameter, value in zip(getattr(stub, "_request_params", ()), values)
    }
    return _ServiceInputs(values[0] if values else None, fields)


def set_service_output(path, stub, out):
    """hgraph parity: publish an interface's output inside a service impl."""
    wiring = _current_wiring()
    stub = _registered_stub_for_path(stub, path, wiring)
    descriptor = stub._require_descriptor() if hasattr(stub, "_require_descriptor") else stub.descriptor
    _hgraph.service_impl_output(
        wiring, descriptor, _resolved_implementation_path(stub, path),
        out=_unwrap(out))


_SERVICE_BUILD_CONTEXTS = {}


class WiringGraphContext:
    """Compatibility view of the active native wiring context.

    Service clients and registrations materialize directly in the C++ wiring
    graph. The legacy explicit ``build_services`` call therefore only marks
    the same authoring boundary for existing Python graphs.
    """

    @classmethod
    def instance(cls):
        _current_wiring()
        return cls

    @staticmethod
    def build_services():
        _current_wiring().build_services()

    @staticmethod
    def add_service_build_context(context, name=None):
        wiring = _wiring_stack[0] if _wiring_stack else _current_wiring()
        contexts = _SERVICE_BUILD_CONTEXTS.setdefault(wiring, [])
        if name is not None and any(existing_name == name for _, existing_name in contexts):
            raise WiringError(
                f"service build context with name {name!r} is already registered")
        contexts.append((context, name))

    @staticmethod
    def registered_service_clients(service):
        """Compatibility reflection over concrete native client identities.

        The native linker de-duplicates build demand by concrete base path;
        expose the old adaptor endpoint shape for catch-all resolvers. Node
        identity is represented by ``None`` because ranking is retained in the
        native client ledger rather than Python wiring-node objects.
        """
        native_kind = {
            "reference": "reference service",
            "subscription": "subscription service",
            "request_reply": "request/reply service",
            "adaptor": "adaptor",
            "service_adaptor": "service adaptor",
        }[service.flavour]
        clients = []
        for base, endpoint, kind, interface_name, specialization, receive in \
                _current_wiring().service_client_records():
            if kind != native_kind or interface_name != service.__name__:
                continue
            type_map = _SERVICE_RESOLUTIONS.get(
                (service.flavour, interface_name, specialization), {})
            clients.append((endpoint, type_map, None, receive))
        return tuple(clients)

    @staticmethod
    def built_services():
        return dict(_current_wiring().built_service_paths())


def impl_input(stub, path=""):
    """Inside a multi-interface implementation: the interface's input
    (subscription key set / request dictionary)."""
    descriptor = stub._require_descriptor() if hasattr(stub, "_require_descriptor") else stub.descriptor
    return WiringPort(_hgraph.service_impl_input(
        _current_wiring(), descriptor, _resolved_implementation_path(stub, path)))


def impl_output(stub, out, path=""):
    """Inside a multi-interface implementation: publish the interface's
    output explicitly."""
    descriptor = stub._require_descriptor() if hasattr(stub, "_require_descriptor") else stub.descriptor
    _hgraph.service_impl_output(
        _current_wiring(), descriptor, _resolved_implementation_path(stub, path),
        out=_unwrap(out))


def _register_resolved_service(path, implementation, kwargs, *, wiring=None):
    wiring = _current_wiring() if wiring is None else wiring
    default_fallback = path is None
    if not implementation.interfaces:
        # Catch-all: an implementation declaring NO interface claims every
        # otherwise-unclaimed endpoint. The underlying candidate mechanism
        # (register_catch_all_service_implementation_candidate) has no
        # flavour, so this is the same facility adaptors reach through
        # @adaptor_impl(interfaces=()) - it is simply no longer
        # adaptor-exclusive (RFC 0011 step 6).
        implementation_inputs, scalar_kwargs = _registration_inputs(implementation, kwargs)
        impl_fn = _bind_registered_impl(implementation, path or "", scalar_kwargs)
        _hgraph.register_unbound_adaptor_impl(
            wiring, _wrap_graph_fn(impl_fn),
            [_unwrap(port) for port in implementation_inputs])
        return
    if len(implementation.interfaces) > 1:
        resolved_paths = {
            _resolved_service_path(stub, path) for stub in implementation.interfaces
        }
        if len(resolved_paths) != 1:
            raise WiringError("multi-interface services require one shared type specialization")
        resolved_path = resolved_paths.pop()
        impl_fn = _bind_registered_impl(implementation, path, kwargs)
        _hgraph.register_multi_service_impl(
            wiring, [stub.descriptor for stub in implementation.interfaces], resolved_path,
            _wrap_graph_fn(impl_fn), default_fallback=default_fallback)
        return
    stub = implementation.interfaces[0]
    resolved_path = _resolved_service_path(stub, path)
    user_path = getattr(stub, "_default_path", "") if path is None else path
    # Time-series values in the registration kwargs become implementation
    # inputs; the rest stays scalar configuration (RFC 0011 step 2).
    implementation_inputs, scalar_kwargs = _registration_inputs(implementation, kwargs)
    impl_fn = _bind_registered_impl(implementation, user_path, scalar_kwargs)
    _hgraph.register_service_impl(
        wiring, stub.descriptor, resolved_path, _wrap_graph_fn(impl_fn),
        inputs=[_unwrap(port) for port in implementation_inputs],
        default_fallback=default_fallback)


def register_service(path, implementation, resolution_dict=None, **kwargs):
    """Bind a service implementation to a client-visible path.

    A single-interface implementation receives its request and publishes its
    response automatically. A multi-interface implementation uses
    ``impl_input`` and ``impl_output`` for each interface.

    :param path: Client-visible service path.
    :param implementation: Callable decorated with :func:`service_impl`.
    :param resolution_dict: Concrete type-variable bindings for a generic
        interface.
    :param kwargs: Wiring-time scalar or time-series configuration passed to
        the implementation.
    """
    if not isinstance(implementation, _ServiceImpl):
        raise WiringError("register_service requires an @service_impl-decorated implementation")
    unresolved = tuple(
        stub for stub in implementation.interfaces
        if _service_needs_resolution(stub)
    )
    if unresolved and not resolution_dict:
        _queue_service_registration(path, implementation, kwargs, unresolved)
        return
    implementation = _resolve_registered_implementation(
        implementation, resolution_dict, "register_service")
    _register_resolved_service(path, implementation, kwargs)
