"""Time-series type expressions mirroring hgraph's: TS[int], TSS[str],
TSD[str, TS[int]], TSL[TS[int], Size[3]], TSB[Schema]. Each subscription
resolves to an interned C++ type handle via the _hgraph registry."""
import datetime

import _hgraph

_SCALAR_NAMES = {
    bool: "bool",
    int: "int",
    float: "float",
    str: "str",
    bytes: "bytes",
    datetime.datetime: "datetime",
    datetime.date: "date",
    datetime.time: "time",
    datetime.timedelta: "timedelta",
    _hgraph.CivilDateTime: "civil_datetime",
    _hgraph.Period: "period",
    _hgraph.ZoneId: "zone_id",
    _hgraph.ZonedDateTime: "zoned_datetime",
    _hgraph.InstantRange: "instant_range",
    _hgraph.CivilDateRange: "civil_date_range",
    _hgraph.InstantRangeSet: "instant_range_set",
    _hgraph.CivilDateRangeSet: "civil_date_range_set",
    _hgraph.MonthEndPolicy: "month_end_policy",
    _hgraph.AmbiguousTimePolicy: "ambiguous_time_policy",
    _hgraph.NonexistentTimePolicy: "nonexistent_time_policy",
    _hgraph.Boundary: "boundary",
}

_COMPOUND_TYPE_CACHE = {}
_PYTHON_OBJECT_TYPE_CACHE = {}
_PYTHON_OBJECT_REGISTRATIONS = {}
_PYTHON_OBJECT_CLASSES = []
_TSB_SCHEMA_CLASSES = {}


def register_native_scalar_type(python_type, native_value_type):
    """Associate a Python class with a registered native scalar schema.

    ``native_value_type`` may be the schema name or the native ``ValueType``
    handle returned by the bridge. Downstream native modules normally use the
    installed C++ helper, which installs the same process-wide association.
    """
    if not isinstance(python_type, type):
        raise TypeError("python_type must be a Python class")
    if isinstance(native_value_type, str):
        native_value_type = _hgraph.value_type(native_value_type)
    if not isinstance(native_value_type, _hgraph.ValueType):
        raise TypeError("native_value_type must be a native schema name or ValueType")
    _hgraph.register_native_scalar_type(python_type, native_value_type)


def register_python_object_type(cls=None, *, fields=None):
    """Register a Python class for Python-owned structured scalar semantics.

    Standard-library dataclasses are recognised automatically. This function
    opts other application classes into the same nominal Bundle contract. An
    ordered ``fields`` mapping may be supplied when annotations do not fully
    describe the public schema. The class is returned so the function can be
    used as a decorator.
    """
    from collections.abc import Mapping

    def register(target):
        from ._compat import CompoundScalar

        if not isinstance(target, type):
            raise TypeError("register_python_object_type expects a Python class")
        if issubclass(target, CompoundScalar):
            raise TypeError(
                "CompoundScalar classes already use native Bundle storage")
        if fields is not None and not isinstance(fields, Mapping):
            raise TypeError("fields must be an ordered mapping of field names to types")
        normalized = None if fields is None else tuple(fields.items())
        if normalized is not None:
            if any(not isinstance(name, str) or not name for name, _ in normalized):
                raise TypeError("registered field names must be non-empty strings")
            if len({name for name, _ in normalized}) != len(normalized):
                raise TypeError("registered field names must be unique")
        previous = _PYTHON_OBJECT_REGISTRATIONS.get(target, ...)
        if previous is not ... and previous != normalized:
            raise TypeError(
                f"{target.__module__}.{target.__qualname__} is already registered "
                "with a different field schema")
        _PYTHON_OBJECT_REGISTRATIONS[target] = normalized
        if target not in _PYTHON_OBJECT_CLASSES:
            _PYTHON_OBJECT_CLASSES.append(target)
        return target

    return register if cls is None else register(cls)


def _is_python_object_class(cls):
    """Whether ``cls`` has Python-owned structured scalar semantics."""
    import dataclasses

    from ._compat import CompoundScalar

    time_series_schema = globals().get("TimeSeriesSchema")
    return (
        isinstance(cls, type)
        and not issubclass(cls, CompoundScalar)
        and not (
            isinstance(time_series_schema, type)
            and issubclass(cls, time_series_schema)
        )
        and (
            dataclasses.is_dataclass(cls)
            or cls in _PYTHON_OBJECT_REGISTRATIONS
        )
    )


def _register_bundle_class(
    meta, scalar, *, python_owned=False, specialization=None
):
    """Register reconstruction policy without exposing it as runtime semantics."""
    import dataclasses
    import inspect
    import types

    raw_descriptor_fields = tuple(
        name for name, _ in meta.fields
        if isinstance(
            inspect.getattr_static(scalar, name, None),
            (types.MemberDescriptorType, types.GetSetDescriptorType),
        )
    )
    try:
        defaulted_constructor_fields = {
            field.name
            for field in dataclasses.fields(scalar)
            if field.init and (
                field.default is not dataclasses.MISSING
                or field.default_factory is not dataclasses.MISSING
            )
        }
    except TypeError:
        defaulted_constructor_fields = set()

    try:
        signature = inspect.signature(scalar)
    except (TypeError, ValueError):
        signature = None
    if signature is None:
        _hgraph.register_bundle_class(
            meta,
            scalar,
            specialization,
            (),
            True,
            raw_descriptor_fields,
            tuple(defaulted_constructor_fields),
            scalar.__new__ is not object.__new__,
        )
        if python_owned:
            _hgraph.register_python_bundle_binding(meta)
        return
    schema_fields = {name for name, _ in meta.fields}
    if python_owned:
        positional_only = tuple(
            name
            for name, parameter in signature.parameters.items()
            if parameter.kind is inspect.Parameter.POSITIONAL_ONLY
            and name in schema_fields
        )
        if positional_only:
            raise TypeError(
                f"{scalar.__module__}.{scalar.__qualname__} cannot be "
                "reconstructed from Bundle fields because constructor "
                f"parameter(s) {positional_only!r} are positional-only")
        required_non_fields = tuple(
            name
            for name, parameter in signature.parameters.items()
            if parameter.default is inspect.Parameter.empty
            and parameter.kind in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                inspect.Parameter.KEYWORD_ONLY,
            )
            and name not in schema_fields
        )
        if required_non_fields:
            raise TypeError(
                f"{scalar.__module__}.{scalar.__qualname__} cannot be "
                "reconstructed from its Bundle schema because required "
                f"constructor parameter(s) {required_non_fields!r} are not fields")
    accepts_kwargs = any(
        parameter.kind is inspect.Parameter.VAR_KEYWORD
        for parameter in signature.parameters.values()
    )
    defaulted_constructor_fields.update(
        name
        for name, parameter in signature.parameters.items()
        if parameter.default is not inspect.Parameter.empty
        and parameter.kind not in (
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
        )
    )
    constructor_fields = () if accepts_kwargs else tuple(
        name for name, parameter in signature.parameters.items()
        if parameter.kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        )
    )
    if (
        python_owned
        and not dataclasses.is_dataclass(scalar)
        and not accepts_kwargs
        and not any(name in schema_fields for name in constructor_fields)
        and schema_fields
    ):
        # A default object constructor cannot consume field-wise data. Mark
        # each declared field as an attempted keyword so reconstruction fails
        # explicitly instead of returning an object with missing attributes.
        # Whole-object storage remains available.
        constructor_fields = tuple(name for name, _ in meta.fields)
    _hgraph.register_bundle_class(
        meta,
        scalar,
        specialization,
        constructor_fields,
        accepts_kwargs,
        raw_descriptor_fields,
        tuple(defaulted_constructor_fields),
        scalar.__new__ is not object.__new__,
    )
    if python_owned:
        _hgraph.register_python_bundle_binding(meta)


def _python_object_required_constructor_fields(scalar):
    """Schema fields that must be supplied to reconstruct ``scalar``."""
    import inspect

    try:
        signature = inspect.signature(scalar)
    except (TypeError, ValueError):
        return ()
    ordered_schema_fields = tuple(_python_object_python_field_types(scalar))
    schema_fields = set(ordered_schema_fields)
    required = tuple(
        name
        for name, parameter in signature.parameters.items()
        if name in schema_fields
        and parameter.default is inspect.Parameter.empty
        and parameter.kind in (
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        )
    )
    if (
        scalar in _PYTHON_OBJECT_REGISTRATIONS
        and not required
        and not any(name in schema_fields for name in signature.parameters)
    ):
        return ordered_schema_fields
    return required


def _finalize_compound_scalar_types():
    """Register every visible concrete descendant of materialized bundles.

    Structured scalar classes can be imported after a base schema was first
    used. Wiring closes each hierarchy immediately before the C++ graph
    snapshot, so the snapshot sees the complete subclass set visible to that
    wiring run.
    """
    from ._compat import CompoundScalar

    generation = _hgraph._registry_generation()
    roots = {
        scalar
        for cached_generation, scalar, arguments in tuple(_COMPOUND_TYPE_CACHE)
        if cached_generation == generation and not arguments
        and isinstance(scalar, type) and issubclass(scalar, CompoundScalar)
    }
    seen = set()

    def visit(parent):
        if parent in seen:
            return
        seen.add(parent)
        for child in parent.__subclasses__():
            if not isinstance(child, type) or not issubclass(child, CompoundScalar):
                continue
            if getattr(child, "__parameters__", ()):
                continue
            _compound_value_type(child)
            visit(child)

    for root in roots:
        visit(root)

    python_roots = {
        scalar
        for cached_generation, scalar, arguments in tuple(_PYTHON_OBJECT_TYPE_CACHE)
        if cached_generation == generation and not arguments
        and _is_python_object_class(scalar)
    }
    python_seen = set()

    def visit_python(parent):
        if parent in python_seen:
            return
        python_seen.add(parent)
        for child in parent.__subclasses__():
            if not _is_python_object_class(child):
                continue
            if getattr(child, "__parameters__", ()):
                continue
            _python_object_value_type(child)
            visit_python(child)

    for root in python_roots:
        visit_python(root)


def _substitute_typevars(tp, substitutions):
    # ``Callable[[A, B], R]`` stores its parameter list as a real list in
    # ``typing.get_args``. Preserve that structure while substituting its
    # members; a list is neither hashable nor reconstructible as a type.
    if isinstance(tp, list):
        return [_substitute_typevars(arg, substitutions) for arg in tp]
    try:
        if tp in substitutions:
            return substitutions[tp]
    except TypeError:
        # Some typing aliases, notably Callable with a concrete parameter
        # list, inherit that list's unhashability.
        pass
    import typing

    origin = typing.get_origin(tp)
    if origin is None:
        return tp
    args = tuple(_substitute_typevars(arg, substitutions) for arg in typing.get_args(tp))
    if hasattr(tp, "copy_with"):
        return tp.copy_with(args)
    try:
        return origin[args[0] if len(args) == 1 else args]
    except TypeError:
        return tp


def _compound_specialization_token(tp):
    import typing

    if isinstance(tp, _hgraph.ValueType):
        return tp.local_name or tp.name
    origin = typing.get_origin(tp)
    if origin is not None:
        args = ",".join(_compound_specialization_token(arg) for arg in typing.get_args(tp))
        return f"{origin.__name__}[{args}]"
    return getattr(tp, "__name__", repr(tp))


def _specialization_alias(scalar, type_args):
    if not type_args:
        return None
    try:
        return scalar[type_args[0] if len(type_args) == 1 else tuple(type_args)]
    except TypeError:
        return None


def _is_self_recursive_annotation(annotation, scalar, substitutions):
    """Recognise the one recursion edge the runtime can close immediately.

    Mutual recursion needs a batch declaration API because neither nominal
    schema is complete when the first class is materialised. Self recursion
    has no such ambiguity and maps directly to ``Owned<Self>``.
    """
    import types
    import typing

    annotation = _substitute_typevars(annotation, substitutions)
    if annotation is scalar or annotation is typing.Self:
        return True
    if isinstance(annotation, typing.ForwardRef):
        annotation = annotation.__forward_arg__
    if isinstance(annotation, str):
        token = annotation.strip().replace(" ", "").replace("'", "").replace('"', "")
        names = {scalar.__name__, scalar.__qualname__}
        if token in names:
            return True
        return any(
            token in {
                f"Optional[{name}]",
                f"typing.Optional[{name}]",
                f"{name}|None",
                f"None|{name}",
            }
            for name in names
        )

    origin = typing.get_origin(annotation)
    if origin in (typing.Union, types.UnionType):
        members = tuple(arg for arg in typing.get_args(annotation) if arg is not type(None))
        return len(members) == 1 and _is_self_recursive_annotation(members[0], scalar, substitutions)
    return False


def _forward_compound_target(annotation, scalar, substitutions):
    """Resolve a CompoundScalar forward reference in the defining scope."""
    import types
    import typing

    from ._compat import CompoundScalar, _COMPOUND_SCALAR_CLASSES

    annotation = _substitute_typevars(annotation, substitutions)
    if isinstance(annotation, type) and issubclass(annotation, CompoundScalar):
        return annotation
    if isinstance(annotation, typing.ForwardRef):
        annotation = annotation.__forward_arg__
    origin = typing.get_origin(annotation)
    if origin in (typing.Union, types.UnionType):
        members = tuple(
            value for value in typing.get_args(annotation)
            if value is not type(None)
        )
        return (
            _forward_compound_target(members[0], scalar, substitutions)
            if len(members) == 1 else None
        )
    if not isinstance(annotation, str):
        return None

    token = annotation.strip().replace(" ", "").replace("'", "").replace('"', "")
    for prefix in ("typing.Optional[", "Optional["):
        if token.startswith(prefix) and token.endswith("]"):
            token = token[len(prefix):-1]
    if token.endswith("|None"):
        token = token[:-5]
    elif token.startswith("None|"):
        token = token[5:]

    scope = scalar.__qualname__.rsplit(".", 1)[0]
    candidates = [
        candidate for candidate in reversed(_COMPOUND_SCALAR_CLASSES)
        if candidate.__module__ == scalar.__module__
        and token in (candidate.__name__, candidate.__qualname__)
    ]
    return next(
        (
            candidate for candidate in candidates
            if candidate.__qualname__.rsplit(".", 1)[0] == scope
        ),
        candidates[0] if candidates else None,
    )


def _mutual_recursive_component(scalar):
    import dataclasses

    graph = {}

    def visit(current):
        if current in graph:
            return
        edges = set()
        try:
            fields = dataclasses.fields(current)
        except TypeError:
            fields = ()
        for field in fields:
            target = _forward_compound_target(field.type, current, {})
            if target is not None:
                edges.add(target)
        graph[current] = edges
        for target in edges:
            visit(target)

    visit(scalar)

    def reaches(start, target, seen=None):
        if start is target:
            return True
        seen = set() if seen is None else seen
        if start in seen:
            return False
        seen.add(start)
        return any(reaches(child, target, seen) for child in graph.get(start, ()))

    return tuple(
        candidate for candidate in graph
        if candidate is not scalar and reaches(candidate, scalar)
    ) + (scalar,)


def _register_mutual_recursive_component(component):
    from ._compat import CompoundScalar
    import dataclasses

    generation = _hgraph._registry_generation()
    unique = tuple(dict.fromkeys(component))
    indices = {scalar: index for index, scalar in enumerate(unique)}
    definitions = []
    for scalar in unique:
        import typing

        try:
            resolved_annotations = typing.get_type_hints(scalar)
        except (NameError, TypeError):
            resolved_annotations = {}
        parent_metas = [
            _compound_value_type(base)
            for base in scalar.__bases__
            if (
                isinstance(base, type)
                and issubclass(base, CompoundScalar)
                and base is not CompoundScalar
                and base not in indices
            )
        ]
        inherited_fields = {}
        for parent in parent_metas:
            inherited_fields.update(parent.fields)
        fields = []
        for field_name, field_type in _compound_field_specs(
                scalar, dataclasses.fields(scalar), inherited_fields):
            if field_type is None:
                fields.append((field_name, inherited_fields[field_name]))
                continue
            field_type = resolved_annotations.get(field_name, field_type)
            target = _forward_compound_target(field_type, scalar, {})
            if target in indices:
                fields.append((field_name, indices[target]))
            else:
                fields.append((field_name, _value_type(field_type)))
        definitions.append((
            scalar.__dict__.get("__compound_namespace__", scalar.__module__),
            scalar.__name__,
            fields,
            parent_metas,
            bool(scalar.__dict__.get("__compound_abstract__", False)),
            scalar.__dict__.get("__compound_discriminator__", "__type__"),
            [],
        ))

    metas = _hgraph.recursive_bundles_vt(definitions)
    for scalar, meta in zip(unique, metas):
        _register_bundle_class(meta, scalar)
        _COMPOUND_TYPE_CACHE[(generation, scalar, ())] = meta


def _locally_declared_annotations(scalar):
    """Annotations declared on this class itself, excluding inherited ones.

    Python 3.14 (PEP 649) materialises class annotations lazily through
    ``__annotate__``, so ``cls.__dict__`` holds no ``__annotations__`` entry;
    ``annotationlib`` reads them without evaluating forward references.
    """
    try:
        import annotationlib
    except ImportError:
        return scalar.__dict__.get("__annotations__", {})
    return annotationlib.get_annotations(
        scalar, format=annotationlib.Format.FORWARDREF)


def _compound_field_specs(scalar, dataclass_fields, inherited_fields):
    """Return the logical fields of a CompoundScalar in schema order.

    ``dataclasses.fields`` intentionally omits ``InitVar`` fields. ExprClass
    uses a local InitVar to replace an inherited stored field with a computed
    descriptor, so absence from that list does not mean absence from the
    logical scalar schema. Fields explicitly marked hidden are implementation
    storage and never participate in the schema.

    A ``None`` type means that the exact inherited field type must be reused.
    """
    visible = {
        field.name: field
        for field in dataclass_fields
        if not field.metadata.get("hidden", False)
    }
    locally_declared = _locally_declared_annotations(scalar)
    specs = []
    consumed = set()

    for name in inherited_fields:
        field = visible.get(name)
        if field is None or name not in locally_declared:
            specs.append((name, None))
        else:
            specs.append((name, field.type))
        consumed.add(name)

    specs.extend(
        (field.name, field.type)
        for field in visible.values()
        if field.name not in consumed
    )
    return specs


def _compound_python_field_types(scalar):
    """The Python annotations corresponding to the logical bundle fields."""
    from ._compat import CompoundScalar
    import dataclasses

    inherited = {}
    for base in scalar.__bases__:
        if isinstance(base, type) and issubclass(base, CompoundScalar) and base is not CompoundScalar:
            inherited.update(_compound_python_field_types(base))

    try:
        dataclass_fields = dataclasses.fields(scalar)
    except TypeError:
        dataclass_fields = ()
    try:
        import typing

        resolved_annotations = typing.get_type_hints(scalar)
    except (NameError, TypeError):
        resolved_annotations = {}

    result = {}
    for name, annotation in _compound_field_specs(scalar, dataclass_fields, inherited):
        result[name] = (
            inherited[name]
            if annotation is None
            else resolved_annotations.get(name, annotation)
        )
    return result


def _python_object_python_field_types(scalar):
    """Ordered Python annotations for a Python-owned structured scalar."""
    import dataclasses
    import typing

    explicit = _PYTHON_OBJECT_REGISTRATIONS.get(scalar, ...)
    if explicit is not ... and explicit is not None:
        return dict(explicit)

    try:
        resolved_annotations = typing.get_type_hints(scalar)
    except (NameError, TypeError):
        resolved_annotations = {}

    if dataclasses.is_dataclass(scalar):
        return {
            field.name: resolved_annotations.get(field.name, field.type)
            for field in dataclasses.fields(scalar)
        }

    result = {}
    for base in reversed(scalar.__mro__):
        if base is object:
            continue
        local = _locally_declared_annotations(base)
        for name, annotation in local.items():
            result[name] = resolved_annotations.get(name, annotation)
    if not result:
        raise TypeError(
            f"{scalar.__module__}.{scalar.__qualname__} has no registered fields "
            "or annotated instance fields")
    return result


def _structured_python_field_types(scalar):
    """Ordered logical annotations for either structured storage policy."""
    from ._compat import CompoundScalar

    if isinstance(scalar, type) and issubclass(scalar, CompoundScalar):
        return _compound_python_field_types(scalar)
    if _is_python_object_class(scalar):
        return _python_object_python_field_types(scalar)
    raise TypeError(f"{scalar!r} is not a structured scalar class")


def _structured_specialized_field_types(schema):
    """Logical annotations with a parameterized class's TypeVars resolved."""
    import typing

    origin = typing.get_origin(schema) or schema
    parameters = tuple(getattr(origin, "__parameters__", ()))
    substitutions = dict(zip(parameters, typing.get_args(schema)))
    return {
        name: _substitute_typevars(annotation, substitutions)
        for name, annotation in _structured_python_field_types(origin).items()
    }


def _python_object_forward_target(annotation, scalar, substitutions):
    """Resolve a Python structured scalar reference in its defining scope."""
    import gc
    import types
    import typing

    annotation = _substitute_typevars(annotation, substitutions)
    origin = typing.get_origin(annotation)
    candidate = origin or annotation
    if _is_python_object_class(candidate):
        return candidate
    if isinstance(annotation, typing.ForwardRef):
        annotation = annotation.__forward_arg__
    if origin in (typing.Union, types.UnionType):
        members = tuple(
            value for value in typing.get_args(annotation)
            if value is not type(None)
        )
        return (
            _python_object_forward_target(members[0], scalar, substitutions)
            if len(members) == 1 else None
        )
    if not isinstance(annotation, str):
        return None

    token = annotation.strip().replace(" ", "").replace("'", "").replace('"', "")
    for prefix in ("typing.Optional[", "Optional["):
        if token.startswith(prefix) and token.endswith("]"):
            token = token[len(prefix):-1]
    if token.endswith("|None"):
        token = token[:-5]
    elif token.startswith("None|"):
        token = token[5:]

    scope = scalar.__qualname__.rsplit(".", 1)[0]
    candidates = [
        candidate for candidate in reversed(_PYTHON_OBJECT_CLASSES)
        if candidate.__module__ == scalar.__module__
        and token in (candidate.__name__, candidate.__qualname__)
    ]
    if not candidates:
        module = __import__(scalar.__module__, fromlist=("*",))
        visible = getattr(module, token, None)
        if _is_python_object_class(visible):
            candidates.append(visible)
    if not candidates:
        # Function-local dataclasses have no module-global name and no
        # __init_subclass__ hook. A bounded GC lookup is needed only while
        # resolving an otherwise-unresolved local forward reference.
        candidates = [
            candidate for candidate in gc.get_objects()
            if isinstance(candidate, type)
            and candidate.__module__ == scalar.__module__
            and token in (candidate.__name__, candidate.__qualname__)
            and _is_python_object_class(candidate)
        ]
    selected = next(
        (
            candidate for candidate in candidates
            if candidate.__qualname__.rsplit(".", 1)[0] == scope
        ),
        candidates[0] if candidates else None,
    )
    if selected is not None and selected not in _PYTHON_OBJECT_CLASSES:
        _PYTHON_OBJECT_CLASSES.append(selected)
    return selected


def _python_object_field_value_type(annotation, scalar, substitutions):
    """Resolve a Python-backed Bundle field without validating its value."""
    import collections.abc as abc
    import types
    import typing

    annotation = _substitute_typevars(annotation, substitutions)
    if (
        annotation in (typing.Callable, abc.Callable)
        or typing.get_origin(annotation) is abc.Callable
    ):
        return _hgraph.value_type("callable")
    target = _python_object_forward_target(annotation, scalar, substitutions)
    if target is not None:
        if target is scalar:
            raise TypeError(
                "self-recursive Python structured scalar containers require "
                "a recursive value recipe")
        if issubclass(scalar, target):
            return _hgraph.owned_vt(_python_object_value_type(target))
        return _value_type(annotation)

    origin = typing.get_origin(annotation)
    if origin in (typing.Union, types.UnionType):
        members = tuple(
            arg for arg in typing.get_args(annotation) if arg is not type(None)
        )
        if len(members) == 1:
            return _python_object_field_value_type(
                members[0], scalar, substitutions)
        return _value_type(annotation)
    if origin is tuple:
        args = typing.get_args(annotation)
        if len(args) == 2 and args[1] is Ellipsis:
            return _hgraph.tuple_vt(
                _python_object_field_value_type(args[0], scalar, substitutions))
        return _hgraph.fixed_tuple_vt([
            _python_object_field_value_type(arg, scalar, substitutions)
            for arg in args
        ])
    if origin in (frozenset, set):
        return _hgraph.set_vt(_python_object_field_value_type(
            typing.get_args(annotation)[0], scalar, substitutions))
    if origin is dict or getattr(origin, "__name__", "") in (
        "frozendict", "Mapping", "MutableMapping"
    ):
        key, value = typing.get_args(annotation)
        return _hgraph.map_vt(
            _python_object_field_value_type(key, scalar, substitutions),
            _python_object_field_value_type(value, scalar, substitutions),
        )
    return _value_type(annotation)


def _python_object_namespace(scalar):
    """Stable nominal namespace, including a local class's defining scope."""
    qualname = scalar.__qualname__
    if "<locals>" not in qualname:
        return scalar.__module__
    return f"{scalar.__module__}.{qualname.rsplit('.', 1)[0]}"


def _python_mutual_recursive_component(scalar):
    graph = {}

    def visit(current):
        if current in graph:
            return
        edges = {
            target
            for annotation in _python_object_python_field_types(current).values()
            if (target := _python_object_forward_target(
                annotation, current, {})) is not None
        }
        graph[current] = edges
        for target in edges:
            visit(target)

    visit(scalar)

    def reaches(start, target, seen=None):
        if start is target:
            return True
        seen = set() if seen is None else seen
        if start in seen:
            return False
        seen.add(start)
        return any(
            reaches(child, target, seen) for child in graph.get(start, ()))

    return tuple(
        candidate for candidate in graph
        if candidate is not scalar and reaches(candidate, scalar)
    ) + (scalar,)


def _register_python_mutual_recursive_component(component):
    generation = _hgraph._registry_generation()
    unique = tuple(dict.fromkeys(component))
    indices = {scalar: index for index, scalar in enumerate(unique)}
    definitions = []

    for scalar in unique:
        parents = [
            _python_object_value_type(base)
            for base in scalar.__bases__
            if _is_python_object_class(base) and base not in indices
        ]
        fields = []
        for name, annotation in _python_object_python_field_types(scalar).items():
            target = _python_object_forward_target(annotation, scalar, {})
            if target in indices:
                fields.append((name, indices[target]))
            else:
                fields.append((
                    name,
                    _python_object_field_value_type(annotation, scalar, {}),
                ))
        definitions.append((
            _python_object_namespace(scalar),
            scalar.__name__,
            fields,
            parents,
            False,
            "__type__",
            [],
        ))

    metas = _hgraph.recursive_bundles_vt(definitions)
    for scalar, meta in zip(unique, metas):
        if scalar not in _PYTHON_OBJECT_CLASSES:
            _PYTHON_OBJECT_CLASSES.append(scalar)
        _register_bundle_class(meta, scalar, python_owned=True)
        _PYTHON_OBJECT_TYPE_CACHE[(generation, scalar, ())] = meta


def _python_object_value_type(scalar, type_args=()):
    """Return the nominal Bundle schema for a Python-owned object class."""
    import typing

    cache_key = (_hgraph._registry_generation(), scalar, tuple(type_args))
    if cache_key in _PYTHON_OBJECT_TYPE_CACHE:
        return _PYTHON_OBJECT_TYPE_CACHE[cache_key]
    if not _is_python_object_class(scalar):
        raise TypeError(f"{scalar!r} is not a registered Python object type")
    if scalar not in _PYTHON_OBJECT_CLASSES:
        _PYTHON_OBJECT_CLASSES.append(scalar)

    parameters = tuple(getattr(scalar, "__parameters__", ()))
    if type_args and len(type_args) != len(parameters):
        raise TypeError(
            f"Python structured scalar {scalar.__qualname__} expects "
            f"{len(parameters)} generic arguments, got {len(type_args)}")
    substitutions = dict(zip(parameters, type_args))

    if not type_args:
        component = _python_mutual_recursive_component(scalar)
        if len(component) > 1:
            _register_python_mutual_recursive_component(component)
            return _PYTHON_OBJECT_TYPE_CACHE[cache_key]

    if type_args:
        argument_patterns = []
        generic = False
        for argument in type_args:
            try:
                argument_patterns.append(
                    _hgraph.scalar_pattern_value(_value_type(argument)))
            except _GenericType as error:
                if error.pattern is None:
                    raise TypeError(
                        f"generic Python object argument {argument!r} "
                        "has no C++ pattern") from error
                generic = True
                argument_patterns.append(error.pattern)
        if generic:
            namespace = _python_object_namespace(scalar)
            qualified_origin = (
                f"{namespace}::{scalar.__name__}"
                if namespace else scalar.__name__
            )
            raise _GenericType(
                repr(scalar),
                _hgraph.scalar_pattern_bundle_generic(
                    f"__bundle__{qualified_origin}",
                    qualified_origin,
                    argument_patterns,
                ),
            )

    parent_metas = []
    inherited_annotations = {}
    consumed = set()
    for original_base in scalar.__dict__.get("__orig_bases__", ()):
        base_origin = typing.get_origin(original_base)
        if not _is_python_object_class(base_origin):
            continue
        base_args = tuple(
            _substitute_typevars(arg, substitutions)
            for arg in typing.get_args(original_base)
        )
        parent_metas.append(_python_object_value_type(base_origin, base_args))
        base_substitutions = dict(
            zip(getattr(base_origin, "__parameters__", ()), base_args))
        inherited_annotations.update({
            name: _substitute_typevars(annotation, base_substitutions)
            for name, annotation
            in _python_object_python_field_types(base_origin).items()
        })
        consumed.add(base_origin)
    for base in scalar.__bases__:
        if _is_python_object_class(base) and base not in consumed:
            parent_metas.append(_python_object_value_type(base))
            inherited_annotations.update(
                _python_object_python_field_types(base))

    inherited_fields = {}
    for parent in parent_metas:
        for field_name, field_type in parent.fields:
            previous = inherited_fields.setdefault(field_name, field_type)
            if previous != field_type:
                raise TypeError(
                    f"Python structured scalar {scalar.__qualname__} inherits "
                    f"incompatible field {field_name!r}")

    annotations = _python_object_python_field_types(scalar)
    local_annotations = _locally_declared_annotations(scalar)
    fields = []
    has_self_recursion = False
    for field_name, field_type in annotations.items():
        field_type = _substitute_typevars(field_type, substitutions)
        if field_name in inherited_fields and field_name not in local_annotations:
            fields.append((field_name, inherited_fields[field_name]))
            continue
        if _is_self_recursive_annotation(field_type, scalar, substitutions):
            fields.append((field_name, None))
            has_self_recursion = True
        else:
            fields.append((
                field_name,
                _python_object_field_value_type(
                    field_type, scalar, substitutions),
            ))

    local_name = scalar.__name__
    if type_args:
        local_name += "[" + ",".join(
            _compound_specialization_token(arg) for arg in type_args) + "]"
    generic_arguments = [_value_type(argument) for argument in type_args]
    register = (
        _hgraph.recursive_bundle_vt
        if has_self_recursion else _hgraph.qualified_bundle_vt
    )
    meta = register(
        _python_object_namespace(scalar),
        local_name,
        fields,
        parent_metas,
        False,
        "__type__",
        generic_arguments,
    )
    _register_bundle_class(
        meta,
        scalar,
        python_owned=True,
        specialization=_specialization_alias(scalar, type_args),
    )
    _PYTHON_OBJECT_TYPE_CACHE[cache_key] = meta

    for child in scalar.__subclasses__():
        if not _is_python_object_class(child):
            continue
        if getattr(child, "__parameters__", ()):
            continue
        if type_args:
            matches_specialization = any(
                typing.get_origin(base) is scalar
                and tuple(typing.get_args(base)) == tuple(type_args)
                for base in child.__dict__.get("__orig_bases__", ())
            )
            if not matches_specialization:
                continue
        _python_object_value_type(child)
    return meta


def _compound_field_value_type(annotation, scalar, substitutions):
    """Resolve a field type, inserting Owned at recursive ancestry edges.

    A closed base bundle includes every concrete leaf. A leaf which embeds
    that base, even through a tuple/set/map, therefore forms a storage-plan
    cycle and must contribute one owner pointer at the point of recursion.
    """
    import collections.abc as abc
    import types
    import typing

    annotation = _substitute_typevars(annotation, substitutions)
    if (annotation in (typing.Callable, abc.Callable)
            or typing.get_origin(annotation) is abc.Callable):
        # A Callable used as a graph scalar denotes a wiring-time WiredFn.
        # Inside a CompoundScalar it is data and must therefore use the
        # evaluation-time ValueCallable representation, as TS[Callable] does.
        return _hgraph.value_type("callable")
    target = _forward_compound_target(annotation, scalar, substitutions)
    if target is not None:
        if target is scalar:
            raise TypeError(
                "self-recursive CompoundScalar containers require a recursive value recipe")
        if issubclass(scalar, target):
            return _hgraph.owned_vt(_compound_value_type(target))
        return _value_type(annotation)

    origin = typing.get_origin(annotation)
    if origin in (typing.Union, types.UnionType):
        members = tuple(arg for arg in typing.get_args(annotation) if arg is not type(None))
        if len(members) == 1:
            return _compound_field_value_type(members[0], scalar, substitutions)
        return _value_type(annotation)
    if origin is tuple:
        args = typing.get_args(annotation)
        if len(args) == 2 and args[1] is Ellipsis:
            return _hgraph.tuple_vt(
                _compound_field_value_type(args[0], scalar, substitutions))
        return _hgraph.fixed_tuple_vt([
            _compound_field_value_type(arg, scalar, substitutions)
            for arg in args
        ])
    if origin in (frozenset, set):
        return _hgraph.set_vt(
            _compound_field_value_type(typing.get_args(annotation)[0], scalar, substitutions))
    if origin is dict or getattr(origin, "__name__", "") in (
            "frozendict", "Mapping", "MutableMapping"):
        key, value = typing.get_args(annotation)
        return _hgraph.map_vt(
            _compound_field_value_type(key, scalar, substitutions),
            _compound_field_value_type(value, scalar, substitutions),
        )
    return _value_type(annotation)


def _is_covariant_compound_field(annotation, inherited_annotation):
    """Whether a Python field annotation safely narrows its inherited bundle type.

    The narrower annotation remains visible to Python reflection, while native
    storage retains the inherited schema so the base prefix stays invariant.
    """
    from ._compat import CompoundScalar
    import typing

    annotation = typing.get_origin(annotation) or annotation
    inherited_annotation = typing.get_origin(inherited_annotation) or inherited_annotation
    return (
        isinstance(annotation, type)
        and isinstance(inherited_annotation, type)
        and issubclass(annotation, CompoundScalar)
        and issubclass(inherited_annotation, CompoundScalar)
        and annotation is not inherited_annotation
        and issubclass(annotation, inherited_annotation)
    )


def _compound_value_type(scalar, type_args=()):
    from ._compat import CompoundScalar
    import dataclasses

    cache_key = (_hgraph._registry_generation(), scalar, tuple(type_args))
    if cache_key in _COMPOUND_TYPE_CACHE:
        return _COMPOUND_TYPE_CACHE[cache_key]

    parameters = tuple(getattr(scalar, "__parameters__", ()))
    if type_args and len(type_args) != len(parameters):
        raise TypeError(
            f"CompoundScalar {scalar.__qualname__} expects {len(parameters)} generic arguments, "
            f"got {len(type_args)}"
        )
    substitutions = dict(zip(parameters, type_args))

    if not type_args:
        component = _mutual_recursive_component(scalar)
        if len(component) > 1:
            _register_mutual_recursive_component(component)
            return _COMPOUND_TYPE_CACHE[cache_key]

    if type_args:
        argument_patterns = []
        generic = False
        for argument in type_args:
            try:
                argument_patterns.append(
                    _hgraph.scalar_pattern_value(_value_type(argument))
                )
            except _GenericType as error:
                if error.pattern is None:
                    raise TypeError(
                        f"generic CompoundScalar argument {argument!r} has no C++ pattern"
                    ) from error
                generic = True
                argument_patterns.append(error.pattern)
        if generic:
            bundle_namespace = scalar.__dict__.get(
                "__compound_namespace__", scalar.__module__
            )
            qualified_origin = (
                f"{bundle_namespace}::{scalar.__name__}"
                if bundle_namespace else scalar.__name__
            )
            schema_variable = f"__bundle__{qualified_origin}"
            raise _GenericType(
                repr(scalar),
                _hgraph.scalar_pattern_bundle_generic(
                    schema_variable, qualified_origin, argument_patterns
                ),
            )

    parent_metas = []
    inherited_annotations = {}
    original_bases = tuple(scalar.__dict__.get("__orig_bases__", ()))
    consumed = set()
    import typing

    for original_base in original_bases:
        base_origin = typing.get_origin(original_base)
        if not isinstance(base_origin, type) or not issubclass(base_origin, CompoundScalar):
            continue
        base_args = tuple(
            _substitute_typevars(arg, substitutions) for arg in typing.get_args(original_base)
        )
        parent_metas.append(_compound_value_type(base_origin, base_args))
        base_substitutions = dict(zip(getattr(base_origin, "__parameters__", ()), base_args))
        inherited_annotations.update({
            name: _substitute_typevars(annotation, base_substitutions)
            for name, annotation in _compound_python_field_types(base_origin).items()
        })
        consumed.add(base_origin)
    for base in scalar.__bases__:
        if (
            isinstance(base, type)
            and issubclass(base, CompoundScalar)
            and base is not CompoundScalar
            and base not in consumed
        ):
            parent_metas.append(_compound_value_type(base))
            inherited_annotations.update(_compound_python_field_types(base))

    inherited_fields = {}
    for parent in parent_metas:
        for field_name, field_type in parent.fields:
            previous = inherited_fields.setdefault(field_name, field_type)
            if previous != field_type:
                raise TypeError(
                    f"CompoundScalar {scalar.__qualname__} inherits incompatible field {field_name!r}"
                )

    try:
        dataclass_fields = dataclasses.fields(scalar)
    except TypeError:
        annotations = {
            name: annotation
            for base in reversed(scalar.__mro__)
            if issubclass(base, CompoundScalar)
            for name, annotation in getattr(base, "__annotations__", {}).items()
        }
        if annotations:
            # A field-bearing CompoundScalar declared without @dataclass is
            # materialised lazily here: apply the frozen-dataclass form (matching
            # upstream's CompoundScalar.__init_subclass__ auto-dataclass and the
            # un-named-compound helper's frozen convention), then read its fields.
            # Classes the user already decorated succeed the fields() call above
            # and never reach this branch.
            dataclass_fields = dataclasses.fields(dataclasses.dataclass(frozen=True)(scalar))
        else:
            dataclass_fields = ()
    fields = []
    has_self_recursion = False
    try:
        resolved_annotations = typing.get_type_hints(scalar)
    except (NameError, TypeError):
        resolved_annotations = {}
    for field_name, field_type in _compound_field_specs(
            scalar, dataclass_fields, inherited_fields):
        if field_type is None:
            fields.append((field_name, inherited_fields[field_name]))
            continue
        else:
            field_type = resolved_annotations.get(field_name, field_type)
        if (
            field_name in inherited_fields
            and field_name in inherited_annotations
            and _is_covariant_compound_field(field_type, inherited_annotations[field_name])
        ):
            fields.append((field_name, inherited_fields[field_name]))
            continue
        if _is_self_recursive_annotation(field_type, scalar, substitutions):
            fields.append((field_name, None))
            has_self_recursion = True
        else:
            fields.append((field_name, _compound_field_value_type(
                field_type, scalar, substitutions)))

    if getattr(scalar, "__unnamed_compound__", False):
        if has_self_recursion:
            raise TypeError("self-recursive CompoundScalar values require a nominal namespace and name")
        return _hgraph.un_named_bundle_vt(fields)

    bundle_namespace = scalar.__dict__.get("__compound_namespace__", scalar.__module__)
    local_name = scalar.__name__
    if type_args:
        local_name += "[" + ",".join(_compound_specialization_token(arg) for arg in type_args) + "]"
    is_abstract = bool(scalar.__dict__.get("__compound_abstract__", False))
    discriminator = scalar.__dict__.get("__compound_discriminator__", "__type__")
    generic_arguments = [_value_type(argument) for argument in type_args]
    register = _hgraph.recursive_bundle_vt if has_self_recursion else _hgraph.qualified_bundle_vt
    meta = register(
        bundle_namespace, local_name, fields, parent_metas, is_abstract, discriminator, generic_arguments
    )
    # Python erases generic arguments on ``instance.__class__``. The runtime
    # schema still carries invariant arguments, while class matching must use
    # the concrete origin class.
    _register_bundle_class(
        meta,
        scalar,
        specialization=_specialization_alias(scalar, type_args),
    )
    _COMPOUND_TYPE_CACHE[cache_key] = meta

    # Close the Python-visible subclass set while wiring is still allowed to
    # add schemas. Generic children are included only when their concrete
    # parent specialization matches this one; open generic children remain a
    # wiring-time error until a specialization is actually used.
    for child in scalar.__subclasses__():
        if not isinstance(child, type) or not issubclass(child, CompoundScalar):
            continue
        if getattr(child, "__parameters__", ()):
            continue
        if type_args:
            matches_specialization = any(
                typing.get_origin(base) is scalar and
                tuple(typing.get_args(base)) == tuple(type_args)
                for base in child.__dict__.get("__orig_bases__", ())
            )
            if not matches_specialization:
                continue
        _compound_value_type(child)
    return meta


def _value_type(scalar):
    if isinstance(scalar, _hgraph.ValueType):
        return scalar
    if isinstance(scalar, type):
        native_value_type = _hgraph.native_scalar_value_type(scalar)
        if native_value_type is not None:
            return native_value_type
    if isinstance(scalar, _ArrayType):
        dimensions = tuple(0 if dimension == -1 else dimension
                           for dimension in scalar.dimensions) or (0,)
        try:
            element = _value_type(scalar.element)
            if any(isinstance(dimension, (_TypeVarSentinel, _typing.TypeVar))
                   for dimension in dimensions):
                raise _GenericType()
            return _hgraph.array_vt(element, list(dimensions))
        except _GenericType as error:
            element_pattern = (error.pattern if error.pattern is not None
                               else _scalar_pattern(scalar.element))
            dimension_patterns = [_size_pattern(dimension) for dimension in dimensions]
            raise _GenericType(
                repr(scalar),
                _hgraph.scalar_pattern_array(element_pattern, dimension_patterns),
            ) from error
    if isinstance(scalar, _SeriesType):
        try:
            return _hgraph.series_vt(_value_type(scalar.element))
        except _GenericType as error:
            raise _GenericType(
                repr(scalar), _hgraph.scalar_pattern_series(error.pattern)) from error
    if scalar is Series:
        return _hgraph.value_type("series")   # element-untyped base
    if isinstance(scalar, _FrameType):
        try:
            row = _value_type(scalar.schema)
            return (_hgraph.frame_vt(row) if scalar.metadata is None
                    else _hgraph.frame_vt(row, _value_type(scalar.metadata)))
        except _GenericType as error:
            raise _GenericType(
                repr(scalar), _hgraph.scalar_pattern_frame(error.pattern)) from error
    if scalar is Frame:
        return _hgraph.value_type("frame")    # schema-untyped base
    if isinstance(scalar, str):
        return _hgraph.value_type(scalar)
    if isinstance(scalar, _TypeVarSentinel):
        constraints = [
            _value_type(constraint)
            for constraint in getattr(scalar, "__constraints__", ())
        ]
        raise _GenericType(
            pattern=_hgraph.scalar_pattern_var(_type_var_name(scalar), constraints)
            if constraints else _hgraph.scalar_pattern_var(_type_var_name(scalar)))
    # typing generics: tuple[X, ...] / tuple[A, B] / frozenset[X] / dict[K, V]
    import collections.abc as _abc
    import enum as _enum
    import typing

    origin = typing.get_origin(scalar)
    if scalar in (typing.Callable, _abc.Callable) or origin is _abc.Callable:
        return _hgraph.value_type("fn")
    if origin is not None:
        args = typing.get_args(scalar)
        import types

        if origin in (typing.Union, types.UnionType):
            members = tuple(arg for arg in args if arg is not type(None))
            if len(members) == 1 and len(members) != len(args):
                # None is represented by an unset Bundle field. The stored
                # value therefore uses exactly the non-None schema and has no
                # extra leaf storage overhead.
                return _value_type(members[0])
            raise TypeError(f"unsupported scalar union type for hgraph: {scalar!r}")
        from ._compat import CompoundScalar
        if isinstance(origin, type) and issubclass(origin, CompoundScalar):
            return _compound_value_type(origin, args)
        if _is_python_object_class(origin):
            return _python_object_value_type(origin, args)
        is_mapping_origin = (
            origin is dict
            or getattr(origin, "__name__", "") == "frozendict"
            or getattr(origin, "__name__", "") in ("Mapping", "MutableMapping")
        )
        if not args:
            if origin is tuple:
                raise _GenericType(repr(scalar), _hgraph.scalar_pattern_unknown_tuple())
            if origin in (frozenset, set):
                raise _GenericType(
                    repr(scalar),
                    _hgraph.scalar_pattern_set(_hgraph.scalar_pattern_var("T")),
                )
            if is_mapping_origin:
                raise _GenericType(
                    repr(scalar),
                    _hgraph.scalar_pattern_map(
                        _hgraph.scalar_pattern_var("K"),
                        _hgraph.scalar_pattern_var("V"),
                    ),
                )
            raise _GenericType(repr(scalar))
        if origin is tuple:
            if len(args) == 2 and args[1] is Ellipsis:
                try:
                    return _hgraph.tuple_vt(_value_type(args[0]))
                except _GenericType as e:
                    raise _GenericType(
                        repr(scalar),
                        _hgraph.scalar_pattern_homogeneous_tuple(e.pattern),
                    ) from e
            values = []
            patterns = []
            generic = False
            for arg in args:
                try:
                    value = _value_type(arg)
                    values.append(value)
                    patterns.append(_hgraph.scalar_pattern_value(value))
                except _GenericType as e:
                    generic = True
                    patterns.append(e.pattern)
            if generic:
                raise _GenericType(
                    repr(scalar),
                    _hgraph.scalar_pattern_fixed_tuple(patterns),
                )
            return _hgraph.fixed_tuple_vt(values)
        if origin in (frozenset, set):
            try:
                return _hgraph.set_vt(_value_type(args[0]))
            except _GenericType as e:
                raise _GenericType(
                    repr(scalar),
                    _hgraph.scalar_pattern_set(e.pattern),
                ) from e
        if is_mapping_origin:
            try:
                return _hgraph.map_vt(_value_type(args[0]), _value_type(args[1]))
            except _GenericType:
                raise _GenericType(
                    repr(scalar),
                    _hgraph.scalar_pattern_map(_scalar_pattern(args[0]), _scalar_pattern(args[1])),
                )
        if isinstance(origin, type):
            # Parameterized application classes (for example Expression[A,
            # B] and type[A]) are opaque Python values. Their type arguments
            # remain Python-side validation/documentation and do not invent a
            # second native schema family.
            return _hgraph.value_type("object")
        raise TypeError(f"unsupported generic scalar type for hgraph: {scalar!r}")
    if scalar is typing.Tuple:
        raise _GenericType(repr(scalar), _hgraph.scalar_pattern_unknown_tuple())
    if scalar in (typing.Set, typing.FrozenSet):
        raise _GenericType(repr(scalar), _hgraph.scalar_pattern_set(_hgraph.scalar_pattern_var("T")))
    if scalar is typing.Mapping:
        raise _GenericType(
            repr(scalar),
            _hgraph.scalar_pattern_map(_hgraph.scalar_pattern_var("K"), _hgraph.scalar_pattern_var("V")),
        )
    from ._compat import JSON as _JSON

    if scalar is _JSON:
        return _hgraph.value_type("JSON")
    name = _SCALAR_NAMES.get(scalar)
    if name is None and scalar is tuple:
        raise _GenericType(scalar.__name__, _hgraph.scalar_pattern_unknown_tuple())
    if name is None and scalar in (frozenset, set):
        raise _GenericType(scalar.__name__, _hgraph.scalar_pattern_set(_hgraph.scalar_pattern_var("T")))
    if name is None and scalar is dict:
        raise _GenericType(
            scalar.__name__,
            _hgraph.scalar_pattern_map(_hgraph.scalar_pattern_var("K"), _hgraph.scalar_pattern_var("V")),
        )
    if name is None and isinstance(scalar, type):
        from ._compat import CompoundScalar

        if scalar is CompoundScalar:
            raise _GenericType("CompoundScalar", _hgraph.scalar_pattern_bundle())
        if issubclass(scalar, CompoundScalar) and scalar is not CompoundScalar:
            # C++-first ruling (2026-07-06): a CompoundScalar IS a C++
            # Bundle value - the schema maps to a named bundle schema.
            return _compound_value_type(scalar)
        if _is_python_object_class(scalar):
            return _python_object_value_type(scalar)
    if name is None and isinstance(scalar, type) and issubclass(scalar, _enum.Enum):
        # A python Enum is a FIRST-CLASS enum scalar (nominal identity by
        # class name; the member table interns with the meta and the class
        # registers for read-back). CmpResult/DivideByZero pre-registered by
        # the bridge resolve to their existing metas by the same name path.
        try:
            return _hgraph.value_type(scalar.__name__)
        except Exception:
            pass
        enum_name = scalar.__name__
        qualname = getattr(scalar, "__qualname__", enum_name)
        if "<locals>" in qualname:
            enum_name = f"{scalar.__module__}.{qualname}"
        enum_members = list(scalar)
        use_public_values = all(isinstance(member.value, int) for member in enum_members)
        members = [
            (member.name, member.value if use_public_values else index)
            for index, member in enumerate(enum_members)
        ]
        return _hgraph.enum_vt(enum_name, members, scalar)
    if name is None and isinstance(scalar, type):
        # Any python class is a first-class scalar (hgraph parity): it maps
        # onto the "object" value kind; type checking stays python-side.
        name = "object"
    if name is None:
        raise TypeError(f"unsupported scalar type for hgraph: {scalar!r}")
    return _hgraph.value_type(name)


class _TsExpr:
    def from_ts(self, *ports, **kwargs):
        """hgraph parity: build a structural port for this type from
        per-element ports. Plain values lift to const at the field type."""
        import _hgraph as _m

        from ._wiring import WiringPort, _unwrap, wire

        if kwargs and not ports and getattr(self.handle, "is_ts", False) and not getattr(self, "_json", False):
            import datetime as _dt

            from ._wiring import wire as _wire

            _COMPONENTS = {
                _m.ts(_value_type(_dt.date)): ("year", "month", "day"),
                _m.ts(_value_type(_dt.timedelta)): (
                    "weeks", "days", "hours", "minutes", "seconds", "milliseconds", "microseconds"),
                _m.ts(_value_type(_dt.datetime)): ("date", "time"),
            }
            components = _COMPONENTS.get(self.handle)
            if components is not None and all(k in components or k == "__strict__" for k in kwargs):
                # combine[TS[date/timedelta/datetime]](components...): plain
                # values const-lift; absent numeric components fill with
                # const 0 (hgraph parity).
                from ._wiring import WiringPort as _WP
                from ._wiring import _infer_ts_type

                call = dict(kwargs)
                strict = call.pop("__strict__", True)
                for name, value in list(call.items()):
                    if value is None:
                        call.pop(name)   # unsupplied positional padding
                        continue
                    if not isinstance(value, _WP):
                        tp = _infer_ts_type([value]) if not isinstance(value, int) else TS[int]
                        call[name] = _wire("const", value, output_type=tp)
                if components[0] == "weeks":
                    for name in components:
                        if name not in call:
                            call[name] = _wire("const", 0, output_type=TS[int])
                    if strict is False:
                        call["__strict__"] = False
                return _wire("combine", output_type=self, **call)

        strict_cs = kwargs.pop("__strict__", True)
        if (kwargs and not ports and self.handle.is_ts_bundle and not getattr(self, "_json", False)):
            # combine[TS[CompoundScalar]](field=...): a structural TSB of the
            # provided fields feeds the erased combine_cs node (CS IS a
            # Bundle value; missing fields stay UNSET). Plain values
            # const-lift at their inferred types.
            call = dict(kwargs)
            cs_class = getattr(self, "_cs_class", None)
            field_types = {}
            if cs_class is not None:
                field_types.update(_structured_specialized_field_types(
                    getattr(self, "_structured_schema", cs_class)))
                # hgraph parity: UNSUPPLIED dataclass fields take their
                # defaults (supplied-but-invalid stays None in non-strict).
                import dataclasses

                try:
                    dataclass_fields = dataclasses.fields(cs_class)
                except TypeError:
                    dataclass_fields = ()
                for field in dataclass_fields:
                    if (field.name not in call and field.default is not dataclasses.MISSING
                            and field.default is not None):
                        call[field.name] = field.default
                if _is_python_object_class(cs_class):
                    unknown = tuple(name for name in call if name not in field_types)
                    if unknown:
                        raise TypeError(
                            f"combine[{self!r}] received unknown field(s) "
                            f"{unknown!r}")
                    missing = tuple(
                        name
                        for name in _python_object_required_constructor_fields(
                            cs_class)
                        if name not in call
                    )
                    if missing:
                        raise TypeError(
                            f"combine[{self!r}] requires constructor field(s) "
                            f"{missing!r}")
            lifted = {}
            for name, value in call.items():
                unwrapped = _unwrap(value)
                if not isinstance(unwrapped, _m.Port):
                    from ._wiring import _infer_ts_type

                    declared = field_types.get(name)
                    tp = TS[declared] if declared is not None else _infer_ts_type([value])
                    if tp is None:
                        raise TypeError(f"combine_cs: cannot infer a type for '{name}'")
                    value = wire("const", value, output_type=tp)
                lifted[name] = value
            fields = [(k, _unwrap(v).ts_type) for k, v in lifted.items()]
            tsb_type = _m.un_named_tsb_type(fields)
            structural = WiringPort(_m.tsb_port(tsb_type, {k: _unwrap(v) for k, v in lifted.items()}))
            if strict_cs is False:
                return wire("combine_cs", structural, __strict__=False, output_type=self)
            return wire("combine_cs", structural, output_type=self)

        if getattr(self, "_json", False):
            # combine[TS[JSON]](**kwargs): the erased combine_json operator
            # (scalar kwargs const-lift at their inferred types).
            lifted = {}
            for name, value in kwargs.items():
                unwrapped = _unwrap(value)
                if not isinstance(unwrapped, _m.Port):
                    from ._wiring import _infer_ts_type

                    tp = _infer_ts_type([value])
                    if isinstance(value, list):
                        tp = _infer_ts_type([tuple(value)])
                        value = tuple(value)
                    if tp is None:
                        raise TypeError(f"combine_json: cannot infer a type for '{name}'")
                    value = wire("const", value, output_type=tp)
                lifted[name] = value
            return wire("combine_json", **lifted)
        if kwargs and not ports and getattr(self.handle, "is_ts", False):
            # The GENERIC kwargs form for any remaining TS-valued target
            # (e.g. combine[TS[Frame[X]]](a=..., b=...)): pack a structural
            # un-named TSB and let the C++ registry's overload matching pick
            # the kernel from the target - no py-side kind tests.
            lifted = {}
            for name, value in kwargs.items():
                unwrapped = _unwrap(value)
                if not isinstance(unwrapped, _m.Port):
                    from ._wiring import _infer_ts_type

                    tp = _infer_ts_type([value])
                    if tp is None:
                        raise TypeError(f"combine: cannot infer a type for '{name}'")
                    value = wire("const", value, output_type=tp)
                lifted[name] = value
            fields = [(k, _unwrap(v).ts_type) for k, v in lifted.items()]
            tsb_type = _m.un_named_tsb_type(fields)
            structural = WiringPort(
                _m.tsb_port(tsb_type, {k: _unwrap(v) for k, v in lifted.items()}))
            if strict_cs is False:
                return wire("combine", structural, __strict__=False, output_type=self)
            return wire("combine", structural, output_type=self)
        if ports and not kwargs:
            # POSITIONAL forms dispatch on the RESOLVED target handle (the
            # C++ type properties; generic targets were completed by
            # resolve_combine_target before reaching here).
            if getattr(self, "_bare_map", False):   # slot may be unset
                # combine[TS[frozendict...]](keys, values) -> combine_map;
                # the BARE form resolves key/value types from the inputs.
                return wire("combine_map", *ports)
            if self.handle.is_ts_mapping:
                return wire("combine_map", *ports, output_type=self)
            if self.handle.is_tss:
                # combine[TSS](a, b, ...): the desired-membership union.
                return wire("combine", *ports, output_type=self)
            if self.handle.is_tsd:
                if all(_unwrap(p).ts_type.is_tsl for p in ports):
                    # combine[TSD](tsl_keys, tsl_values): ticking key set -
                    # the combine_tsd kernel binds its own REF-valued output.
                    return wire("combine_tsd", *ports, __strict__=strict_cs)
                # combine[TSD](keys_ts, values_ts): the TS[tuple] zip kernel.
                return wire("convert", *ports, output_type=self)
            if self.handle.is_ts_sequence:
                # combine[TS[Tuple...]](a, b, ...): pack a structural TSB;
                # the erased tuple-combine kernel fills the row.
                fields = [(f"_{i}", _unwrap(p).ts_type) for i, p in enumerate(ports)]
                tsb_type = _m.un_named_tsb_type(fields)
                structural = WiringPort(
                    _m.tsb_port(tsb_type, {f"_{i}": _unwrap(p) for i, p in enumerate(ports)}))
                if strict_cs is False:
                    return wire("combine", structural, __strict__=False, output_type=self)
                return wire("combine", structural, output_type=self)
            return WiringPort(_m.tsl_port([_unwrap(p) for p in ports], self.handle))
        field_ports = {}
        field_types = dict(_m.ts_field_types(self.handle))
        for name, value in kwargs.items():
            if name not in field_types:
                raise TypeError(f"unknown field '{name}' for {self!r}")
            unwrapped = _unwrap(value)
            if not isinstance(unwrapped, _m.Port):
                value = wire("const", value, output_type=field_types[name])
                unwrapped = _unwrap(value)
            field_ports[name] = unwrapped
        # hgraph parity: a PARTIAL named TSB fills unsupplied fields with a
        # never-ticking source, so the bundle carries only the given fields.
        for name, ftype in field_types.items():
            if name not in field_ports:
                field_ports[name] = _unwrap(wire("nothing", output_type=_TsExpr(ftype, repr(ftype))))
        return WiringPort(_m.tsb_port(self.handle, field_ports))

    """A resolved time-series type: wraps the C++ TsType handle."""

    __slots__ = (
        "handle",
        "_label",
        "is_ref",
        "_bare_map",
        "_json",
        "_cs_class",
        "_py_class",
        "_structured_schema",
    )

    def __init__(self, handle, label):
        self.handle = handle
        self._label = label
        self.is_ref = False

    def __eq__(self, other):
        # Schema identity is the interned C++ handle (pointer identity in
        # the registry); the label is presentation only.
        if isinstance(other, _TsExpr):
            return self.handle == other.handle
        return NotImplemented

    def __hash__(self):
        return hash(repr(self.handle))

    def __repr__(self):
        return self._label

    def __or__(self, other):
        # Upstream permits ``TS[...] | None`` on ordinary Python helper
        # signatures. Wiring optionality is carried by the default/binding;
        # the native schema remains the non-None time-series type.
        if other is None or other is type(None):
            return self
        return NotImplemented

    def __ror__(self, other):
        return self.__or__(other)


def _resolve(ts):
    if isinstance(ts, _TsExpr):
        return ts.handle
    raise TypeError(f"expected a time-series type (TS[...] etc.), got {ts!r}")


class _GenericType(Exception):
    """Raised internally when a type expression contains a type variable."""

    def __init__(self, label=None, pattern=None):
        super().__init__(label)
        self.label = label
        self.pattern = pattern


def _scalar_pattern(scalar):
    try:
        return _hgraph.scalar_pattern_value(_value_type(scalar))
    except _GenericType as e:
        if e.pattern is None:
            raise TypeError(f"generic scalar {scalar!r} did not provide a C++ pattern") from e
        return e.pattern


def _type_pattern(ts):
    if isinstance(ts, (_TypeVarSentinel, _typing.TypeVar)):
        if _type_var_is_scalar(ts):
            raise TypeError(f"scalar TypeVar {ts!r} is not a time-series type")
        constraints = [
            constraint.handle
            for constraint in getattr(ts, "__constraints__", ())
            if isinstance(constraint, _TsExpr)
        ]
        if len(constraints) != len(getattr(ts, "__constraints__", ())):
            raise TypeError(f"time-series TypeVar {ts!r} has a non-concrete constraint")
        return (_hgraph.type_pattern_var(_type_var_name(ts), constraints)
                if constraints else _hgraph.type_pattern_var(_type_var_name(ts)))
    if isinstance(ts, _GenericTsExpr):
        if ts.pattern is None:
            raise TypeError(f"generic time-series {ts!r} did not provide a C++ pattern")
        return ts.pattern
    if isinstance(ts, _TsExpr):
        return _hgraph.type_pattern_concrete(ts.handle)
    raise TypeError(f"expected a time-series type (TS[...] etc.), got {ts!r}")


def _pattern_of(annotation):
    """The C++ TypePattern for ANY time-series annotation - concrete
    (_TsExpr), generic (_GenericTsExpr carries its pattern) or a bare
    sentinel. The single currency of wiring-time resolution."""
    if isinstance(annotation, _TsExpr):
        return _hgraph.type_pattern_concrete(annotation.handle)
    if isinstance(annotation, _GenericTsExpr):
        if annotation.pattern is None:
            raise TypeError(f"generic annotation {annotation!r} carries no C++ pattern")
        return annotation.pattern
    if isinstance(annotation, (_TypeVarSentinel, _typing.TypeVar)):
        return _type_pattern(annotation)
    raise TypeError(f"not a time-series annotation: {annotation!r}")


def _size_pattern(size):
    if isinstance(size, _TypeVarSentinel):
        return _hgraph.size_pattern_var(_type_var_name(size))
    return _hgraph.size_pattern_value(int(size))


class _TSMeta(type):
    def __getitem__(cls, scalar):
        import collections.abc as _abc
        import typing

        origin = typing.get_origin(scalar)
        if scalar in (typing.Callable, _abc.Callable) or origin is _abc.Callable:
            return _TsExpr(_hgraph.ts(_hgraph.value_type("callable")), "TS[Callable]")
        try:
            expr = _TsExpr(_hgraph.ts(_value_type(scalar)), f"TS[{getattr(scalar, '__name__', scalar)}]")
        except _GenericType as e:
            return _GenericTsExpr(f"TS[{scalar!r}]", pattern=_hgraph.type_pattern_ts(e.pattern))
        from ._compat import CompoundScalar as _CS

        structured_origin = typing.get_origin(scalar) or scalar
        if (
            isinstance(structured_origin, type)
            and (
                issubclass(structured_origin, _CS)
                or _is_python_object_class(structured_origin)
            )
        ):
            # ``_cs_class`` is the established storage-independent Bundle
            # marker used by combine/reflection. It deliberately names the
            # origin because Python aliases are not runtime classes.
            expr._cs_class = structured_origin
            expr._structured_schema = scalar
        if isinstance(scalar, type):
            expr._py_class = scalar   # dispatch reads the DECLARED class
        # BARE frozendict (combine[TS[frozendict]](...)): the key/value
        # types resolve from the wired inputs.
        from ._compat import JSON as _JSON2

        try:
            from frozendict import frozendict as _frozendict
        except ModuleNotFoundError:
            _frozendict = None
        if _frozendict is not None and scalar is _frozendict:
            expr._bare_map = True
        if scalar is _JSON2:
            expr._json = True
        return expr


class TS(metaclass=_TSMeta):
    """TS[scalar] — a single time-series value."""


class _TSSMeta(type):
    def __getitem__(cls, scalar):
        try:
            value_type = _value_type(scalar)
            if not value_type.is_hashable or not value_type.is_equatable:
                raise TypeError(
                    f"TSS element type {scalar!r} must be hashable and equatable")
            return _TsExpr(_hgraph.tss(value_type), f"TSS[{getattr(scalar, '__name__', scalar)}]")
        except _GenericType:
            return _GenericTsExpr(f"TSS[{scalar!r}]", pattern=_hgraph.type_pattern_tss(_scalar_pattern(scalar)))


class TSS(metaclass=_TSSMeta):
    """TSS[scalar] — a time-series set."""


class _TSDMeta(type):
    @staticmethod
    def from_ts(*args, **kwargs):
        """hgraph parity: combine[TSD](keys, *values) / TSD.from_ts(a=..., b=...)
        wires the combine_tsd operator family (static or ticking key sets)."""
        from ._wiring import wire

        strict = kwargs.pop("__strict__", None)
        if kwargs and not args:
            # the kwargs form: field names are the static key set
            args = (tuple(kwargs.keys()), *kwargs.values())
        extra = {} if strict is None else {"__strict__": strict}
        return wire("combine_tsd", *args, **extra)

    def __getitem__(cls, item):
        key, value = item
        try:
            if isinstance(value, (_GenericTsExpr, _TypeVarSentinel)):
                raise _GenericType()
            key_type = _value_type(key)
            if not key_type.is_hashable or not key_type.is_equatable:
                raise TypeError(
                    f"TSD key type {key!r} must be hashable and equatable")
            return _TsExpr(_hgraph.tsd(key_type, _resolve(value)), f"TSD[{key!r}, {value!r}]")
        except _GenericType:
            return _GenericTsExpr(
                f"TSD[{key!r}, {value!r}]",
                pattern=_hgraph.type_pattern_tsd(_scalar_pattern(key), _type_pattern(value)),
            )


class TSD(metaclass=_TSDMeta):
    """TSD[key_scalar, TS[...]] — a keyed time-series dictionary."""


class _SeriesMeta(type):
    def __getitem__(cls, element):
        # Series[T] - an arrow-backed single column. The C++ value kind is
        # the single "series" scalar; the element type documents intent and
        # rides the arrow array at runtime.
        return _value_expr_series(element)


def _value_expr_series(element):
    # A ScalarExpr-like handle over the "series" value type; TS[Series[T]]
    # wraps it as a time series.
    return _SeriesType(element)


class _SeriesType:
    """Series[T]: resolves to the 'series' scalar value type."""

    __slots__ = ("element",)

    def __init__(self, element):
        self.element = element

    def __repr__(self):
        return f"Series[{getattr(self.element, '__name__', self.element)!r}]"


class Series(metaclass=_SeriesMeta):
    """Series[T] - a first-class arrow-backed column scalar."""


class _FrameMeta(type):
    def __getitem__(cls, schema):
        # Frame[Schema] - an arrow-backed table. The C++ value kind is the
        # 'frame' scalar; the typed form carries its column bundle so table
        # operators can resolve columns (an input schema is a MINIMUM
        # requirement, an output schema is exact - the P4 ruling).
        if isinstance(schema, tuple):
            if len(schema) != 2:
                raise TypeError("Frame expects Frame[Row] or Frame[Row, Metadata]")
            return _FrameType(schema[0], schema[1])
        return _FrameType(schema)


class _FrameType:
    """Frame[Schema]: resolves to the typed 'frame' scalar value type."""

    __slots__ = ("schema", "metadata")

    def __init__(self, schema, metadata=None):
        self.schema = schema
        self.metadata = metadata

    def __repr__(self):
        row = getattr(self.schema, "__name__", self.schema)
        if self.metadata is None:
            return f"Frame[{row!r}]"
        metadata = getattr(self.metadata, "__name__", self.metadata)
        return f"Frame[{row!r}, {metadata!r}]"

    def __eq__(self, other):
        return (isinstance(other, _FrameType) and self.schema is other.schema
                and self.metadata is other.metadata)

    def __hash__(self):
        return hash((_FrameType, self.schema, self.metadata))


class Frame(metaclass=_FrameMeta):
    """Frame[Schema] - a first-class arrow-backed table scalar."""


class _TSWMeta(type):
    def __getitem__(cls, item):
        # TSW[T] / TSW[T, WindowSize[N]] / TSW[T, WindowSize[N], WindowSize[M]].
        # Each subscript form maps DIRECTLY onto a C++ type expression:
        # int sizes -> a tick window, timedelta sizes -> a duration window,
        # a WINDOW_SIZE / WINDOW_SIZE_MIN sentinel anywhere -> the generic
        # window pattern (sizes resolve from the wired port). The bare form
        # carries period 0 (supplied at to_window time).
        items = item if isinstance(item, tuple) else (item,)
        value, sizes = items[0], items[1:]
        label = f"TSW[{item!r}]"
        if any(isinstance(size, _TypeVarSentinel) for size in sizes):
            try:
                element = _hgraph.scalar_pattern_value(_value_type(value))
            except _GenericType as e:
                element = e.pattern
            return _GenericTsExpr(label, pattern=_hgraph.type_pattern_tsw(element))
        try:
            value_type = _value_type(value)
        except _GenericType as e:
            period = int(sizes[0]) if sizes and isinstance(sizes[0], int) else 0
            pattern = (_hgraph.type_pattern_tsw(e.pattern, period)
                       if period > 0 else _hgraph.type_pattern_tsw(e.pattern))
            return _GenericTsExpr(label, pattern=pattern)
        if any(isinstance(size, datetime.timedelta) for size in sizes):
            return _TsExpr(_hgraph.tsw_duration(value_type, *sizes), label)
        return _TsExpr(_hgraph.tsw(value_type, *(sizes or (0,))), label)


class TSW(metaclass=_TSWMeta):
    """TSW[T] — a tick-based window over TS[T]."""


class WindowSize:
    """WindowSize[N] — TSW size marker (N ticks, or a timedelta duration)."""

    def __class_getitem__(cls, size):
        if isinstance(size, datetime.timedelta):
            return size
        return int(size)


class Size:
    """Size[N] — the fixed-size marker for TSL."""

    def __class_getitem__(cls, size):
        return int(size)


class _TSLMeta(type):
    @staticmethod
    def from_ts(*ports, tp=None):
        """hgraph parity: build a TSL from individual TS ports. A single
        iterable argument expands; ``tp=`` forces the element type (ports of
        a narrower type convert to it)."""
        import types
        import _hgraph as _m

        from ._wiring import WiringPort, _unwrap, wire

        if len(ports) == 1 and isinstance(ports[0], (types.GeneratorType, list, tuple)):
            ports = tuple(ports[0])
        if tp is not None and isinstance(tp, _TsExpr):
            ports = tuple(
                p if isinstance(p, WiringPort) and _unwrap(p).ts_type == tp.handle
                else wire("convert", p, output_type=tp)
                for p in ports)
        return WiringPort(_m.tsl_port([_unwrap(p) for p in ports]))

    def __getitem__(cls, item):
        element, size = item
        try:
            if isinstance(element, (_GenericTsExpr, _TypeVarSentinel)) or isinstance(size, _TypeVarSentinel):
                raise _GenericType()
            return _TsExpr(_hgraph.tsl(_resolve(element), int(size)), f"TSL[{element!r}, {size}]")
        except _GenericType:
            return _GenericTsExpr(
                f"TSL[{element!r}, {size!r}]",
                pattern=_hgraph.type_pattern_tsl(_type_pattern(element), _size_pattern(size)),
            )


class TSL(metaclass=_TSLMeta):
    """TSL[TS[...], Size[N]] — a fixed-size time-series list."""


class TimeSeriesSchema:
    """Subclass with annotated TS fields to describe a TSB shape."""

    __scalar_type__ = None

    @classmethod
    def scalar_type(cls):
        """Return the CompoundScalar this schema was lifted from, if any."""
        return cls.__dict__.get("__scalar_type__")

    @staticmethod
    def from_scalar_schema(schema):
        """Lift every logical structured scalar field to a ``TS`` field.

        The generated class is cached on the scalar schema, matching the
        upstream API and preserving nominal identity across repeated calls.
        """
        from ._compat import CompoundScalar

        if schema is CompoundScalar:
            return TimeSeriesSchema
        if (
            not isinstance(schema, type)
            or not (
                issubclass(schema, CompoundScalar)
                or _is_python_object_class(schema)
            )
        ):
            raise TypeError(
                "Can only create bundle schema from structured scalar "
                f"classes, not {schema!r}")
        cached = schema.__dict__.get("__bundle_type__")
        if cached is not None:
            return cached

        annotations = {
            name: TS[annotation]
            for name, annotation in (
                _compound_python_field_types(schema)
                if issubclass(schema, CompoundScalar)
                else _python_object_python_field_types(schema)
            ).items()
        }
        bundle = type(
            f"{schema.__name__}Bundle",
            (TimeSeriesSchema,),
            {"__annotations__": annotations, "__module__": schema.__module__},
        )
        bundle.__scalar_type__ = schema
        schema.__bundle_type__ = bundle
        return bundle


class _TSBMeta(type):
    def __getitem__(cls, schema):
        if isinstance(schema, (_TypeVarSentinel, _typing.TypeVar)):
            return _GenericTsExpr(
                f"TSB[{schema!r}]", pattern=_hgraph.type_pattern_tsb(_type_var_name(schema)))
        # hgraph's INLINE schema: TSB["lhs": TS[int], "rhs": TS[int]] - an
        # un-named structural bundle with the given fields.
        if isinstance(schema, slice):
            schema = (schema,)
        if isinstance(schema, tuple) and schema and all(isinstance(s, slice) for s in schema):
            fields = [(s.start, _resolve(s.stop)) for s in schema]
            label = ", ".join(f"{n}: {t!r}" for n, t in fields)
            return _TsExpr(_hgraph.un_named_tsb_type(fields), f"TSB[{label}]")
        # ``typing.Generic`` produces an alias rather than a class. Resolve
        # the origin for MRO/field discovery and specialize its field patterns
        # through the shared C++ TypePattern substitution primitive.
        import typing
        from ._compat import CompoundScalar as _CS

        origin = typing.get_origin(schema) or schema
        type_args = tuple(typing.get_args(schema))
        # Compatibility schemas such as BoolResult manufacture a plain
        # annotated class. TimeSeriesSchema remains the public authoring base,
        # but the structural legacy form is intentionally accepted here.
        if not isinstance(origin, type):
            raise TypeError(f"TSB expects an annotated schema class, got {schema!r}")
        parameters = tuple(getattr(origin, "__parameters__", ()))
        if type_args and len(type_args) != len(parameters):
            raise TypeError(
                f"TimeSeriesSchema {origin.__qualname__} expects {len(parameters)} generic arguments, "
                f"got {len(type_args)}")

        scalar_replacements = {}
        size_replacements = {}
        ts_replacements = {}
        for parameter, argument in zip(parameters, type_args):
            if isinstance(argument, int) and not isinstance(argument, bool):
                size_replacements[_type_var_name(parameter)] = _hgraph.size_pattern_value(argument)
                continue
            if isinstance(argument, (_TypeVarSentinel, typing.TypeVar)):
                replacement = _hgraph.scalar_pattern_var(_type_var_name(argument))
                size_replacements[_type_var_name(parameter)] = _hgraph.size_pattern_var(
                    _type_var_name(argument))
            else:
                replacement = _scalar_pattern(argument)
            scalar_replacements[_type_var_name(parameter)] = replacement

            ts_argument = None
            if isinstance(argument, _TsExpr):
                ts_argument = argument
            elif isinstance(argument, type) and issubclass(argument, TimeSeriesSchema):
                ts_argument = cls[argument]
            if isinstance(ts_argument, _TsExpr):
                ts_replacements[_type_var_name(parameter)] = ts_argument.handle

        # hgraph parity: schema INHERITANCE - base-class fields first (MRO
        # reversed), subclass fields after; later duplicates override.
        annotations = {}
        for klass in reversed(origin.__mro__):
            annotations.update(getattr(klass, "__annotations__", {}))
        is_cs = issubclass(origin, _CS)
        is_python_object = _is_python_object_class(origin)
        compound_meta = None
        if is_cs or is_python_object:
            # TSB[structured scalar]: scalar annotations LIFT to TS fields;
            # the bundle keeps the scalar Bundle name so conversion uses the
            # registered native or Python-owned construction policy.
            try:
                compound_meta = _value_type(schema)
            except _GenericType:
                # Keep an unspecialized structured scalar as a C++ type
                # pattern; its nominal Bundle is created after resolution.
                compound_meta = None
            substitutions = dict(zip(parameters, type_args))
            annotations = {
                name: TS[_substitute_typevars(tp, substitutions)]
                for name, tp in (
                    _compound_python_field_types(origin)
                    if is_cs
                    else _python_object_python_field_types(origin)
                ).items()
            }

        field_names = []
        field_patterns = []
        fields = []
        generic = False
        scope = _hgraph.ResolutionScope()
        for name, replacement in ts_replacements.items():
            scope.bind_ts(name, replacement)
        for name, ts in annotations.items():
            pattern = _pattern_of(ts)
            if scalar_replacements:
                pattern = _hgraph.type_pattern_substitute_scalars(pattern, scalar_replacements)
            if size_replacements:
                pattern = _hgraph.type_pattern_substitute_sizes(pattern, size_replacements)
            resolved = scope.resolve_ts(pattern)
            field_names.append(name)
            field_patterns.append(pattern)
            if resolved is None:
                generic = True
            else:
                fields.append((name, resolved))
        if generic:
            return _GenericTsExpr(
                f"TSB[{origin.__name__}]",
                pattern=_hgraph.type_pattern_tsb_fields(field_names, field_patterns))

        # The registry's TSB namespace is GLOBAL; python classes are scoped
        # (tests re-define same-named local schemas freely). Qualify with the
        # module + qualname so distinct classes never collide; the plain
        # __name__ stays for stable top-level classes (nicer diagnostics).
        name = compound_meta.name if compound_meta is not None else origin.__name__
        qualname = getattr(origin, "__qualname__", origin.__name__)
        if compound_meta is None and "<locals>" in qualname:
            # Function-local TimeSeriesSchema classes need nominal isolation.
            name = f"{origin.__module__}.{qualname}"
        if compound_meta is None and type_args:
            name += "[" + ",".join(_compound_specialization_token(arg) for arg in type_args) + "]"
        expression = _TsExpr(_hgraph.tsb(name, fields), f"TSB[{origin.__name__}]")
        _TSB_SCHEMA_CLASSES[expression.handle] = origin
        if compound_meta is not None:
            _hgraph.register_tsb_compound_class(expression.handle, compound_meta)
        return expression


class TSB(metaclass=_TSBMeta):
    """TSB[SchemaClass] — a named time-series bundle."""


class _ContextExpr:
    """CONTEXT[X] — a context-injected parameter's type marker."""

    __slots__ = ("ts",)

    def __init__(self, ts):
        self.ts = ts

    def __repr__(self):
        return f"CONTEXT[{self.ts!r}]"


class _CONTEXTMeta(type):
    def __getitem__(cls, item):
        if isinstance(item, (_TsExpr, _GenericTsExpr)):
            return _ContextExpr(item)
        if isinstance(item, (_TypeVarSentinel, _typing.TypeVar)) and not _type_var_is_scalar(item):
            return _ContextExpr(item)
        # CONTEXT[SomeScalar] means CONTEXT[TS[SomeScalar]] (hgraph parity).
        return _ContextExpr(TS[item])


class CONTEXT(metaclass=_CONTEXTMeta):
    """Annotate a node parameter as context-injected: resolved from the
    nearest published ``with port:`` context of matching type (and name,
    when specified). Default ``None`` = optional; ``REQUIRED`` /
    ``REQUIRED["name"]`` = mandatory."""


class _Required:
    __slots__ = ("name",)

    def __init__(self, name=None):
        self.name = name

    def __getitem__(self, name):
        return _Required(name)

    def __repr__(self):
        return f"REQUIRED[{self.name!r}]" if self.name else "REQUIRED"


REQUIRED = _Required()


class _TypeVarSentinelMeta(type):
    def __instancecheck__(cls, instance):
        import typing

        return super().__instancecheck__(instance) or isinstance(instance, typing.TypeVar)


class _TypeVarSentinel(metaclass=_TypeVarSentinelMeta):
    """hgraph's generic type variables (SCALAR / TIME_SERIES_TYPE / ...):
    usable as annotations - resolution happens from the wired arguments,
    exactly like an un-annotated parameter. ``is_scalar`` mirrors upstream's
    HgScalarTypeVar/HgTimeSeriesTypeVar split (a SCALAR-kind parameter is
    never a time-series input)."""

    __slots__ = ("name", "is_scalar", "__constraints__")

    def __init__(self, name, is_scalar=False, constraints=()):
        self.name = name
        self.is_scalar = is_scalar
        self.__constraints__ = tuple(constraints)

    def __repr__(self):
        return self.name


import typing as _typing


def _type_var_name(value):
    """Return the resolution name without mutating ``typing.TypeVar`` objects."""
    return getattr(value, "name", getattr(value, "__name__", str(value)))


def _type_var_is_scalar(value):
    """Classify a generic variable by its declared bound or constraints.

    Python ``TypeVar`` is used for both scalar schemas and time-series schemas
    by compatibility clients. A time-series bound/constraint is authoritative;
    unbounded variables retain the scalar default used by CompoundScalar
    generics.
    """
    if not isinstance(value, _typing.TypeVar):
        return bool(getattr(value, "is_scalar", False))
    candidates = (*getattr(value, "__constraints__", ()), getattr(value, "__bound__", None))
    return not any(
        isinstance(candidate, (_TsExpr, _GenericTsExpr))
        or (
            isinstance(candidate, type)
            and issubclass(candidate, TimeSeriesSchema)
        )
        for candidate in candidates
    )


SCALAR = _typing.TypeVar("SCALAR")
SCHEMA = _typing.TypeVar("SCHEMA", bound="CompoundScalar")
TS_SCHEMA = _TypeVarSentinel("TS_SCHEMA")
SCALAR_1 = _TypeVarSentinel("SCALAR_1", is_scalar=True)
SCALAR_2 = _TypeVarSentinel("SCALAR_2", is_scalar=True)
KEYABLE_SCALAR = _TypeVarSentinel("KEYABLE_SCALAR", is_scalar=True)
NUMBER = _TypeVarSentinel("NUMBER", is_scalar=True, constraints=(int, float))
NUMBER_2 = _TypeVarSentinel("NUMBER_2", is_scalar=True, constraints=(int, float))
TIME_SERIES_TYPE = _TypeVarSentinel("TIME_SERIES_TYPE")
TIME_SERIES_TYPE_1 = _TypeVarSentinel("TIME_SERIES_TYPE_1")
TIME_SERIES_TYPE_2 = _TypeVarSentinel("TIME_SERIES_TYPE_2")
OUT = _TypeVarSentinel("OUT")
K_1 = _TypeVarSentinel("K_1", is_scalar=True)
SIZE = _typing.TypeVar("SIZE")
V = _TypeVarSentinel("V")
K = _TypeVarSentinel("K", is_scalar=True)
WINDOW_SIZE = _TypeVarSentinel("WINDOW_SIZE", is_scalar=True)
ENUM = _TypeVarSentinel("ENUM", is_scalar=True)
WINDOW_SIZE_MIN = _TypeVarSentinel("WINDOW_SIZE_MIN", is_scalar=True)
TABLE = _TypeVarSentinel("TABLE", is_scalar=True)
COMPOUND_SCALAR = _typing.TypeVar("COMPOUND_SCALAR", bound="CompoundScalar")
COMPOUND_SCALAR_1 = _typing.TypeVar("COMPOUND_SCALAR_1", bound="CompoundScalar")


def with_signature(fn=None, *, annotations=None, args=None, kwargs=None, defaults=None,
                   return_annotation=None):
    """hgraph parity (ported from upstream _typing_utils): rewrite a
    function's signature - per-param annotation overrides, expansion of
    ``*args``/``**kwargs`` into named parameters, and the return type."""
    from inspect import signature, Parameter, Signature

    if fn is None:
        return lambda f: with_signature(f, annotations=annotations, args=args, kwargs=kwargs,
                                        defaults=defaults, return_annotation=return_annotation)

    sig = signature(fn)
    annotations = annotations or {}
    defaults = defaults or {}
    new_params = []
    new_annotations = {}
    for n, parameter in sig.parameters.items():
        if parameter.kind in (Parameter.POSITIONAL_OR_KEYWORD, Parameter.POSITIONAL_ONLY,
                              Parameter.KEYWORD_ONLY):
            if n in annotations:
                new_params.append(Parameter(n, Parameter.POSITIONAL_OR_KEYWORD,
                                            annotation=annotations[n],
                                            default=defaults.get(n, Parameter.empty)))
                new_annotations[n] = annotations[n]
            else:
                new_params.append(parameter)
                new_annotations[n] = parameter.annotation
        if parameter.kind == Parameter.VAR_POSITIONAL:
            if args is None:
                if n in annotations:
                    new_params.append(Parameter(n, Parameter.VAR_POSITIONAL,
                                                annotation=annotations[n],
                                                default=defaults.get(n, Parameter.empty)))
                    new_annotations[n] = annotations[n]
                else:
                    raise ValueError(
                        f"with_signature was not provided annotations for args however there is a "
                        f"*{n} and no entry in annotations")
            else:
                for n, a in args.items():
                    new_params.append(Parameter(n, Parameter.POSITIONAL_OR_KEYWORD, annotation=a,
                                                default=defaults.get(n, Parameter.empty)))
                    new_annotations[n] = a
            args = None
        if parameter.kind == Parameter.VAR_KEYWORD:
            for n, a in (kwargs or {}).items():
                new_params.append(Parameter(n, Parameter.KEYWORD_ONLY, annotation=a,
                                            default=defaults.get(n, Parameter.empty)))
                new_annotations[n] = a
            kwargs = None

    if args is not None:
        raise ValueError("with_signature was provided annotations for *args however there is no "
                         "*argument in the current function signature")
    if kwargs is not None:
        raise ValueError("with_signature was provided annotations for **kwargs however there is no "
                         "**argument in the current function signature")
    if return_annotation is not None:
        new_annotations["return"] = return_annotation

    fn.__signature__ = Signature(parameters=new_params, return_annotation=return_annotation)
    fn.__annotations__ = new_annotations
    return fn


class _GenericTsExpr:
    """A generic (unresolved) time-series annotation: TS[SCALAR] etc.
    Treated like an absent annotation - types resolve from wired ports or
    sample values. ``is_ref``/``inner`` carry REF[TYPEVAR] structure so
    generic py nodes can resolve from actual arguments."""

    __slots__ = ("label", "is_ref", "inner", "pattern")

    def __init__(self, label, is_ref=False, inner=None, pattern=None):
        self.label = label
        self.is_ref = is_ref
        self.inner = inner
        self.pattern = pattern

    def __repr__(self):
        return self.label

    def __or__(self, other):
        if other is None or other is type(None):
            return self
        return NotImplemented

    def __ror__(self, other):
        return self.__or__(other)


# SIGNAL - an input consumed for its ticks only; any time-series type binds.
SIGNAL = _GenericTsExpr("SIGNAL", pattern=_hgraph.type_pattern_signal())


class _ArrayType:
    """Resolved or generic shaped-array scalar expression."""

    __slots__ = ("element", "dimensions")

    def __init__(self, element, dimensions):
        self.element = element
        self.dimensions = tuple(dimensions)

    @property
    def __args__(self):
        return (self.element, *self.dimensions)

    def __repr__(self):
        suffix = "" if not self.dimensions else ", " + ", ".join(
            f"Size[{dimension}]" for dimension in self.dimensions
        )
        return f"Array[{self.element!r}{suffix}]"

    def __eq__(self, other):
        return (isinstance(other, _ArrayType) and self.element == other.element
                and self.dimensions == other.dimensions)

    def __hash__(self):
        return hash((self.element, self.dimensions))


class Array:
    """Array[T, Size[...]] - a native shaped-array scalar annotation."""

    def __class_getitem__(cls, item):
        items = item if isinstance(item, tuple) else (item,)
        if not items:
            raise TypeError("Array requires an element type")
        return _ArrayType(items[0], items[1:])


def ts_schema(**kwargs):
    """hgraph parity: build an un-named TimeSeriesSchema type from kwargs.
    The type name is derived from the field spec so equal schemas intern to
    the same TSB name and different ones never collide."""
    import hashlib

    label = "_".join(f"{name}_{ts!r}" for name, ts in kwargs.items())
    digest = hashlib.md5(label.encode()).hexdigest()[:12]
    schema = type(f"UnNamedTimeSeriesSchema_{digest}", (TimeSeriesSchema,), {})
    schema.__annotations__ = dict(kwargs)
    return schema


def compound_scalar(**kwargs):
    """hgraph parity: build an un-named CompoundScalar type from kwargs (the
    ts_schema analogue at the scalar layer). The type name derives from the
    field spec so equal shapes intern to the same C++ bundle."""
    import dataclasses
    import hashlib

    from ._compat import CompoundScalar

    label = "_".join(f"{name}_{tp!r}" for name, tp in kwargs.items())
    digest = hashlib.md5(label.encode()).hexdigest()[:12]
    cls = type(f"UnNamedCompoundScalar_{digest}", (CompoundScalar,),
               {"__annotations__": dict(kwargs), "__unnamed_compound__": True})
    return dataclasses.dataclass(frozen=True)(cls)


class _DefaultMeta(type):
    def __getitem__(cls, item):
        return item   # DEFAULT[OUT] documents the defaulted output


class DEFAULT(metaclass=_DefaultMeta):
    """hgraph's DEFAULT[...] output marker (documentary here)."""


class _KeyValueMeta(type):
    def __getitem__(cls, item):
        # KeyValue[K, TS_TYPE] - hgraph's generic key/value schema for
        # dict-to-bundle conversions: fields key: TS[K], value: TS_TYPE.
        key_scalar, value_ts = item
        label = f"KeyValue[{key_scalar!r}, {value_ts!r}]"
        schema = type("KeyValue", (TimeSeriesSchema,), {
            "__annotations__": {"key": TS[key_scalar], "value": value_ts},
        })
        schema.__qualname__ = f"<locals>.{label}"   # unique registry name per instantiation
        return schema


class KeyValue(metaclass=_KeyValueMeta):
    """KeyValue[K, TS] - the key/value TimeSeriesSchema (hgraph parity)."""


class _AutoResolve:
    """AUTO_RESOLVE - a Type[...] parameter default that receives the
    RESOLVED type at wiring (hgraph parity)."""

    def __repr__(self):
        return "AUTO_RESOLVE"


AUTO_RESOLVE = _AutoResolve()


class _REFMeta(type):
    def __getitem__(cls, item):
        # Howard's REF ruling (2026-07-05): references are OPAQUE VALUES -
        # storable and emittable, never dereferenced (.output is not
        # exposed). A REF[X] input receives the reference itself; a non-REF
        # input bound to a REF source receives the DEREFERENCED value.
        import _hgraph as _m

        if isinstance(item, (_TypeVarSentinel, _GenericTsExpr)):
            # REF over a generic: resolved at call time from the actual arg.
            pattern = _hgraph.type_pattern_ref(
                _hgraph.type_pattern_var(_type_var_name(item))
                if isinstance(item, _TypeVarSentinel) else _type_pattern(item)
            )
            return _GenericTsExpr(f"REF[{item!r}]", is_ref=True, inner=item, pattern=pattern)
        expr = _TsExpr(_m.ref_ts(_resolve(item)), f"REF[{item!r}]")
        expr.is_ref = True
        return expr


class REF(metaclass=_REFMeta):
    """REF[X] - an opaque reference over X: pass/store/emit the reference
    value; dereferencing (.output) is not exposed (agreed deviation)."""
