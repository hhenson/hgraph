from collections.abc import Mapping
from functools import cache
from inspect import getattr_static
from struct import pack
from types import GetSetDescriptorType, MemberDescriptorType

from hgraph._types import _is_python_object_class, _python_object_python_field_types


_NEGATIVE_ZERO_BITS = pack("!d", -0.0)


def _stored_fields(cls):
    return _registered_fields(cls) if _is_python_object_class(cls) else ()


@cache
def _registered_fields(cls):
    names = []
    for name, annotation in _python_object_python_field_types(cls).items():
        descriptor = getattr_static(cls, name, None)
        if not isinstance(descriptor, MemberDescriptorType) and hasattr(type(descriptor), "__get__"):
            if annotation not in (bool, int, str, bytes, type(None)):
                # Its result may carry a zero sign. Do not execute this getter.
                return None
        else:
            names.append(name)
    return tuple(names)


def _backing_key(value, active):
    """Compare ordinary backing data without invoking application equality or getters."""
    cls = type(value)
    if cls in (bool, int, str, bytes, type(None)):
        return cls, value
    if cls is float:
        return float, pack("!d", value)
    if cls not in (tuple, list, dict, set, frozenset) or id(value) in active:
        return object, id(value)
    active.add(id(value))
    try:
        if cls is dict:
            return dict, frozenset((_backing_key(k, active), _backing_key(v, active)) for k, v in value.items())
        members = (_backing_key(item, active) for item in value)
        return cls, frozenset(members) if cls in (set, frozenset) else tuple(members)
    finally:
        active.remove(id(value))


def _storage_descriptors(cls):
    for owner in cls.__mro__:
        for name, descriptor in vars(owner).items():
            if isinstance(descriptor, MemberDescriptorType) or (
                name == "__dict__" and isinstance(descriptor, GetSetDescriptorType)
            ):
                yield owner, name, descriptor


def _descriptor_storage(value):
    members = []
    for owner, name, descriptor in _storage_descriptors(type(value)):
        try:
            raw = descriptor.__get__(value, type(value))
        except AttributeError:
            continue
        if name != "__dict__" or raw:
            members.append((id(owner), name, _backing_key(raw, set())))
    # A class with no inspectable state can compute a property from identity.
    return tuple(members) if members else id(value)


def _field_values(value, names):
    for name in names:
        try:
            yield name, object.__getattribute__(value, name)
        except AttributeError:
            pass


def _mapping_locations(value, active):
    result = []
    for key, member in value.items():
        key_signs = _locations(key, active)
        value_signs = _locations(member, active)
        if key_signs or value_signs:
            result.append((key, key_signs, value_signs))
    return frozenset(result) or None


def _member_locations(value, active):
    if isinstance(value, Mapping):
        return _mapping_locations(value, active)
    if isinstance(value, (set, frozenset)):
        return frozenset((item, signs) for item in value if (signs := _locations(item, active))) or None
    if isinstance(value, (tuple, list)):
        items = enumerate(value)
    else:
        names = _stored_fields(type(value))
        if names is None:
            return ("descriptor", _descriptor_storage(value))
        items = _field_values(value, names)
    return tuple((key, signs) for key, item in items if (signs := _locations(item, active))) or None


def _locations(value, active):
    """Record signs by carried key for unordered values and heterogeneous pairs."""
    if isinstance(value, float):
        return True if pack("!d", value) == _NEGATIVE_ZERO_BITS else None
    if type(value) in (int, str, bytes, bool, type(None)) or id(value) in active:
        return None
    active.add(id(value))
    try:
        return _member_locations(value, active)
    finally:
        active.remove(id(value))


def _matching_members(a, b, same):
    if type(a) is type(b):
        if isinstance(a, (tuple, list)) and len(a) == len(b):
            return all(same(x, y) for x, y in zip(a, b))
        fields = _stored_fields(type(a))
        if fields is None:
            return _descriptor_storage(a) == _descriptor_storage(b)
        if fields:
            left = dict(_field_values(a, fields))
            right = dict(_field_values(b, fields))
            return all(same(left.get(key), right.get(key)) for key in left.keys() | right.keys())
    return _locations(a, set()) == _locations(b, set())


def stored_zero_signs_match(left, right, plain):
    """Inspect retained fields without coercing their values or calling user getters."""
    active = set()

    def same(a, b):
        if a is b:
            return True
        result = plain(a, b)
        if result is not None:
            return result
        pair = id(a), id(b)
        if pair in active:
            return True
        active.add(pair)
        try:
            return _matching_members(a, b, same)
        finally:
            active.remove(pair)

    return same(left, right)
