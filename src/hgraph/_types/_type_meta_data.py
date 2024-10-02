from functools import lru_cache
from typing import TypeVar, Type, Optional, TYPE_CHECKING


if TYPE_CHECKING:
    from hgraph._types._ref_meta_data import HgREFTypeMetaData

__all__ = ("ParseError", "HgTypeMetaData", "AUTO_RESOLVE")


AUTO_RESOLVE = object()  # Used to indicate that a type should be auto-resolved

TYPE = TypeVar("TYPE", bound=type)


class ParseError(RuntimeError):
    """
    Generated if a parsing error occurs when extracting type metadata from a given input.
    """


class HgTypeMetaData:
    """
    The type meta-data provides reflective information describing a supplied type. It contains
    a collection of useful methods and properties that are used when wiring and reasoning about
    HGraph signatures.

    The key entry-point into this class is the parse_type and parse_value methods. These methods construct
    instances of ``HgTypeMetaData`` from the given inputs. This is the top level class and can construct
    any of the supported types. It is possible to use either the ``HgScalarTypeMetaData`` or ``HgTimeSeriesTypeMetaData``
    to parse the types with if you know what type is expected.

    This defines the following useful properties:

    is_resolved
        If this instance of type is resolved, or in other words, does this type or any of its children
        contain an unresolved type. (Unresolved means has a TypeVar in the type declaration)

    is_scalar
        True if this represents a scalar type.

    is_atomic
        True if this represents a type has no child elements. For example, a str or int type.
        An example of a NON-ATOMIC element could be tuple[int, ...] where this has additional
        child type information (in this case an int type).

    is_generic
        True if this represents a generic type or in other words, a TypeVar element.

    is_injectable
        True if this represents an injectable type. These are types that are required to be
        injected into the function signature, for example, STATE.

    is_reference
        True if this type is a reference type.

    is_context_manager
        True if this type represents a context manager.

    is_context_wired
        Is this an auto-wired input from a context.

    py_type
        The python type this meta-data type represents.
    """

    is_resolved: bool  # Does this instance of metadata contain a generic entry, i.e. requires resolution
    is_scalar: bool
    is_atomic: bool = False
    is_generic: bool = False  # Is this instance of metadata representing a template type (i.e. TypeVar)
    is_injectable: bool = False  # This indicates the type represents an injectable property (such as ExecutionContext)
    is_reference: bool = False
    is_context_manager: bool = False  # Is this a context manager type
    is_context_wired: bool = False  # Is this auto-wiring from context
    py_type: Type  # The python type that represents this type

    @classmethod
    @lru_cache(maxsize=None)
    def parse_type(cls, value_tp) -> Optional["HgTypeMetaData"]:
        """
        This accepts a python type (``value_tp``) and returns the ``HgTypeMetaData`` instance that represents
        the type supplied. If the type does not resolve to a valid HGraph type, ``ParseError`` is raised.
        """
        from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData
        from hgraph._types._time_series_meta_data import HgTimeSeriesTypeMetaData

        parse_order = (HgTimeSeriesTypeMetaData, HgScalarTypeMetaData)
        if isinstance(value_tp, parse_order):
            return value_tp
        for parser in parse_order:
            if meta_data := parser.parse_type(value_tp):
                return meta_data
        raise ParseError(f"Unable to parse '{value_tp}'")

    @classmethod
    def parse_value(cls, value) -> Optional["HgTypeMetaData"]:
        """
        Attempts to determine the HGraph type from the value supplied.
        This is not as reliable as parsing the type and could result in an
        incorrect result or may be unable to extract the type at all.
        But it is useful for auto-resolution and validation.
        """
        from hgraph._types._time_series_meta_data import HgTimeSeriesTypeMetaData
        from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData

        parse_order = (HgTimeSeriesTypeMetaData, HgScalarTypeMetaData)
        if isinstance(value, HgTypeMetaData):
            raise ParseError(f"Parse value was passed a type meta instead '{value}'")
        for parser in parse_order:
            if meta_data := parser.parse_value(value):
                return meta_data
        raise ParseError(f"Unable to parse '{value}'")

    def matches(self, tp: "HgTypeMetaData") -> bool:
        """
        Can this instance of meta-data match the supplied type?
        This is used to determine if a type can be wired to another type.
        It does not provide a guarantee that the types are compatible, only that they could match.
        For example:

        ::

            add_(lhs: TS[NUMERIC], rhs: TS[NUMERIC])

        in this case ``TS[int]`` and ``TS[float]`` could match,
        but if the inputs to lhs and rhs were ``TS[int]`` and ``TS[float]`` respectively,
        then the types would not be a match for each input but not for the function as a whole.
        """
        return self.py_type == tp.py_type  # By default, if the python types are the same, then the types match.

    def matches_type(self, tp: TYPE):
        """
        Will match a standard python type (``tp``) to this HGraph type.
        The function is effectively a call to ``parse_type`` and then calls ``match`` on the result.
        """
        return self.matches(self.parse_type(tp))

    def resolve(self, resolution_dict, weak: bool = False) -> "HgTypeMetaData":
        """
        Resolve any ``TypeVar`` s that are found in this type using the ``resolution_dict`` provided. If weak is
        ``False`` and a type can't be resolved, it will raise an Exception. If weak is set to ``True`` then
        any types that are unresolved remain unresolved in the returned type.

        :param resolution_dict: The dictionary containing the currently resolved types so far (TypeVar->HgTypeMetaData).
        :param weak: When ``True`` this will self when an exact resolution is not possible.
        :return: The resolved HGraph type instance.
        """
        if self.is_resolved:
            return self

    @property
    def has_references(self) -> bool:
        """
        :return: True if this type is or has a reference in it.
        """
        return False

    def dereference(self) -> "HgTypeMetaData":
        """
        Returns the dereferenced the type.

        :return: The dereferenced value of the type, this is performed recursively. The resultant type will represent
                 the type without any reference value included.
        """
        return self

    def as_reference(self) -> "HgREFTypeMetaData":
        """
        Converts the type meta-data to a reference type if the type is not already a reference type.
        If the type is already a reference type, it will be returned as is.

        .. note:: This DOES NOT recurse the type hierarchy.

        :return: The reference variation of this type (or itself).
        """
        from hgraph._types._ref_meta_data import HgREFTypeMetaData
        from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData

        return self if isinstance(self, (HgREFTypeMetaData, HgScalarTypeMetaData)) else HgREFTypeMetaData(self)

    @property
    def type_vars(self) -> set:
        """
        :return: The set of type-vars that are associated to this type instance.
        """
        return set()

    @property
    def generic_rank(self) -> dict[type, float]:
        """
        The generic rank indicates how imprecision exists in the type. The higher the rank, the more imprecise.
        With the highest rank being 1.0 and the smallest being 0.0.
        This ranking is used to determine the best match when wiring types by summing up the ranks and picking
        the lowest sum of the inputs as the best match.
        """
        return {}

    def build_resolution_dict(self, resolution_dict, wired_type: "HgTypeMetaData"):
        """
        Attempts to resolve any unresolved types using the wired type supplied. Any resolutions made are added to the
        ``resolution_dict``. This is used to:

        1. Validate that resolutions made previously for the same ``TypeVar`` instances are still valid.
        2. When resolution is made to a different type, determine if the types are convertible, if so, pick the lowest
           conversion to bind to.

        Once all the types have had a go at determining the ``resolution_dict``, the types are resolved for real in a
        second pass. The outputs are fully reliant on types to be resolved using the ``wired_type`` s on the inputs to
        resolve the output types.

        :param resolution_dict: A dictionary of TypeVar to HgTypeMetaData instances.
        :param wired_type: The wired type provided to assist in building the type-resolution dictionary.
        """
        if self.is_resolved:
            return

        if self != wired_type:
            self.do_build_resolution_dict(resolution_dict, wired_type.dereference() if wired_type else None)

    def do_build_resolution_dict(self, resolution_dict, wired_type: "HgTypeMetaData"):
        """
        Implementation method for ``build_resolution_dict`` - to be overridden by the derived classes.
        Do not override the ``build_resolution_dict`` method in derived classes.

        :param resolution_dict: A dictionary of TypeVar to HgTypeMetaData instances.
        :param wired_type: The wired type provided to assist in building the type-resolution dictionary.
        """
        if wired_type is not None and type(self) != type(wired_type):
            from hgraph._wiring._wiring_errors import IncorrectTypeBinding

            raise IncorrectTypeBinding(self, wired_type)
