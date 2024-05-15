from typing import TypeVar, Type, Optional

__all__ = ('ParseError', 'HgTypeMetaData', 'AUTO_RESOLVE')


AUTO_RESOLVE = object()  # Used to indicate that a type should be auto-resolved


class ParseError(RuntimeError):
    ...


class HgTypeMetaData:
    is_resolved: bool  # Does this instance of metadata contain a generic entry, i.e. requires resolution
    is_scalar: bool
    is_atomic: bool = False
    is_generic: bool = False  # Is this instance of metadata representing a template type (i.e. TypeVar)
    is_injectable: bool = False  # This indicates the type represent an injectable property (such as ExecutionContext)
    is_reference: bool = False
    is_context_manager: bool = False  # Is this a context manager type
    is_context_wired: bool = False  # Is this auto-wiring from context
    py_type: Type  # The python type that represents this type

    @classmethod
    def parse_type(cls, value_tp) -> Optional["HgTypeMetaData"]:
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
        For example: add_(lhs: TS[NUMERIC], rhs: TS[NUMERIC]), in this case TS[int] and TS[float] could match,
        but if the inputs to lhs and rhs were TS[int] and TS[float] respectively, then the types would not be a match
        for each individual input but not for the function as a whole.
        """
        return self.py_type == tp.py_type  # By default if the python types are the same, then the types match.

    def is_sub_class(self, tp: "HgTypeMetaData") -> bool:
        """
        If this meta data a sub-class of the other, determines convertibility. That is in a wiring context,
        it is possible to supply a sub-class of a type into an input constrained by the type.
        """
        raise NotImplementedError()

    def is_convertable(self, tp: "HgTypeMetaData") -> bool:
        """
        Is it possible to convert from the source type to the destination *type*. This is used to support
        automatic type conversions where it makes sense.
        """
        raise NotImplementedError()

    def resolve(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], weak=False) -> "HgTypeMetaData":
        """
        Return a resolve type instance using the resolution dictionary supplied to map type var instances
        to resolved types.
        If there are missing types an appropriate exception should be thrown.
        :param weak:
        """
        if self.is_resolved:
            return self

    @property
    def has_references(self) -> bool:
        return False

    def dereference(self) -> "HgTypeMetaData":
        return self

    @property
    def typevars(self):
        return ()

    @property
    def operator_rank(self) -> float:
        """
        The operator rank indicates how imprecision exists in the type. The higher the rank, the more imprecise.
        With the highest rank being 1.0 and the smallest being 0.0.
        This ranking is used to determine the best match when wiring types by summing up the ranks and picking
        the lowest sum of the inputs as the best match.
        """
        return 1e-10

    def build_resolution_dict(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], wired_type: "HgTypeMetaData"):
        """
        Attempts to resolve any un-resolved types using the wired type supplied. Any resolutions made are added to the
        resolution_dict. This is used to:
        1. Validate that resolutions made previously for the same type-var instances are still valid.
        2. When resolution is made to a different type, determine if the types are convertible, if so pick the lowest
           conversion to bind to.
        Once all the types have had a go at determining the resolution_dict, the types are resolved for real in a second
        pass.
        The outputs are fully reliant on types to be resolved using the wired_types on the inputs to resolve the output
        types.
        """
        if self.is_resolved:
            return

        if self != wired_type:
            self.do_build_resolution_dict(resolution_dict, wired_type.dereference() if wired_type else None)

    def do_build_resolution_dict(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], wired_type: "HgTypeMetaData"):
        """
        Implementation method for build_resolution_dict - to be overriden by the derived classes
        """
        if wired_type is not None and type(self) != type(wired_type):
            from hgraph._wiring._wiring_errors import IncorrectTypeBinding
            raise IncorrectTypeBinding(self, wired_type)
