from typing import TypeVar, Type, Optional

__all__ = ('ParseError', 'HgTypeMetaData', 'WiringError', 'IncorrectTypeBinding')


class ParseError(RuntimeError):
    ...


class WiringError(RuntimeError):
    ...


class IncorrectTypeBinding(WiringError):

    def __init__(self, expected_type: "HgTypeMetaData", actual_type: "HgTypeMetaData",
                 arg: str = None,  *args, **kwargs):
        if arg is None:
            super().__init__(f"Type '{str(expected_type)}' is not the same as the wired type '{str(actual_type)}'", *args,
                         **kwargs)
        else:
            super().__init__(f"{arg}: {str(expected_type)} <- {str(actual_type)} is not type compatible")
        self.expected_type = expected_type  # The type expected by the signature
        self.actual_type = actual_type  # The actual type wired in
        self.arg = arg  # The argument name that was being wired


class HgTypeMetaData:
    is_resolved: bool  # Does this instance of metadata contain a generic entry, i.e. requires resolution
    is_scalar: bool
    is_atomic: bool = False
    is_generic: bool = False  # Is this instance of metadata representing a template type (i.e. TypeVar)
    is_injectable: bool = False  # This indicates the type represent an injectable property (such as ExecutionContext)
    py_type: Type  # The python type that represents this type

    @classmethod
    def parse(cls, value) -> Optional["HgTypeMetaData"]:
        from hg._types._scalar_type_meta_data import HgScalarTypeMetaData
        from hg._types._time_series_meta_data import HgTimeSeriesTypeMetaData
        parse_order = [HgScalarTypeMetaData, HgTimeSeriesTypeMetaData]
        for parser in parse_order:
            if meta_data := parser.parse(value):
                return meta_data
        raise ParseError(f"Unable to parse '{value}'")

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

    def resolve(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"]) -> "HgTypeMetaData":
        """
        Return a resolve type instance using the resolution dictionary supplied to map type var instances
        to resolved types.
        If there are missing types an appropriate exception should be thrown.
        """
        if self.is_resolved:
            return self

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
        if wired_type is not None and type(self) != type(wired_type):
            raise ParseError(f"The input type '{type(self)}' "
                             f"does not match the supplied wiring type '{type(wired_type)}'")
