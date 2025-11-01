from typing import Generic, Mapping, Any, Union

from hgraph._types._scalar_types import CompoundScalar
from hgraph._types._time_series_types import TimeSeriesOutput
from hgraph._types._tsb_type import TS_SCHEMA, TimeSeriesSchema
from hgraph._types._type_meta_data import ParseError
from hgraph._types._typing_utils import nth

__all__ = ("RECORDABLE_STATE",)


class RECORDABLE_STATE(Generic[TS_SCHEMA]):
    """
    This form of state will allow for the recording and re-reinstatement of state
    within a ``component`` instance.

    The recordable state is similar to the STATE injectable. But unlike the standard STATE,
    this is a time-series that is local to the node that requests it. It is similar to
    a feed-back, but the feed-back is limited to the current node only. Additionally, there
    is no activation of the node when the time-series values are modified.
    """

    def __init__(self, __schema__: TS_SCHEMA, **kwargs):
        self.__schema__: TS_SCHEMA = __schema__
        self._ts_values: Mapping[str, TimeSeriesOutput] = {
            k: kwargs.get(k, None) for k in self.__schema__.__meta_data_schema__.keys()
        }  # Initialise the values to None or kwargs provided

    def __class_getitem__(cls, item) -> Any:
        # For now limit to validation of item
        if item is not TS_SCHEMA:
            from hgraph._types._type_meta_data import HgTypeMetaData

            if HgTypeMetaData.parse_type(item).is_scalar:
                if isinstance(item, type) and issubclass(item, CompoundScalar):
                    item = TimeSeriesSchema.from_scalar_schema(item)
                else:
                    raise ParseError(
                        f"Type '{item}' must be a TimeSeriesSchema or a valid TypeVar (bound to TimeSeriesSchema)"
                    )

        out = super(RECORDABLE_STATE, cls).__class_getitem__(item)
        return out

    @property
    def as_schema(self) -> TS_SCHEMA:
        """
        Exposes the RECORDABLE_STATE as the schema type. This is useful for type completion in tools such as PyCharm / VSCode.
        It is a convenience method, it is possible to access the properties of the schema directly from the TSB
        instances as well.
        """
        return self

    def __getattr__(self, item) -> TimeSeriesOutput:
        """
        The time-series value for the property associated to item in the schema
        :param item:
        :return:
        """
        ts_values = self.__dict__.get("_ts_values")
        if item == "_ts_values":
            if ts_values is None:
                raise AttributeError(item)
            return ts_values
        if ts_values and item in ts_values:
            return ts_values[item]
        else:
            return super().__getattribute__(item)

    def __getitem__(self, item: Union[int, str]) -> "TimeSeriesOutput":
        """
        If item is of type int, will return the item defined by the sequence of the schema. If it is a str, then
        the item as named.
        """
        if type(item) is int:
            return self._ts_values[nth(iter(self.__schema__.__meta_data_schema__), item)]
        else:
            return self._ts_values[item]
