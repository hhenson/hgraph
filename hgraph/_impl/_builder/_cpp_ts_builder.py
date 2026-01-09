"""
C++ Time-Series Builder Factory

This module provides a TimeSeriesBuilderFactory implementation that creates
CppTimeSeriesInputBuilder and CppTimeSeriesOutputBuilder instances. These
builders carry TSMeta which allows the Node to create TSValue storage
internally, bypassing the legacy TimeSeriesType creation path.

Usage:
    When use_cpp feature is enabled, this factory is used instead of
    PythonTimeSeriesBuilderFactory. The builders it creates provide ts_meta()
    which Node builders use to create TSValue storage.
"""

from typing import TYPE_CHECKING, cast

from hgraph._builder._ts_builder import TimeSeriesBuilderFactory

if TYPE_CHECKING:
    from hgraph._builder._input_builder import InputBuilder
    from hgraph._builder._output_builder import OutputBuilder
    from hgraph._types._time_series_meta_data import HgTimeSeriesTypeMetaData

__all__ = ("CppTimeSeriesBuilderFactory",)


class CppTimeSeriesBuilderFactory(TimeSeriesBuilderFactory):
    """
    Factory that creates C++ time-series builders with TSMeta.

    This factory converts Python HgTimeSeriesTypeMetaData to C++ TSMeta via
    the cpp_type property, then creates CppTimeSeriesInputBuilder or
    CppTimeSeriesOutputBuilder instances that carry the TSMeta.

    The Node constructor uses TSMeta to create TSValue storage internally,
    which is more efficient than the legacy shared_ptr-based approach.
    """

    def make_input_builder(self, value_tp: "HgTimeSeriesTypeMetaData") -> "InputBuilder":
        """
        Create a C++ input builder for the given type.

        Gets the C++ TSMeta from value_tp.cpp_type and creates a
        CppTimeSeriesInputBuilder that the C++ node builder can use.
        """
        import hgraph._hgraph as _hgraph

        ts_meta = value_tp.cpp_type
        if ts_meta is None:
            raise RuntimeError(
                f"CppTimeSeriesBuilderFactory: Cannot get cpp_type for {value_tp}. "
                f"Ensure the type is resolved and has C++ type support."
            )

        return _hgraph.CppTimeSeriesInputBuilder(ts_meta)

    def make_output_builder(self, value_tp: "HgTimeSeriesTypeMetaData") -> "OutputBuilder":
        """
        Create a C++ output builder for the given type.

        Gets the C++ TSMeta from value_tp.cpp_type and creates a
        CppTimeSeriesOutputBuilder that the C++ node builder can use.
        """
        import hgraph._hgraph as _hgraph

        ts_meta = value_tp.cpp_type
        if ts_meta is None:
            raise RuntimeError(
                f"CppTimeSeriesBuilderFactory: Cannot get cpp_type for {value_tp}. "
                f"Ensure the type is resolved and has C++ type support."
            )

        return _hgraph.CppTimeSeriesOutputBuilder(ts_meta)
