"""
Tests for FQPath - Fully-qualified path for time-series navigation.

This module tests the FQPath infrastructure that converts slot-based
ShortPath indices to semantic path elements:
- Field names for TSB navigation
- Indices for TSL navigation
- Actual key values for TSD navigation

FQPath is created on-demand via ShortPath.to_fq() by navigating through ViewData.
"""

from datetime import datetime

import pytest

from hgraph._feature_switch import is_feature_enabled


# Skip all tests if C++ is not enabled
pytestmark = pytest.mark.skipif(
    not is_feature_enabled("use_cpp"),
    reason="C++ runtime not enabled"
)


# Fixed test time
TEST_TIME = datetime(2024, 1, 1, 0, 0, 0)


@pytest.fixture
def hgraph_module():
    """Get the C++ hgraph module."""
    import hgraph._hgraph as _hgraph
    return _hgraph


@pytest.fixture
def value_module(hgraph_module):
    """Get the value submodule."""
    return hgraph_module.value


# ============================================================================
# TSMeta Fixtures
# ============================================================================


@pytest.fixture
def ts_int_meta(hgraph_module):
    """Create TSMeta for TS[int]."""
    from hgraph._types._ts_meta_data import HgTSTypeMetaData
    from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData

    ts_meta = HgTSTypeMetaData(HgScalarTypeMetaData.parse_type(int))
    return ts_meta.cpp_type


@pytest.fixture
def ts_float_meta(hgraph_module):
    """Create TSMeta for TS[float]."""
    from hgraph._types._ts_meta_data import HgTSTypeMetaData
    from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData

    ts_meta = HgTSTypeMetaData(HgScalarTypeMetaData.parse_type(float))
    return ts_meta.cpp_type


@pytest.fixture
def tsl_ts_int_meta(hgraph_module):
    """Create TSMeta for TSL[TS[int], Size[3]]."""
    from hgraph._types._tsl_meta_data import HgTSLTypeMetaData
    from hgraph._types._ts_meta_data import HgTSTypeMetaData
    from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData
    from hgraph._types._scalar_types import Size

    element_ts = HgTSTypeMetaData(HgScalarTypeMetaData.parse_type(int))
    size_tp = HgScalarTypeMetaData.parse_type(Size[3])
    tsl_meta = HgTSLTypeMetaData(element_ts, size_tp)
    return tsl_meta.cpp_type


@pytest.fixture
def tsb_meta(hgraph_module):
    """Create TSMeta for TSB[x: TS[int], y: TS[float]]."""
    from hgraph._types._tsb_meta_data import HgTSBTypeMetaData, HgTimeSeriesSchemaTypeMetaData
    from hgraph._types._tsb_type import TimeSeriesSchema
    from hgraph._types._ts_type import TS

    class TestSchema(TimeSeriesSchema):
        x: TS[int]
        y: TS[float]

    schema_meta = HgTimeSeriesSchemaTypeMetaData(TestSchema)
    tsb = HgTSBTypeMetaData(schema_meta)
    return tsb.cpp_type


@pytest.fixture
def tsb_nested_tsl_meta(hgraph_module):
    """Create TSMeta for TSB[items: TSL[TS[int], Size[2]], name: TS[str]]."""
    from hgraph._types._tsb_meta_data import HgTSBTypeMetaData, HgTimeSeriesSchemaTypeMetaData
    from hgraph._types._tsb_type import TimeSeriesSchema
    from hgraph._types._ts_type import TS
    from hgraph._types._tsl_type import TSL
    from hgraph._types._scalar_types import Size

    class NestedSchema(TimeSeriesSchema):
        items: TSL[TS[int], Size[2]]
        name: TS[str]

    schema_meta = HgTimeSeriesSchemaTypeMetaData(NestedSchema)
    tsb = HgTSBTypeMetaData(schema_meta)
    return tsb.cpp_type


# ============================================================================
# Section 1: FQPath Basic Tests
# ============================================================================


class TestFQPathBasics:
    """Basic tests for FQPath construction and properties."""

    def test_fqpath_default_construction(self, hgraph_module):
        """FQPath can be default-constructed."""
        FQPath = hgraph_module.FQPath

        fq = FQPath()

        assert fq.is_root()
        assert fq.depth() == 0
        assert fq.node_id() == []
        assert fq.path() == []

    def test_fqpath_str_representation(self, hgraph_module):
        """Empty FQPath has correct string representation via __str__."""
        FQPath = hgraph_module.FQPath

        fq = FQPath()

        s = str(fq)
        assert "[].out" in s  # Default port type is OUTPUT

    def test_fqpath_to_python_empty(self, hgraph_module):
        """Empty FQPath converts to Python tuple correctly."""
        FQPath = hgraph_module.FQPath

        fq = FQPath()
        py = fq.to_python()

        assert isinstance(py, tuple)
        assert len(py) == 3
        assert py[0] == []  # node_id
        assert py[1] == "OUTPUT"  # port_type string
        assert py[2] == []  # path

    def test_fqpath_port_type(self, hgraph_module):
        """FQPath port_type returns the port type."""
        FQPath = hgraph_module.FQPath
        PortType = hgraph_module.PortType

        fq = FQPath()
        assert fq.port_type() == PortType.OUTPUT


# ============================================================================
# Section 2: PathElement Tests
# ============================================================================


class TestPathElement:
    """Tests for PathElement types and operations."""

    def test_path_element_type_queries(self, hgraph_module):
        """PathElement has type query methods."""
        PathElement = hgraph_module.PathElement
        assert hasattr(PathElement, 'is_field')
        assert hasattr(PathElement, 'is_index')
        assert hasattr(PathElement, 'is_key')

    def test_path_element_accessors(self, hgraph_module):
        """PathElement has accessor methods."""
        PathElement = hgraph_module.PathElement
        assert hasattr(PathElement, 'as_field')
        assert hasattr(PathElement, 'as_index')
        assert hasattr(PathElement, 'to_python')

    def test_path_element_str_representation(self, hgraph_module):
        """PathElement has __str__ for string representation."""
        PathElement = hgraph_module.PathElement
        assert hasattr(PathElement, '__str__')


# ============================================================================
# Section 3: TSB Field Path Navigation Tests
# ============================================================================


class TestTSBFieldPaths:
    """Tests for TSB navigation path strings."""

    def test_tsb_root_path_string(self, hgraph_module, tsb_meta):
        """TSB root has path string."""
        TSOutput = hgraph_module.TSOutput

        output = TSOutput(tsb_meta, None)
        root_view = output.view(TEST_TIME)

        path_str = root_view.ts_view().short_path()
        assert isinstance(path_str, str)
        assert ".out" in path_str

    def test_tsb_field_navigation_changes_path(self, hgraph_module, tsb_meta):
        """Navigating TSB field changes the path string."""
        TSOutput = hgraph_module.TSOutput

        output = TSOutput(tsb_meta, None)
        root_view = output.view(TEST_TIME)

        root_path = root_view.ts_view().short_path()
        field_view = root_view.field("x")
        field_path = field_view.ts_view().short_path()

        # Field path should be longer/different than root path
        assert field_path != root_path
        assert len(field_path) > len(root_path)

    def test_tsb_different_fields_have_different_paths(self, hgraph_module, tsb_meta):
        """Different TSB fields produce different path strings."""
        TSOutput = hgraph_module.TSOutput

        output = TSOutput(tsb_meta, None)
        root_view = output.view(TEST_TIME)

        field_x = root_view.field("x")
        field_y = root_view.field("y")

        path_x = field_x.ts_view().short_path()
        path_y = field_y.ts_view().short_path()

        # Different fields should have different paths
        assert path_x != path_y


# ============================================================================
# Section 4: TSL Index Path Navigation Tests
# ============================================================================


class TestTSLIndexPaths:
    """Tests for TSL navigation path strings."""

    def test_tsl_element_navigation_changes_path(self, hgraph_module, tsl_ts_int_meta):
        """Navigating TSL element changes the path string."""
        TSOutput = hgraph_module.TSOutput

        output = TSOutput(tsl_ts_int_meta, None)
        root_view = output.view(TEST_TIME)

        root_path = root_view.ts_view().short_path()

        tsl_view = root_view.ts_view().as_list()
        elem_view = tsl_view.at(0)
        elem_path = elem_view.short_path()

        # Element path should be different from root
        assert elem_path != root_path

    def test_tsl_different_indices_have_different_paths(
        self, hgraph_module, tsl_ts_int_meta
    ):
        """Different TSL indices produce different path strings."""
        TSOutput = hgraph_module.TSOutput

        output = TSOutput(tsl_ts_int_meta, None)
        root_view = output.view(TEST_TIME)

        tsl_view = root_view.ts_view().as_list()
        elem_0 = tsl_view.at(0)
        elem_1 = tsl_view.at(1)

        path_0 = elem_0.short_path()
        path_1 = elem_1.short_path()

        # Different indices should have different paths
        assert path_0 != path_1


# ============================================================================
# Section 5: Nested Path Navigation Tests
# ============================================================================


class TestNestedPaths:
    """Tests for nested type navigation paths."""

    def test_tsb_with_nested_tsl_produces_nested_path(
        self, hgraph_module, tsb_nested_tsl_meta
    ):
        """TSB containing TSL produces nested path string."""
        TSOutput = hgraph_module.TSOutput

        output = TSOutput(tsb_nested_tsl_meta, None)
        root_view = output.view(TEST_TIME)

        # Navigate: TSB.items[0]
        items_view = root_view.field("items")
        tsl_view = items_view.ts_view().as_list()
        elem_view = tsl_view.at(0)

        path = elem_view.short_path()
        # Path should contain multiple segments
        assert path.count("[") >= 2  # At least two bracket segments


# ============================================================================
# Section 6: PortType Tests
# ============================================================================


class TestPortType:
    """Tests for PortType enum."""

    def test_port_type_values(self, hgraph_module):
        """PortType enum has INPUT and OUTPUT values."""
        PortType = hgraph_module.PortType

        assert hasattr(PortType, "INPUT")
        assert hasattr(PortType, "OUTPUT")

    def test_port_type_string_representation(self, hgraph_module):
        """PortType values have string representation."""
        PortType = hgraph_module.PortType

        assert "INPUT" in str(PortType.INPUT)
        assert "OUTPUT" in str(PortType.OUTPUT)

    def test_port_type_equality(self, hgraph_module):
        """PortType values can be compared for equality."""
        PortType = hgraph_module.PortType

        assert PortType.INPUT == PortType.INPUT
        assert PortType.OUTPUT == PortType.OUTPUT
        assert PortType.INPUT != PortType.OUTPUT


# ============================================================================
# Section 7: FQPath Equality and Comparison Tests
# ============================================================================


class TestFQPathEquality:
    """Tests for FQPath equality and comparison operators."""

    def test_fqpath_equality_empty(self, hgraph_module):
        """Two empty FQPaths are equal."""
        FQPath = hgraph_module.FQPath

        fq1 = FQPath()
        fq2 = FQPath()

        assert fq1 == fq2

    def test_fqpath_self_equality(self, hgraph_module):
        """FQPath equals itself."""
        FQPath = hgraph_module.FQPath

        fq = FQPath()
        assert fq == fq

    def test_fqpath_less_than_defined(self, hgraph_module):
        """FQPath has less-than comparison."""
        FQPath = hgraph_module.FQPath

        fq1 = FQPath()
        fq2 = FQPath()

        # Two equal FQPaths should not be less than each other
        assert not (fq1 < fq2)
        assert not (fq2 < fq1)


# ============================================================================
# Section 8: Integration Tests with TSOutput/TSInput
# ============================================================================


class TestFQPathIntegration:
    """Integration tests for path strings with TSOutput and TSInput."""

    def test_tsoutput_view_has_path(self, hgraph_module, ts_int_meta):
        """TSOutput view has a path string."""
        TSOutput = hgraph_module.TSOutput

        output = TSOutput(ts_int_meta, None)
        view = output.view(TEST_TIME)

        ts_view = view.ts_view()
        path = ts_view.short_path()
        assert isinstance(path, str)

    def test_tsinput_view_has_path(self, hgraph_module, ts_int_meta):
        """TSInput view has a path string."""
        TSInput = hgraph_module.TSInput

        input_ts = TSInput(ts_int_meta, None)
        view = input_ts.view(TEST_TIME)

        ts_view = view.ts_view()
        path = ts_view.short_path()
        assert isinstance(path, str)

    def test_output_fq_path_method(self, hgraph_module, ts_int_meta):
        """TSView has fq_path method returning FQPath."""
        TSOutput = hgraph_module.TSOutput
        FQPath = hgraph_module.FQPath

        output = TSOutput(ts_int_meta, None)
        view = output.view(TEST_TIME)

        ts_view = view.ts_view()
        fq = ts_view.fq_path()
        assert isinstance(fq, FQPath)


# ============================================================================
# Section 9: FQPath API Completeness Tests
# ============================================================================


class TestFQPathAPI:
    """Tests for FQPath API methods."""

    def test_fqpath_has_expected_methods(self, hgraph_module):
        """FQPath class has all expected methods."""
        FQPath = hgraph_module.FQPath

        expected_methods = [
            'node_id',
            'port_type',
            'path',
            'depth',
            'is_root',
            'to_python',
        ]

        for method in expected_methods:
            assert hasattr(FQPath, method), f"FQPath missing method: {method}"

    def test_path_element_has_expected_methods(self, hgraph_module):
        """PathElement class has all expected methods."""
        PathElement = hgraph_module.PathElement

        expected_methods = [
            'is_field',
            'is_index',
            'is_key',
            'as_field',
            'as_index',
            'to_python',
        ]

        for method in expected_methods:
            assert hasattr(PathElement, method), f"PathElement missing method: {method}"
