"""
Tests for schema dereferencing in TSTypeRegistry.

Schema dereferencing transforms REF[T] -> T recursively throughout a schema tree.
This is used by SIGNAL to bind to actual data sources rather than reference wrappers.
"""

import pytest

from hgraph._feature_switch import is_feature_enabled

# Skip all tests if C++ is not enabled
pytestmark = pytest.mark.skipif(
    not is_feature_enabled("use_cpp"),
    reason="C++ runtime not enabled"
)


@pytest.fixture
def hgraph_module():
    """Import and return the hgraph module (ensures C++ runtime is loaded)."""
    import hgraph._hgraph as _hgraph
    return _hgraph


@pytest.fixture
def registry(hgraph_module):
    """Get the TSTypeRegistry instance."""
    return hgraph_module.TSTypeRegistry.instance()


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
def tss_int_meta(hgraph_module):
    """Create TSMeta for TSS[int]."""
    from hgraph._types._tss_meta_data import HgTSSTypeMetaData
    from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData

    tss_meta = HgTSSTypeMetaData(HgScalarTypeMetaData.parse_type(int))
    return tss_meta.cpp_type


@pytest.fixture
def ref_ts_int_meta(hgraph_module, ts_int_meta):
    """Create TSMeta for REF[TS[int]]."""
    from hgraph._types._ref_meta_data import HgREFTypeMetaData
    from hgraph._types._ts_meta_data import HgTSTypeMetaData
    from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData

    inner = HgTSTypeMetaData(HgScalarTypeMetaData.parse_type(int))
    ref_meta = HgREFTypeMetaData(inner)
    return ref_meta.cpp_type


@pytest.fixture
def ref_ts_float_meta(hgraph_module, ts_float_meta):
    """Create TSMeta for REF[TS[float]]."""
    from hgraph._types._ref_meta_data import HgREFTypeMetaData
    from hgraph._types._ts_meta_data import HgTSTypeMetaData
    from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData

    inner = HgTSTypeMetaData(HgScalarTypeMetaData.parse_type(float))
    ref_meta = HgREFTypeMetaData(inner)
    return ref_meta.cpp_type


@pytest.fixture
def tsl_ts_int_meta(hgraph_module):
    """Create TSMeta for TSL[TS[int], Size[5]]."""
    from hgraph._types._tsl_meta_data import HgTSLTypeMetaData
    from hgraph._types._ts_meta_data import HgTSTypeMetaData
    from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData
    from hgraph._types._type_meta_data import HgTypeMetaData

    element_ts = HgTSTypeMetaData(HgScalarTypeMetaData.parse_type(int))
    tsl_meta = HgTSLTypeMetaData(element_ts, HgTypeMetaData.parse_type(int))
    return tsl_meta.cpp_type


@pytest.fixture
def tsl_ref_ts_int_meta(hgraph_module):
    """Create TSMeta for TSL[REF[TS[int]], Size[5]]."""
    from hgraph._types._tsl_meta_data import HgTSLTypeMetaData
    from hgraph._types._ref_meta_data import HgREFTypeMetaData
    from hgraph._types._ts_meta_data import HgTSTypeMetaData
    from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData
    from hgraph._types._type_meta_data import HgTypeMetaData

    inner = HgTSTypeMetaData(HgScalarTypeMetaData.parse_type(int))
    ref_meta = HgREFTypeMetaData(inner)
    tsl_meta = HgTSLTypeMetaData(ref_meta, HgTypeMetaData.parse_type(int))
    return tsl_meta.cpp_type


@pytest.fixture
def tsd_str_ts_float_meta(hgraph_module):
    """Create TSMeta for TSD[str, TS[float]]."""
    from hgraph._types._tsd_meta_data import HgTSDTypeMetaData
    from hgraph._types._ts_meta_data import HgTSTypeMetaData
    from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData

    key_type = HgScalarTypeMetaData.parse_type(str)
    value_ts = HgTSTypeMetaData(HgScalarTypeMetaData.parse_type(float))
    tsd_meta = HgTSDTypeMetaData(key_type, value_ts)
    return tsd_meta.cpp_type


@pytest.fixture
def tsd_str_ref_ts_float_meta(hgraph_module):
    """Create TSMeta for TSD[str, REF[TS[float]]]."""
    from hgraph._types._tsd_meta_data import HgTSDTypeMetaData
    from hgraph._types._ref_meta_data import HgREFTypeMetaData
    from hgraph._types._ts_meta_data import HgTSTypeMetaData
    from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData

    key_type = HgScalarTypeMetaData.parse_type(str)
    inner = HgTSTypeMetaData(HgScalarTypeMetaData.parse_type(float))
    ref_meta = HgREFTypeMetaData(inner)
    tsd_meta = HgTSDTypeMetaData(key_type, ref_meta)
    return tsd_meta.cpp_type


class TestContainsRef:
    """Tests for TSTypeRegistry.contains_ref()"""

    def test_ts_does_not_contain_ref(self, registry, ts_int_meta):
        """TS[T] contains no REF."""
        assert registry.contains_ref(ts_int_meta) == False

    def test_ref_contains_ref(self, registry, ref_ts_int_meta):
        """REF[TS[T]] contains REF."""
        assert registry.contains_ref(ref_ts_int_meta) == True

    def test_tss_does_not_contain_ref(self, registry, tss_int_meta):
        """TSS[T] contains no REF."""
        assert registry.contains_ref(tss_int_meta) == False

    def test_signal_does_not_contain_ref(self, registry):
        """SIGNAL contains no REF."""
        sig = registry.signal()
        assert registry.contains_ref(sig) == False

    def test_tsl_with_ts_element_does_not_contain_ref(self, registry, tsl_ts_int_meta):
        """TSL[TS[T], N] contains no REF."""
        assert registry.contains_ref(tsl_ts_int_meta) == False

    def test_tsl_with_ref_element_contains_ref(self, registry, tsl_ref_ts_int_meta):
        """TSL[REF[TS[T]], N] contains REF."""
        assert registry.contains_ref(tsl_ref_ts_int_meta) == True

    def test_tsd_with_ts_value_does_not_contain_ref(self, registry, tsd_str_ts_float_meta):
        """TSD[K, TS[V]] contains no REF."""
        assert registry.contains_ref(tsd_str_ts_float_meta) == False

    def test_tsd_with_ref_value_contains_ref(self, registry, tsd_str_ref_ts_float_meta):
        """TSD[K, REF[TS[V]]] contains REF."""
        assert registry.contains_ref(tsd_str_ref_ts_float_meta) == True


class TestDereference:
    """Tests for TSTypeRegistry.dereference()"""

    def test_dereference_ts_returns_same(self, registry, ts_int_meta):
        """Dereferencing TS[T] returns the same schema."""
        result = registry.dereference(ts_int_meta)
        assert result is ts_int_meta

    def test_dereference_ref_returns_target(self, registry, ts_int_meta, ref_ts_int_meta):
        """Dereferencing REF[TS[T]] returns TS[T]."""
        result = registry.dereference(ref_ts_int_meta)
        assert result is ts_int_meta
        assert result is not ref_ts_int_meta

    def test_dereference_tss_returns_same(self, registry, tss_int_meta):
        """Dereferencing TSS[T] returns the same schema."""
        result = registry.dereference(tss_int_meta)
        assert result is tss_int_meta

    def test_dereference_signal_returns_same(self, registry):
        """Dereferencing SIGNAL returns the same schema."""
        sig = registry.signal()
        result = registry.dereference(sig)
        assert result is sig

    def test_dereference_tsl_without_ref_returns_same(self, registry, tsl_ts_int_meta):
        """Dereferencing TSL[TS[T], N] returns the same schema."""
        result = registry.dereference(tsl_ts_int_meta)
        assert result is tsl_ts_int_meta

    def test_dereference_tsl_with_ref_element(self, registry, hgraph_module, tsl_ref_ts_int_meta, ts_int_meta):
        """Dereferencing TSL[REF[TS[T]], N] returns TSL[TS[T], N]."""
        result = registry.dereference(tsl_ref_ts_int_meta)

        # Result should be a different TSL with TS[int] element
        assert result is not tsl_ref_ts_int_meta
        assert result.kind == hgraph_module.TSKind.TSL
        assert result.element_ts is ts_int_meta

    def test_dereference_tsd_without_ref_returns_same(self, registry, tsd_str_ts_float_meta):
        """Dereferencing TSD[K, TS[V]] returns the same schema."""
        result = registry.dereference(tsd_str_ts_float_meta)
        assert result is tsd_str_ts_float_meta

    def test_dereference_tsd_with_ref_value(self, registry, hgraph_module, tsd_str_ref_ts_float_meta, ts_float_meta):
        """Dereferencing TSD[K, REF[TS[V]]] returns TSD[K, TS[V]]."""
        result = registry.dereference(tsd_str_ref_ts_float_meta)

        # Result should be a different TSD with TS[float] value
        assert result is not tsd_str_ref_ts_float_meta
        assert result.kind == hgraph_module.TSKind.TSD
        assert result.element_ts is ts_float_meta

    def test_dereference_caching(self, registry, ref_ts_int_meta):
        """Dereferencing the same schema twice returns the same result."""
        result1 = registry.dereference(ref_ts_int_meta)
        result2 = registry.dereference(ref_ts_int_meta)
        assert result1 is result2
