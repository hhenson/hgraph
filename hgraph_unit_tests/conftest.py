from pathlib import Path

import pytest

from hgraph._feature_switch import is_feature_enabled


_CPP_ONLY_TEST_FILES = frozenset(
    {
        "test_cpp_static_node_export.py",
        "test_cpp_v2_node_overrides.py",
        "test_cpp_v2_python_node_builder.py",
        "test_cpp_native_compound_scalar.py",
        "test_cpp_type_integration.py",
        "test_ts_cpp_type.py",
    }
)


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    del config

    if is_feature_enabled("use_cpp"):
        return

    skip_cpp = pytest.mark.skip(reason="C++-only test when HGRAPH_USE_CPP=0")
    for item in items:
        if Path(str(item.fspath)).name in _CPP_ONLY_TEST_FILES:
            item.add_marker(skip_cpp)
