"""Every persistence compatibility shim warns, and says when it goes.

RFC 0025's deprecation policy: the compatibility layer is retained in full
through the pre-1.0 bridge release and removed at 1.0 with the
``hgraph.adaptors`` package (RFC 0005). A silent translation is not a
deprecation -- before this, one shim of seven warned and the rest forwarded
quietly, some under comments describing a "deprecation window" that emitted
nothing.

Each test asserts the warning at the point of USE. A structural check that a
shim still exists cannot tell a warning from a silent forward, which is how
the one warning that did exist came to name the wrong release.
"""

import importlib
import sys

import pytest

import hgraph as hg


REMOVAL = "1.0"


def _uncached(module_name, *names):
    """Drop __getattr__ caches so the first-access warning fires again."""
    modules = [sys.modules.get(module_name)]
    if module_name.endswith("._data_frame_record_replay"):
        modules.append(sys.modules.get(module_name.rsplit(".", 1)[0]))
    for module in modules:
        if module is None:
            continue
        for name in names:
            module.__dict__.pop(name, None)


@pytest.mark.parametrize("name", ["frame_store_contains", "frame_store_read"])
def test_core_frame_store_helpers_warn(name):
    with pytest.warns(DeprecationWarning, match=rf"hgraph\.{name}.*removed in {REMOVAL}"):
        try:
            getattr(hg, name)("some/key")
        except ModuleNotFoundError as error:
            # Core-only install: the warning precedes the pointed install error.
            assert error.name == "hgraph_persistence"


@pytest.mark.parametrize(
    "name",
    [
        "WriteMode",
        "DataFrameStorage",
        "BaseDataFrameStorage",
        "FileBasedDataFrameStorage",
        "MemoryDataFrameStorage",
        "set_data_frame_record_path",
        "set_data_frame_overrides",
    ],
)
def test_adaptor_storage_surface_warns(name):
    module_name = "hgraph.adaptors.data_frame._data_frame_record_replay"
    module = importlib.import_module(module_name)
    _uncached(module_name, name)
    expected = rf"hgraph\.adaptors\.data_frame\.{name}.*hgraph_persistence\.compat\.{name}.*removed in {REMOVAL}"
    with pytest.warns(DeprecationWarning, match=expected):
        try:
            getattr(module, name)
        except ModuleNotFoundError as error:
            assert error.name == "hgraph_persistence"


def test_module_only_storage_names_warn():
    # These three were never re-exported at package level; they are reachable
    # only through the private module, and are shims all the same.
    module_name = "hgraph.adaptors.data_frame._data_frame_record_replay"
    module = importlib.import_module(module_name)
    for name in (
        "get_data_frame_record_overrides",
        "DATA_FRAME_RECORD_REPLAY_PATH",
        "DATA_FRAME_RECORD_OVERRIDES",
    ):
        _uncached(module_name, name)
        with pytest.warns(DeprecationWarning, match=rf"removed in {REMOVAL}"):
            try:
                getattr(module, name)
            except ModuleNotFoundError as error:
                assert error.name == "hgraph_persistence"


def test_backend_sentinel_warns_and_points_at_the_backend_id():
    # Its replacement is NOT in hgraph_persistence.compat, which does not
    # define it; a warning naming compat would send the reader to an
    # AttributeError.
    module_name = "hgraph.adaptors.data_frame._data_frame_record_replay"
    module = importlib.import_module(module_name)
    _uncached(module_name, "DATA_FRAME_RECORD_REPLAY")
    with pytest.warns(
        DeprecationWarning,
        match=rf"DATA_FRAME_RECORD_REPLAY.*FRAME_BACKEND.*removed in {REMOVAL}",
    ):
        value = getattr(module, "DATA_FRAME_RECORD_REPLAY")
    assert value == ":data_frame:__data_frame_record_replay__"


@pytest.mark.parametrize(
    ("legacy", "backend"), [("InMemory", "memory"), ("InMemoryDense", "testing")]
)
def test_legacy_model_constants_warn_and_still_translate(legacy, backend):
    with hg.GlobalState() as state:
        with pytest.warns(
            DeprecationWarning, match=rf"{legacy}.*{backend}.*removed in {REMOVAL}"
        ):
            hg.set_record_replay_config(legacy)
        # The translation still happens: deprecating it must not break it.
        assert state["__record_replay_model__"] == backend


def test_set_record_replay_model_alias_warns():
    with hg.GlobalState():
        with pytest.warns(
            DeprecationWarning,
            match=rf"set_record_replay_model.*set_record_replay_config.*removed in {REMOVAL}",
        ):
            hg.set_record_replay_model("memory")


def test_the_removal_release_has_exactly_one_definition():
    # The release was inlined in a message once and went stale, telling users
    # 0.9 after the policy settled on 1.0. Keep one source of truth.
    from hgraph import _deprecation

    assert _deprecation.PERSISTENCE_REMOVAL_RELEASE == REMOVAL
    for message in (
        _deprecation.moved_to_persistence("a", "b"),
        _deprecation.deprecated_compat_name("a", "b"),
    ):
        assert REMOVAL in message
