"""Runtime checks for optional adaptor dependency boundaries."""

import os
from pathlib import Path
import subprocess
import sys
import textwrap


ROOT = Path(__file__).resolve().parents[2]


def test_optional_adaptor_namespaces_import_without_client_dependencies():
    script = textwrap.dedent(
        """
        import sys

        blocked = {
            "adbc_driver_snowflake", "boto3", "connectorx", "deltalake",
            "duckdb", "pandas", "polars", "sqlalchemy",
        }

        class BlockOptionalImports:
            def find_spec(self, fullname, path=None, target=None):
                if fullname.partition(".")[0] in blocked:
                    raise ModuleNotFoundError(
                        f"blocked optional dependency: {fullname}",
                        name=fullname,
                    )
                return None

        sys.meta_path.insert(0, BlockOptionalImports())

        import hgraph.adaptors.delta
        import hgraph.adaptors.sql

        from hgraph.adaptors.delta.delta_adaptor_raw import (
            _require_boto3,
            _require_delta_lake,
            _require_polars as require_delta_polars,
        )
        from hgraph.adaptors.sql.sql_connection import (
            _require_polars as require_sql_polars,
            _require_snowflake,
            _require_sqlalchemy,
        )

        helpers = (
            (_require_boto3, "delta"),
            (_require_delta_lake, "delta"),
            (require_delta_polars, "delta"),
            (require_sql_polars, "sql"),
            (_require_sqlalchemy, "sql"),
            (_require_snowflake, "snowflake"),
        )
        for helper, extra in helpers:
            try:
                helper()
            except RuntimeError as error:
                assert f"'{extra}' extra" in str(error), str(error)
            else:
                raise AssertionError(f"{helper.__name__} imported a blocked client")
        """
    )
    python_path = os.pathsep.join(
        filter(None, (str(ROOT / "python"), os.environ.get("PYTHONPATH")))
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=ROOT,
        env={**os.environ, "PYTHONPATH": python_path},
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr


def test_legacy_kafka_shim_does_not_load_an_absent_extension():
    script = textwrap.dedent(
        """
        import sys

        class BlockKafkaExtension:
            def find_spec(self, fullname, path=None, target=None):
                if fullname.partition(".")[0] == "hgraph_kafka":
                    raise ModuleNotFoundError(
                        f"blocked optional extension: {fullname}",
                        name="hgraph_kafka",
                    )
                return None

        sys.meta_path.insert(0, BlockKafkaExtension())

        import hgraph
        import hgraph.adaptors

        assert "hgraph_kafka" not in sys.modules
        try:
            import hgraph.adaptors.kafka
        except ModuleNotFoundError as error:
            assert error.name == "hgraph_kafka"
            assert "pip install hgraph-kafka" in str(error)
        else:
            raise AssertionError("the legacy shim imported without hgraph-kafka")
        """
    )
    python_path = os.pathsep.join(
        filter(None, (str(ROOT / "python"), os.environ.get("PYTHONPATH")))
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=ROOT,
        env={**os.environ, "PYTHONPATH": python_path},
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr


def test_per_call_durable_model_names_the_missing_persistence_extension():
    # A per-call ``model=`` selecting a durable backend is an extension load
    # point (RFC 0025). Without hgraph-persistence the wiring call must raise
    # the pointed install error, not an unexplained overload-resolution
    # failure. The blocker keeps the assertion meaningful in environments
    # where the extension IS installed (the wheel-test legs).
    script = textwrap.dedent(
        """
        import sys

        # Editable-install finders would shadow PYTHONPATH with a different
        # checkout's package; drop them first (a no-op in CI).
        sys.meta_path = [
            finder for finder in sys.meta_path
            if "ScikitBuild" not in type(finder).__name__
        ]

        class BlockPersistenceExtension:
            def find_spec(self, fullname, path=None, target=None):
                if fullname.partition(".")[0] == "hgraph_persistence":
                    raise ModuleNotFoundError(
                        f"blocked optional extension: {fullname}",
                        name="hgraph_persistence",
                    )
                return None

        sys.meta_path.insert(0, BlockPersistenceExtension())

        import hgraph as hg
        from hgraph import TS

        assert "hgraph_persistence" not in sys.modules
        try:
            hg.record[TS[int]](ts=None, key="ts", model=hg.DATA_FRAME)
        except ModuleNotFoundError as error:
            assert error.name == "hgraph_persistence"
            assert "pip install hgraph-persistence" in str(error)
        else:
            raise AssertionError("per-call durable model wired without hgraph-persistence")

        # Wiring failures on the durable operators also diagnose the missing
        # distribution: replay_const has no in-memory implementation, so its
        # resolution failure names the install, not just "no operator".
        from hgraph import GlobalState
        from hgraph.test import eval_node

        @hg.graph
        def use_replay_const() -> TS[int]:
            return hg.replay_const[TS[int]](key="price")

        try:
            with GlobalState():
                eval_node(use_replay_const)
        except Exception as error:
            assert "pip install hgraph-persistence" in str(error), str(error)
        else:
            raise AssertionError("replay_const wired without hgraph-persistence")
        """
    )
    python_path = os.pathsep.join(
        filter(None, (str(ROOT / "python"), os.environ.get("PYTHONPATH")))
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=ROOT,
        env={**os.environ, "PYTHONPATH": python_path},
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stdout + result.stderr
