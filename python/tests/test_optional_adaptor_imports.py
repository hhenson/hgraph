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
