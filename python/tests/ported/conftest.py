"""Ported hgraph test suite (from ext/main/hgraph_unit_tests).

Conventions:
- Files are near-verbatim copies; only genuinely unsupported pieces are
  marked with pytest.mark.skip carrying the deviation/gap reason.
- Agreed deviations (REF is value-only; TSD internals; ...) skip with
  reason "deviation: ...". Unimplemented surface skips with "gap: ...".
"""


def pytest_configure(config):
    config.addinivalue_line("markers", "smoke: core operator compatibility coverage")
    config.addinivalue_line(
        "markers", "wip: newly ported file, excluded from the compatibility gate")
