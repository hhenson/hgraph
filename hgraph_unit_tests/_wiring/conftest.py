"""pytest configuration for _wiring tests.

Clears the REF output cache between tests to prevent test isolation issues.
The cache uses TSValue pointers as keys, which can be reused across tests.
"""
import pytest

@pytest.fixture(autouse=True)
def clear_ref_cache():
    """Clear the REF output cache before each test."""
    from hgraph._hgraph import _clear_ref_output_cache
    _clear_ref_output_cache()
    yield
    # Also clear after test to ensure clean state
    _clear_ref_output_cache()
