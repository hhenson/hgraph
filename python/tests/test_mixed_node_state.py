"""Local cache and durable state share the native node lifecycle."""
import gc
import weakref

import hgraph as hg
import pytest


class Durable(hg.TimeSeriesSchema):
    total: hg.TS[int]


class Cache:
    total = None


@pytest.mark.parametrize("typed", [False, True])
@pytest.mark.parametrize("failure", [None, "start", "eval", "stop"])
def test_mixed_cache_lifecycle_and_cleanup(typed, failure):
    cache_type = hg.STATE[Cache] if typed else hg.STATE
    refs = []
    stops = []

    @hg.compute_node
    def counter(ts: hg.TS[int], cache: cache_type = None,
                state: hg.RECORDABLE_STATE[Durable] = None) -> hg.TS[int]:
        assert cache.total == state.total.value
        if failure == "eval":
            raise RuntimeError("mixed eval failure")
        cache.total += ts.value
        state.total.value = cache.total
        return cache.total

    @counter.start
    def start(cache: cache_type = None, state: hg.RECORDABLE_STATE[Durable] = None):
        assert not state.total.valid
        assert getattr(cache, "total", None) is None
        # Untyped namespaces are not weak-referenceable; attach a probe to both.
        cache.probe = Cache()
        refs.append(weakref.ref(cache.probe))
        state.total.value = 0
        cache.total = state.total.value
        if failure == "start":
            raise RuntimeError("mixed start failure")

    @counter.stop
    def stop(cache: cache_type = None, state: hg.RECORDABLE_STATE[Durable] = None):
        assert cache.total == state.total.value
        stops.append(cache.total)
        if failure == "stop":
            raise RuntimeError("mixed stop failure")

    if failure:
        with pytest.raises(RuntimeError, match=f"mixed {failure} failure"):
            hg.eval_node(counter, [1, 2])
    else:
        assert hg.eval_node(counter, [1, 2]) == [1, 3]
        assert hg.eval_node(counter, [4]) == [4]
        assert stops == [3, 4]
    gc.collect()
    assert refs and all(ref() is None for ref in refs)
    if failure == "start":
        assert not stops


def test_mixed_cache_is_released_without_a_stop_hook():
    refs = []

    @hg.compute_node
    def counter(ts: hg.TS[int], cache: hg.STATE = None,
                state: hg.RECORDABLE_STATE[Durable] = None) -> hg.TS[int]:
        cache.probe = Cache()
        refs.append(weakref.ref(cache.probe))
        state.total.value = ts.value
        return state.total.value

    assert hg.eval_node(counter, [1]) == [1]
    gc.collect()
    assert refs[0]() is None
