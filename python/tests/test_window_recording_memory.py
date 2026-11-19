"""Sparse in-memory window replay across two graph runs."""

from datetime import timedelta

import pytest

import hgraph as hg


Window = hg.TSW[int, hg.WindowSize[2 * hg.MIN_TD], hg.WindowSize[timedelta(0)]]


class Readings(hg.TimeSeriesSchema):
    window: Window
    sample: hg.TS[int]


@hg.graph
def record_samples(samples: hg.TS[int]):
    window = hg.to_window(samples, 2 * hg.MIN_TD, timedelta(0))
    hg.record(window, "samples", recordable_id="readings")
    hg.record(hg.TSB[Readings].from_ts(window=window, sample=samples), "bundle", recordable_id="readings")


@pytest.mark.parametrize("start", [2, 3, 4, 5])
@pytest.mark.parametrize("nested", [False, True], ids=["window", "bundle_field"])
def test_partial_replay_silently_restores_the_samples_needed_for_departures(start, nested):
    seen = []

    @hg.compute_node(valid=())
    def observe(window: Window) -> hg.TS[int]:
        held = list(zip(window.value, window.value_times))
        removed = [(sample["value"], sample["time"]) for sample in window.window_delta["removed"]]
        seen.append((window.last_modified_time, held, removed))
        return len(held)

    @hg.graph
    def replay() -> hg.TS[int]:
        if nested:
            return observe(hg.replay("bundle", hg.TSB[Readings], recordable_id="readings").window)
        return observe(hg.replay("samples", Window, recordable_id="readings"))

    t0, step = hg.MIN_ST, hg.MIN_TD
    with hg.GlobalState():
        hg.set_record_replay_config(hg.IN_MEMORY)
        hg.eval_node(record_samples, [7, 2])
        hg.eval_node(replay, __start_time__=t0 + start * step, __end_time__=t0 + 6 * step)

    # At 2 both samples are held quietly. Each subsequent departure is
    # visible once, including when it falls exactly on the replay start.
    departures = [
        (t0 + 3 * step, [(2, t0 + step)], [(7, t0)]),
        (t0 + 4 * step, [], [(2, t0 + step)]),
    ]
    assert seen == [event for event in departures if event[0] >= t0 + start * step]


@pytest.mark.parametrize("live", [False, True])
def test_recovering_component_keeps_original_sample_times_and_prefers_a_live_restart_tick(live):
    seen = []

    @hg.compute_node
    def inspect_window(window: Window) -> hg.TS[int]:
        seen.append((list(zip(window.value, window.value_times)), window.window_delta))
        return len(window.value)

    @hg.component
    def strategy(window: Window) -> hg.TS[int]:
        return inspect_window(window)

    @hg.graph
    def run(samples: hg.TS[int]) -> hg.TS[int]:
        return strategy(hg.to_window(samples, 2 * hg.MIN_TD, timedelta(0)))

    t0, step = hg.MIN_ST, hg.MIN_TD
    restart = t0 + 2 * step
    with hg.GlobalState():
        hg.set_record_replay_config(hg.IN_MEMORY)
        with hg.RecordReplayContext(mode=hg.RecordReplayEnum.RECORD):
            hg.eval_node(run, [7, 2, 4], __end_time__=restart + step)
        seen.clear()
        with hg.RecordReplayContext(mode=hg.RecordReplayEnum.RECOVER):
            hg.eval_node(run, [9] if live else [], __start_time__=restart, __end_time__=restart + step)

    (held, delta), = seen
    assert held == [(7, t0), (2, t0 + step), (9 if live else 4, restart)]
    if live:
        assert list(delta["added"]) == [9]
        assert "replacement" not in delta
    else:
        assert [(s["value"], s["time"]) for s in delta["replacement"]] == held
