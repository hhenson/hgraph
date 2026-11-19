"""Versioned JSON window events and the historical inputs accepted by replay."""

import json
from datetime import timedelta

import pytest

import hgraph as hg


Window = hg.TSW[int, hg.WindowSize[2 * hg.MIN_TD], hg.WindowSize[timedelta(0)]]


@hg.graph
def encode(samples: hg.TS[int], reset: hg.SIGNAL) -> hg.TS[str]:
    window = hg.to_window(samples, 2 * hg.MIN_TD, timedelta(0), reset)
    return hg.to_json(window, delta=True)


def read_history(records):
    seen = []

    @hg.compute_node
    def observe(window: Window) -> hg.TS[int]:
        seen.append((window.last_modified_time, list(zip(window.value, window.value_times)), window.window_delta))
        return len(window.value)

    @hg.graph
    def replay(text: hg.TS[str]) -> hg.TS[int]:
        return observe(hg.from_json[Window](text))

    hg.eval_node(replay, records)
    return seen


def test_versioned_json_replays_the_arrival_departure_and_empty_clear(tmp_path):
    # The 7 at offset 0 leaves just as 8 arrives. The final reset observes
    # the already-empty window. Each is one publication, at its original time.
    records = hg.eval_node(encode, [7, None, None, 8], [None] * 7 + [True])
    t0 = hg.MIN_ST
    t3 = t0 + 3 * hg.MIN_TD
    assert [json.loads(row)["$hgraph.window"] for row in records if row] == [1] * 4
    # Close the file before starting a second graph. Store the sparse publication
    # offsets separately from the arrival timestamps carried inside each delta.
    log = tmp_path / "events.jsonl"
    log.write_text("".join(json.dumps([t, row]) + "\n" for t, row in enumerate(records) if row))
    restored = [None] * len(records)
    for line in log.read_text().splitlines():
        offset, restored_row = json.loads(line)
        restored[offset] = restored_row
    events = read_history(restored)
    assert [time for time, _, _ in events] == [t0, t3, t3 + 3 * hg.MIN_TD, t3 + 4 * hg.MIN_TD]
    assert [history for _, history, _ in events] == [[(7, t0)], [(8, t3)], [], []]
    assert events[1][2]["removed"] == [{"value": 7, "time": t0}]
    assert events[2][2]["removed"] == [{"value": 8, "time": t3}]
    assert events[3][2]["cleared"]


def test_legacy_json_rows_keep_their_silence_and_snapshot_meaning():
    # Historical lists described the entire held window at each recorded row.
    # A bare scalar describes an arrival, whose prune-on-push uses the row time.
    events = read_history(["[4, 5]", None, None, None, None, "9"])
    assert [history for _, history, _ in events] == [
        [(4, hg.MIN_ST), (5, hg.MIN_ST)], [(9, hg.MIN_ST + 5 * hg.MIN_TD)],
    ]
    assert events[1][2]["removed"] == [
        {"value": 4, "time": hg.MIN_ST}, {"value": 5, "time": hg.MIN_ST},
    ]


def test_json_window_record_rejects_an_unknown_version_even_with_the_event_first():
    with pytest.raises(Exception, match="unsupported window recording version"):
        read_history(['{"event": "future encoding", "$hgraph.window": 2}'])


def test_recorded_replacements_preserve_duplicate_sample_times_and_empty_history():
    def replacement(samples):
        return json.dumps({"$hgraph.window": 1, "event": {
            "cleared": False, "added": [], "removed": [], "replacement": samples,
        }})

    t0, t1 = hg.MIN_ST, hg.MIN_ST + hg.MIN_TD
    samples = [{"value": 4, "time": t.isoformat() + "Z"} for t in (t0, t0, t1)]
    events = read_history([None, replacement(samples), None, None, None, replacement([])])
    assert [history for _, history, _ in events] == [[(4, t0), (4, t0), (4, t1)], []]
    assert [time for time, _, _ in events] == [t1, t0 + 5 * hg.MIN_TD]
    assert events[-1][2]["replacement"] == []


def test_historical_list_patch_replays_window_arrivals_nested_in_bundles():
    class Quote(hg.TimeSeriesSchema):
        w: Window
        side: hg.TS[int]

    Positions = hg.TSL[hg.TSB[Quote], hg.Size[2]]

    @hg.graph
    def transcode(rows: hg.TS[str]) -> hg.TS[str]:
        return hg.to_json(hg.from_json[Positions](rows), delta=True)

    # A released writer serialized a window as a scalar arrival inside the
    # positional delta. The later writer put a structured event at that path.
    old = '{"removed":[],"modified":{"0":{"w":7,"side":7},"1":{"w":7,"side":7}}}'
    newer = '{"modified":{"0":{"w":{"cleared":false,"added":[8],"removed":[]}}}}'
    out = hg.eval_node(transcode, [old, newer])
    first, second = [json.loads(event)["event"]["modified"] for event in out]
    assert first == {str(i): {"w": {"cleared": False, "added": [7], "removed": []}, "side": 7}
                     for i in (0, 1)}
    assert second == {"0": {"w": {"cleared": False, "added": [8], "removed": []}}}


def test_numeric_dictionary_key_matching_the_marker_remains_user_data():
    @hg.graph
    def transcode(rows: hg.TS[str]) -> hg.TS[str]:
        return hg.to_json(hg.from_json[hg.TSD[str, Window]](rows), delta=True)

    events = hg.eval_node(transcode, ['{"$hgraph.window":7}'])
    change = json.loads(events[0])["event"]["modified"]["$hgraph.window"]
    assert list(change["added"]) == [7]


def test_old_compound_arrival_inside_a_list_preserves_quote_fields():
    class Quote(hg.CompoundScalar):
        bid: int
        ask: int

    QuoteWindow = hg.TSW[Quote, hg.WindowSize[2], hg.WindowSize[1]]
    Positions = hg.TSL[QuoteWindow, hg.Size[1]]

    @hg.graph
    def transcode(rows: hg.TS[str]) -> hg.TS[str]:
        return hg.to_json(hg.from_json[Positions](rows), delta=True)

    old = '{"removed":[],"modified":{"0":{"bid":7,"ask":8}}}'
    out = hg.eval_node(transcode, [old])
    assert json.loads(out[0])["event"]["modified"]["0"] == {
        "cleared": False, "added": [{"bid": 7, "ask": 8}], "removed": [],
    }
