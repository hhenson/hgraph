"""Run one strategy day per process, restoring the previous completed day.

Run with a new directory:
    python completed_days.py /tmp/strategy-checkpoints 1
    python completed_days.py /tmp/strategy-checkpoints 2

Day one produces totals 1, 3; day two restores 3 and produces 6, 10.
"""

from datetime import datetime, timedelta

import hgraph as hg
import hgraph_persistence as persistence


class RunningTotalState(hg.TimeSeriesSchema):
    total: hg.TS[int]


@hg.compute_node
def running_total(value: hg.TS[int],
                  state: hg.RECORDABLE_STATE[RunningTotalState] = None) -> hg.TS[int]:
    total = (state.total.value if state.total.valid else 0) + value.value
    state.total.value = total
    return total


@hg.component(recordable_id="daily-strategy")
def strategy(value: hg.TS[int]) -> hg.TS[int]:
    return running_total(value, __recordable_id__="total")


@hg.generator
def observations(start: datetime, day: int) -> hg.TS[int]:
    yield start + timedelta(hours=9), day * 2 - 1
    yield start + timedelta(hours=16), day * 2


@hg.graph
def application(start: datetime, day: int) -> hg.TS[int]:
    return strategy(observations(start, day))


def run_day(directory, day):
    if day < 1:
        raise ValueError("day must be positive")
    start = datetime(2026, 1, 1) + timedelta(days=day - 1)
    store = persistence.ComponentCheckpointStore(directory)
    with hg.GlobalState() as state:
        persistence.configure_component_recovery(
            store, "daily-strategy", f"day-{day}",
            f"day-{day - 1}" if day > 1 else None,
            revision="example-v1", global_state=state)
        return hg.run_graph(application, start, day, start_time=start,
                            end_time=start + timedelta(days=1))


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory")
    parser.add_argument("day", type=int)
    options = parser.parse_args()
    for timestamp, total in run_day(options.directory, options.day):
        print(f"{timestamp}: {total}")
