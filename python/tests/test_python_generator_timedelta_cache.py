"""The generator timedelta identity cache must only trust exact timedeltas.

The stable-ABI caster accepts ``timedelta`` subclasses, and a subclass can
expose ``days`` / ``seconds`` / ``microseconds`` as properties over mutable
instance state. Re-yielding such an instance after mutating it must schedule
with the new delay, so only exact ``datetime.timedelta`` objects are cached by
identity (developer guide, python_bridge.rst "Platform notes").
"""
import datetime

import hgraph as hg
from hgraph import TS, eval_node

US = datetime.timedelta(microseconds=1)


class MutableDelta(datetime.timedelta):
    """A timedelta whose fields are read from mutable instance state."""

    @property
    def days(self):
        return 0

    @property
    def seconds(self):
        return 0

    @property
    def microseconds(self):
        return self.micros


def test_generator_rereads_a_mutated_timedelta_subclass_on_each_yield():
    @hg.generator
    def ticks(_clock: hg.EvaluationClock = None) -> TS[datetime.datetime]:
        delay = MutableDelta()
        for step in (1, 2, 3):
            delay.micros = step
            yield delay, _clock.evaluation_time

    # Each value is the time the generator resumed, i.e. the previous tick:
    # delays of 1, 2 and 3 microseconds land the ticks at +1, +3 and +6.
    assert eval_node(ticks, __elide__=True) == [hg.MIN_ST, hg.MIN_ST + 1 * US, hg.MIN_ST + 3 * US]


def test_generator_identity_cache_still_serves_exact_timedeltas():
    delay = datetime.timedelta(microseconds=2)

    @hg.generator
    def ticks(_clock: hg.EvaluationClock = None) -> TS[datetime.datetime]:
        for _ in range(3):
            yield delay, _clock.evaluation_time

    assert eval_node(ticks, __elide__=True) == [hg.MIN_ST, hg.MIN_ST + 2 * US, hg.MIN_ST + 4 * US]
