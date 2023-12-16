from datetime import timedelta, datetime

from hgraph import TS, SCALAR, TSB, TimeSeriesSchema, compute_node, STATE

__all__ = ("buffer",)


class BufferResult(TimeSeriesSchema):
    buffer: TS[tuple[SCALAR, ...]]
    index: TS[tuple[datetime, ...]]


def buffer(ts: TS[SCALAR], window: timedelta | int, wait_till_full: bool = True) -> BufferResult:
    """
    Buffers the time-series. Emits a tuple of values representing the elements in the buffer.
    and a tuple of corresponding time-stamps representing the time-points at which the elements
    in the buffer correspond.

    When the window is an int, a cyclic buffer is created, if the window is a timedelta, then
    a deque is used to buffer the elements.
    """
    @compute_node
    def cyclic_buffer(ts: TS[SCALAR], window: int, wait_till_full: bool, state: STATE = None) -> BufferResult:
        from collections import deque
        buffer: deque[SCALAR] = state.buffer
        index: deque[datetime] = state.index
        buffer.append(ts.value)
        index.append(ts.last_modified_time)
        if not wait_till_full or len(buffer) == window:
            return {'buffer': tuple(buffer), 'index': tuple(index)}

    @cyclic_buffer.start
    def cyclic_buffer_start(window: int, state: STATE) -> BufferResult:
        from collections import deque
        state.buffer = deque[SCALAR](maxlen=window)
        state.index = deque[datetime](maxlen=window)