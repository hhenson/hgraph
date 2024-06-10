import pytest

from hgraph.nodes import center_of_mass_to_alpha, span_to_alpha
from hgraph.nodes._stream_analytical_operators import clip
from hgraph.test import eval_node


def test_conversions():
    assert center_of_mass_to_alpha(1.0) == 0.5
    assert span_to_alpha(1.0) == 1.0


@pytest.mark.parametrize(
    ["values", "min", "max", "expected"],
    [
        [[2, 1, 0, -1, -2], -1, 1, [1, 1, 0, -1, -1]],
        [[2.0, 1.0, 0.0, -1.0, -2.0], -1.0, 1.0, [1.0, 1.0, 0.0, -1.0, -1.0]],
    ]
)
def test_clip(values, min, max, expected):
    assert eval_node(clip, values, min, max) == expected


def test_clip_failure():
    with pytest.raises(RuntimeError):
        assert eval_node(clip, [1.0], 1.0, -1.0) == [1.0]


