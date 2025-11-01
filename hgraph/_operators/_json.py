from typing import Type, Union

from hgraph._types import TS, SCALAR, DEFAULT
from hgraph._wiring._decorators import operator


class JSON:
    def __init__(self, j):
        self.json = j

    json: Union[bool, float, str, dict, list] = None


@operator
def json_decode(ts: TS[SCALAR]) -> TS[JSON]:
    pass


@operator
def json_encode(ts: TS[JSON], _tp: Type[SCALAR] = DEFAULT[SCALAR]) -> TS[SCALAR]:
    pass
