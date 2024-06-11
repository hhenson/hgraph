from typing import Type

from hgraph import compute_node, convert, SCALAR, TS, OUT, TSS, TSS_OUT, PythonSetDelta, DEFAULT

_all__ = ()


@compute_node(overloads=convert,
              requires=lambda m, s: m[OUT].py_type is TSS or
                                    m[OUT].matches_type(TSS[m[SCALAR].py_type]),
              )
def convert_ts_to_tss(ts: TS[SCALAR], to: Type[OUT] = DEFAULT[OUT], _output: TSS_OUT[SCALAR] = None) -> TSS[SCALAR]:
    return PythonSetDelta({ts.value}, _output.value if _output.valid else set())
