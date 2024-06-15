Hgraph Library contribution
---------------------------

1. Try to be pythonic. Overload operators and do not introduce new names if there is an operator that provides correct semantics
2. Name operators that implement python operators (that are a `__dunder__`) with an underscore at the end.
3. Name operators that have same name (and semantic) as python keywords or functions in python standard libs (functools, itertools etc) with an underscore at the end.
4. Name operator overloads as `\<operator>_<type>` e.g. `add_dates`, `min_int` etc
5. If an operator has additional inputs, provide reasonable defaults, e.g. if `eq_floats` takes `epsilon`, has `epsilon: float = 1e-9`
6. Put overloads for a type in `_\<type>_operators.py` in hgraph.nodes (or in the library where the underlying type is defined)
7. Operator overloads do not need to be exported unless there is a specific exception
8. Once the initial library API is agreed all changes should go via a proposal process (actual process TBD)
9. Default to stricter validity requirements in operators, eg all_valid for bundles and reuiring all inputs to be valid for sum_ etc
10. Provide `__strict__` scalar arg to operators to allow for more lenient validity requirements when passed `False`
11. Prefix all node parameters that are injected by the engine with an underscore, e.g. `_state: STATE` and `_sched: SCHEDULER`.
12. Format code with [black](https://black.readthedocs.io/en/stable/).
13. Parameters injected by the engine should be the last in the parameter list.