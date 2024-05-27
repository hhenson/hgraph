Hgraph Library contribution
---------------------------

1. Try to be pythonic. Overload operators and do not introduce new names if there is an operator that provides correct semantics
2. Name operators that implement python operators (that are a __dunder__) with an underscore at the end.
3. Name operators that have same name (and semantic) as python keywords or functions in python standard libs (functools, itertools etc) with an underscore at the end.
4. Name operator overloads as <operator>_<type> e.g. `add_dates`, `min_int` etc
5. If an operator has additional inputs, provide reasonable defaults, e.g. if `eq_floats` takes `epsilon`, has `epsilon: float = 1e-9`
6. Put overloads for a type in _<type>_operators.py in hgraph.nodes (or in the library where the underlying type is defined)
7. Export all overloads in case direct use is required, e.g. do float comparison with different epsilon
8. Once the initial library API is agreed all changes should go via a proposal process (actual process TBD)
