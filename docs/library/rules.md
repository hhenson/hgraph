Hgraph Library contribution
---------------------------

1. Try to be pythonic. Overload operators and do not introduce new names if there is an operator that provides correct semantics
2. Put overloads for a type in _<type>_operators.py in hgraph.nodes (or in the library where the underlying type is defined)
3. Name operator overloads as <operator>_<type> e.g. `add_dates`, `min_int` etc
4. If an operator has addition inputs, provide reasonable defaults, e.g. if `eq_floats` takes `epsilon`, has `epsilon: float = 1e-9`
5. Export all overloads in case direct use is required, e.g. do float comparison with different epsilon
6. Once the initial library API is agreed all changes should go via github issue and discussion before a PR is created