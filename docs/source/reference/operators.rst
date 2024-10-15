Operators
=========

The operators are a set of interfaces describing an operation that can be applied over a number of potential inputs
types.

Where possible the operators are mapped onto Python symbolic operators (for example +, -, /, etc.).
When describing an operator that clashes with a builtin operator name such as ``add`` we add an ``_`` to the end of
the name. In this case it would be ``add_``.

See this for a quick guide of supported types and operators :doc:`operators_support`.

These are the operators that are provided by the HGraph library.

.. autofunction:: hgraph.abs_

.. autofunction:: hgraph.add_

.. autofunction:: hgraph.bit_and

.. autofunction:: hgraph.bit_or

.. autofunction:: hgraph.bit_xor

.. autofunction:: hgraph.contains_

.. autofunction:: hgraph.difference

.. autoclass:: hgraph.DivideByZero

.. autofunction:: hgraph.div_

.. autofunction:: hgraph.divmod_

.. autofunction:: hgraph.eq_

.. autofunction:: hgraph.floordiv_

.. autofunction:: hgraph.ge_

.. autofunction:: hgraph.getattr_

.. autofunction:: hgraph.getitem_

.. autofunction:: hgraph.gt_

.. autofunction:: hgraph.intersection

.. autofunction:: hgraph.invert_

.. autofunction:: hgraph.is_empty

.. autofunction:: hgraph.le_

.. autofunction:: hgraph.len_

.. autofunction:: hgraph.lshift_

.. autofunction:: hgraph.lt_

.. autofunction:: hgraph.max_

.. autofunction:: hgraph.mean

.. autofunction:: hgraph.min_

.. autofunction:: hgraph.mod_

.. autofunction:: hgraph.mul_

.. autofunction:: hgraph.ne_

.. autofunction:: hgraph.neg_

.. autofunction:: hgraph.not_

.. autofunction:: hgraph.or_

.. autofunction:: hgraph.pos_

.. autofunction:: hgraph.pow_

.. autofunction:: hgraph.rshift_

.. autofunction:: hgraph.std

.. autofunction:: hgraph.str_

.. autofunction:: hgraph.sub_

.. autofunction:: hgraph.sum_

.. autofunction:: hgraph.symmetric_difference

.. autofunction:: hgraph.type_

.. autofunction:: hgraph.union

.. autofunction:: hgraph.var

.. autofunction:: hgraph.zero
