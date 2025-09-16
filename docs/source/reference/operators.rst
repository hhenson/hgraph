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

.. autofunction:: hgraph.all_

.. autofunction:: hgraph.any_

.. autofunction:: hgraph.apply

.. autofunction:: hgraph.average

.. autofunction:: hgraph.batch

.. autofunction:: hgraph.bit_and

.. autofunction:: hgraph.bit_or

.. autofunction:: hgraph.bit_xor

.. autoclass:: hgraph.BoolResult

.. autofunction:: hgraph.cast_

.. autofunction:: hgraph.call

.. autofunction:: hgraph.clip

.. autofunction:: hgraph.cmp_

.. autoclass:: hgraph.CmpResult

.. autofunction:: hgraph.combine

.. autofunction:: hgraph.const

.. autofunction:: hgraph.contains_

.. autofunction:: hgraph.convert

.. autofunction:: hgraph.collapse_keys

.. autofunction:: hgraph.collect

.. autofunction:: hgraph.count

.. autofunction:: hgraph.day_of_month

.. autofunction:: hgraph.default

.. autoclass:: hgraph.DebugContext

.. autofunction:: hgraph.debug_print

.. autofunction:: hgraph.dedup

.. autofunction:: hgraph.diff

.. autofunction:: hgraph.difference

.. autoclass:: hgraph.DivideByZero

.. autofunction:: hgraph.div_

.. autofunction:: hgraph.divmod_

.. autofunction:: hgraph.downcast_

.. autofunction:: hgraph.downcast_ref

.. autofunction:: hgraph.drop

.. autofunction:: hgraph.emwa

.. autofunction:: hgraph.emit

.. autofunction:: hgraph.eq_

.. autofunction:: hgraph.evaluation_time_in_range

.. autofunction:: hgraph.explode

.. autofunction:: hgraph.filter_

.. autofunction:: hgraph.filter_by

.. autofunction:: hgraph.flip

.. autofunction:: hgraph.flip_keys

.. autofunction:: hgraph.floordiv_

.. autofunction:: hgraph.format_

.. autofunction:: hgraph.from_json

.. autofunction:: hgraph.gate

.. autofunction:: hgraph.ge_

.. autofunction:: hgraph.getattr_

.. autofunction:: hgraph.getitem_

.. autofunction:: hgraph.gt_

.. autofunction:: hgraph.if_

.. autofunction:: hgraph.if_cmp

.. autofunction:: hgraph.if_then_else

.. autofunction:: hgraph.if_true

.. autofunction:: hgraph.index_of

.. autofunction:: hgraph.intersection

.. autofunction:: hgraph.invert_

.. autofunction:: hgraph.is_empty

.. autofunction:: hgraph.join

.. autofunction:: hgraph.keys_

.. autofunction:: hgraph.lag

.. autofunction:: hgraph.last_modified_date

.. autofunction:: hgraph.last_modified_time

.. autofunction:: hgraph.le_

.. autofunction:: hgraph.len_

.. autofunction:: hgraph.lift

.. autofunction:: hgraph.ln

.. autofunction:: hgraph.log_

.. autofunction:: hgraph.lshift_

.. autofunction:: hgraph.lt_

.. autoclass:: hgraph.Match

.. autofunction:: hgraph.match_

.. autofunction:: hgraph.max_

.. autofunction:: hgraph.mean

.. autofunction:: hgraph.merge

.. autofunction:: hgraph.min_

.. autofunction:: hgraph.mod_

.. autofunction:: hgraph.modified

.. autofunction:: hgraph.month_of_year

.. autofunction:: hgraph.mul_

.. autofunction:: hgraph.ne_

.. autofunction:: hgraph.neg_

.. autofunction:: hgraph.not_

.. autofunction:: hgraph.nothing

.. autofunction:: hgraph.null_sink

.. autofunction:: hgraph.or_

.. autofunction:: hgraph.partition

.. autofunction:: hgraph.pass_through_node

.. autofunction:: hgraph.pos_

.. autofunction:: hgraph.pow_

.. autofunction:: hgraph.print_

.. autofunction:: hgraph.race

.. autofunction:: hgraph.rekey

.. autofunction:: hgraph.replace

.. autofunction:: hgraph.resample

.. autofunction:: hgraph.round_

.. autofunction:: hgraph.route_by_index

.. autofunction:: hgraph.rshift_

.. autofunction:: hgraph.sample

.. autofunction:: hgraph.schedule

.. autofunction:: hgraph.slice_

.. autofunction:: hgraph.split

.. autofunction:: hgraph.std

.. autofunction:: hgraph.str_

.. autofunction:: hgraph.step

.. autofunction:: hgraph.stop_engine

.. autofunction:: hgraph.str_

.. autofunction:: hgraph.sub_

.. autofunction:: hgraph.substr

.. autofunction:: hgraph.sum_

.. autofunction:: hgraph.symmetric_difference

.. autofunction:: hgraph.take

.. autofunction:: hgraph.throttle

.. autofunction:: hgraph.to_json

.. autofunction:: hgraph.to_window

.. autofunction:: hgraph.type_

.. autofunction:: hgraph.uncollapse_keys

.. autofunction:: hgraph.union

.. autofunction:: hgraph.unpartition

.. autofunction:: hgraph.valid

.. autofunction:: hgraph.values_

.. autofunction:: hgraph.var

.. autoclass:: hgraph.WindowResult

.. autofunction:: hgraph.year

.. autofunction:: hgraph.zero
