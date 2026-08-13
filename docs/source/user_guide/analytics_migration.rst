Analytics package migration
===========================

The numerical analytical operator family moved from the core ``hgraph``
distribution to the separately installed ``hgraph-analytics`` distribution in
0.8. Install both packages and import the extension explicitly:

.. code-block:: bash

   pip install hgraph hgraph-analytics

.. code-block:: python

   import hgraph as hg
   import hgraph_analytics as hga

   change = hga.diff(price)
   returns = hga.pct_change(price)
   bounded = hga.clip(returns, -0.25, 0.25)
   smoothed = hga.ewma(bounded, alpha=0.2)

The former root-level Python names remain as deprecated compatibility graphs
for the 0.8 migration. Each graph emits ``DeprecationWarning`` when wired and
then imports and delegates to its ``hgraph_analytics`` replacement. The import
is deliberately lazy: ``import hgraph`` does not require or import the
analytics distribution, but wiring one of these aliases requires
``hgraph-analytics`` to be installed. New code should use the explicit
``hgraph_analytics`` names above.

Python name changes
-------------------

.. list-table::
   :header-rows: 1

   * - Former core name
     - New analytics name
   * - ``hgraph.diff``
     - ``hgraph_analytics.diff``
   * - ``hgraph.count``
     - ``hgraph_analytics.count``
   * - ``hgraph.clip``
     - ``hgraph_analytics.clip``
   * - ``hgraph.ewma``
     - ``hgraph_analytics.ewma``
   * - ``hgraph.pct_change`` and ``hgraph.nodes.pct_change``
     - ``hgraph_analytics.pct_change``
   * - ``hgraph.center_of_mass_to_alpha``
     - ``hgraph_analytics.center_of_mass_to_alpha``
   * - ``hgraph.span_to_alpha``
     - ``hgraph_analytics.span_to_alpha``
   * - ``hgraph.nodes.np_quantile`` and ``hgraph.numpy_.quantile``
     - ``hgraph_analytics.quantile``
   * - ``hgraph.nodes.np_std``
     - ``hgraph_analytics.array_std``
   * - ``hgraph.nodes.np_rolling_window``
     - ``hgraph_analytics.rolling_window``
   * - ``hgraph.std``
     - ``hgraph_analytics.std``
   * - ``hgraph.var``
     - ``hgraph_analytics.var``
   * - ``hgraph.rolling_average`` and ``hgraph.nodes.rolling_average``
     - ``hgraph_analytics.rolling_mean``
   * - ``hgraph.resample``
     - ``hgraph_analytics.resample``
   * - ``hgraph.numpy_.as_array``
     - ``hgraph_analytics.window_values``
   * - ``hgraph.numpy_.get_item``
     - ``hgraph_analytics.array_get_item``
   * - ``hgraph.numpy_.cumsum``
     - ``hgraph_analytics.cumulative_sum``
   * - ``hgraph.numpy_.corrcoef``
     - ``hgraph_analytics.correlation``

``pct_change`` keeps the one-observation fractional-change behavior when called
without additional arguments. The analytics version also accepts a positive
observation-count ``period`` and an explicit ``DivideByZero`` policy. The other
members of the original analytical family retain their existing 0.8 native
signatures and triggering behavior.
``array_std`` names the shaped-array reduction explicitly, avoiding ambiguity
with the collection and running ``std`` overloads. ``quantile`` drops the
NumPy-only ``keepdims`` compatibility argument because its contract returns a
scalar. ``rolling_window`` returns ``RollingWindowResult`` rather than the
retired ``NpRollingWindowResult`` compatibility schema.
``window_values`` names the actual fixed-window materialization rather than a
generic array conversion. ``array_get_item``, ``cumulative_sum``, and
``correlation`` use descriptive hgraph names instead of mirroring NumPy's
spellings. The ``hgraph.numpy_`` module is retired; import these functions from
``hgraph_analytics``. The former root operator spellings ``hgraph.as_array``,
``hgraph.get_item``, ``hgraph.cumsum``, ``hgraph.corrcoef``, and
``hgraph.quantile`` remain deprecated lazy aliases. The former
``hgraph.nodes.np_quantile``, ``np_std``, and ``np_rolling_window`` names are
also retained as deprecated aliases for source compatibility; no new
NumPy-prefixed APIs are added to analytics.

The generic dispersion and scheduled-statistics family has moved as well.
``std`` and ``var`` preserve the released shape-dependent contracts: a scalar
stream is a running population estimator, a typed window accepts ``ddof`` for
``std``, and current collections use their existing sample estimator with zero
for fewer than two valid members. ``rolling_average`` is now ``rolling_mean``
to use the same reduction noun as ``mean`` and dataframe APIs. ``resample``
retains its original behavior of emitting the latest valid value at every
positive engine-time interval, including intervals without a new source tick.

The former module also exported ``ARRAY``, ``ARRAY_1``, ``add_docs``,
``extract_type_from_array``, and ``extract_dimensions_from_array``. These were
module-authoring helpers rather than analytical operators and have no analytics
replacement. Use ``hgraph.Array`` directly in annotations and define local type
variables where a generic array annotation is needed; code should not depend on
the retired documentation-copying or private array-type inspection helpers.

C++ name changes
----------------

Native applications replace the ``hgraph::stdlib`` markers with the matching
``hgraph::analytics`` markers, link ``hgraph::analytics``, and register the
extension after registering the core standard operators:

.. code-block:: cpp

   #include <hgraph/analytics/operators.h>

   namespace hg = hgraph;
   namespace hga = hgraph::analytics;

   hg::stdlib::register_standard_operators();
   hga::register_analytics_operators();
   auto change = hg::wire<hga::diff>(wiring, price);

The moved markers are ``diff``, ``count``, ``clip``, ``ewma``, ``pct_change``,
``window_values``, ``array_get_item``, ``cumulative_sum``, ``correlation``,
``quantile``, ``array_std``, ``rolling_window``, ``std_``, ``var_``,
``rolling_mean``, and ``resample``. The C++
``hgraph::analytics::rolling_window`` marker consumes a typed core ``TSW``;
the Python convenience graph constructs that window from ``ts``, ``period``,
and ``min_window_period`` first. Core does not link to or register the
extension, so applications must opt in before wiring these names.

Scope
-----

This migration covers the operators from the released Python hgraph
``_analytical_operators`` family, ``pct_change``, every former
``hgraph.numpy_`` analytical transformation, and the remaining generic
statistical estimators. Core still owns generic window construction,
the primitive ``mean`` fold, the ``Array`` type, and structural indexing for
core collection types.
