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
``hgraph_analytics``.

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
``quantile``, ``array_std``, and ``rolling_window``. The C++
``hgraph::analytics::rolling_window`` marker consumes a typed core ``TSW``;
the Python convenience graph constructs that window from ``ts``, ``period``,
and ``min_window_period`` first. Core does not link to or register the
extension, so applications must opt in before wiring these names.

Scope
-----

This migration covers the operators from the released Python hgraph
``_analytical_operators`` family, ``pct_change``, and every former
``hgraph.numpy_`` analytical transformation. Core still owns generic window
construction, reductions such as ``mean``/``std``/``var``, the ``Array`` type,
and structural indexing for core collection types.
