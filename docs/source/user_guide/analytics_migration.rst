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

``pct_change`` keeps the one-observation fractional-change behavior when called
without additional arguments. The analytics version also accepts a positive
observation-count ``period`` and an explicit ``DivideByZero`` policy. The other
operators retain their existing 0.8 native signatures and triggering behavior.

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

The moved markers are ``diff``, ``count``, ``clip``, ``ewma``, and
``pct_change``. Core does not link to or register the extension, so applications
must opt in before wiring these names.

Scope
-----

This migration covers the operators from the released Python hgraph
``_analytical_operators`` family and ``pct_change``. Window construction and
rolling windows, reductions such as ``mean``/``std``/``var``, and shaped-array
operators remain in core; their contracts belong to those existing operator
families rather than this package move.
