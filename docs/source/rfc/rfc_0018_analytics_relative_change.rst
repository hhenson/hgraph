RFC 0018: Analytics Relative Change
===================================

:Status: Proposed
:Author: Howard Henson
:Created: 2026-08-11
:Target: hgraph-analytics 0.8

Summary
-------

Introduce the first C++-first operator in the ``hgraph-analytics`` extension:
``pct_change`` computes the fractional change between the current numeric
observation and an earlier observation selected by a positive tick-count
period. The extension is a separately installed CMake and Python distribution
inside the hgraph monorepo, as proposed by :doc:`rfc_0005_hgraph_1_0_api`.

The existing one-period ``hgraph.pct_change`` implementation moves into
``hgraph_analytics`` with the rest of the numerical analytical family. The
extension adds the period and division-policy contract specified here without
leaving a second core registry contract behind.

Motivation
----------

Fractional change recurs in telemetry, scientific processing, economic data,
and financial research. The 0.8 core operator preserves the original Python
graph's one-period expression but cannot state a longer observation horizon or
zero-denominator policy. Downstream momentum work has now exercised both the
one-period and longer-period forms, providing implementation evidence for a
domain-independent contract.

``pct_change`` is analytics rather than a financial return. It operates on
plain numbers and knows nothing about price kind, corporate actions, currency,
market sessions, sampling, or missing-price policy. Those financial semantics
remain downstream concerns.

Ownership boundary
------------------

Core ``hgraph`` owns the causal primitives and policies used by the graph:
``lag``, arithmetic operators, tick validity, and ``DivideByZero``.

``hgraph-analytics`` owns generic numerical transforms and estimators whose
period, denominator, warm-up, or window conventions are analytics policy. This
RFC adds ``pct_change`` and migrates the existing ``diff``, ``count``, ``clip``,
and ``ewma`` family with it. Rolling-window, reduction, and shaped-array
operators retain their existing ownership and follow separate acceptance work.

Downstream finance packages own simple, log, and total-return contracts that
select and align adjusted prices, currencies, calendars, and sessions. They may
compose this generic operator after making those choices explicit.

Public contract
---------------

Python
~~~~~~

.. code-block:: python

   import hgraph as hg
   import hgraph_analytics as hga

   one_period = hga.pct_change(value)
   annual = hga.pct_change(
       value,
       period=12,
       divide_by_zero=hg.DivideByZero.NAN,
   )

The signature is:

.. code-block:: text

   pct_change(
       ts: TS[number],
       period: int = 1,
       divide_by_zero: DivideByZero = DivideByZero.ERROR,
   ) -> TS[float]

C++
~~~

The installed extension exports ``hgraph::analytics`` and the public marker
``hgraph::analytics::pct_change``. Applications call
``register_analytics_operators()`` once per operator-registry lifetime, then
wire the marker normally:

.. code-block:: cpp

   namespace hg = hgraph;
   namespace hga = hgraph::analytics;

   auto change = hg::wire<hga::pct_change, hg::TS<hg::Float>>(
       wiring, value, hg::Int{12},
       hg::stdlib::DivideByZero::Nan);

Semantics
~~~~~~~~~

For positive ``period = p``, the output on accepted observation ``t`` is:

.. math::

   (x_t - x_{t-p}) / x_{t-p}

The operator:

* counts valid source observations, not graph cycles or wall-clock intervals;
* produces no output until the prior observation exists;
* triggers only from a valid input tick;
* returns ``TS[float]`` for integer and floating-point inputs;
* applies the wiring-time ``DivideByZero`` policy when the prior value is zero;
* otherwise preserves normal IEEE propagation of NaN and infinity; and
* never fills, resamples, filters, or changes the input's observation schedule.

The name follows established dataframe vocabulary but the result is a
fractional change: ``0.05`` denotes five percent. It is not multiplied by 100.

Errors and exclusions
---------------------

``period <= 0`` is rejected while wiring. Negative periods would require a
future observation and violate the causal streaming contract.

A duration is deliberately not accepted. ``lag(ts, timedelta)`` delays an
event; it does not define which historical observation represents a duration
endpoint on an irregular stream. Callers must first establish an explicit
sampling or session schedule and then use an observation-count period.

Dataframe row-wise percentage change is also outside this contract. A future
tabular operator must state its ordering, partitioning, selected columns, and
axis rather than infer a time axis from row position.

Implementation
--------------

The extension registers concrete integer and floating-point graph overloads.
Each graph wires one tick-count ``lag``, subtracts the prior value from the
current value, and divides by that same prior port with the selected core
policy. The graph adds no per-tick allocation or policy string dispatch; the
only retained state is the core lag queue of ``period`` values.

The implementation remains a graph composition rather than a bespoke runtime
node so triggering, readiness, recording, and division behavior stay owned by
their core primitives.

Compatibility and migration
---------------------------

The core ``hgraph.pct_change(ts)`` contract and registry name are removed in the
same migration that introduces ``hgraph.analytics.pct_change``. Callers import
``hgraph_analytics``; a one-period call is otherwise source equivalent, while
longer periods and explicit zero policies are additive extension surface. The
complete analytical-family mapping is recorded in
:doc:`../user_guide/analytics_migration`. Core never imports or autoloads the
extension.

Alternatives considered
-----------------------

Extending the 0.8 core marker would deepen a dependency that RFC 0005 already
proposes removing. Naming the operator ``return`` would incorrectly claim
financial sampling and adjustment semantics. Adding ``ratio``,
``growth_factor``, or ``log_change`` in the same change would create a family
without equivalent downstream evidence; those remain compositions or later
proposals.

Acceptance criteria
-------------------

* Public C++ and Python calls cover integer and floating-point inputs,
  one-period and longer-period changes, warm-up gaps, sparse valid ticks,
  every applicable zero-denominator policy, and invalid periods.
* C++ and Python use the same native graph overloads and golden results.
* A separately configured installed-SDK C++ consumer builds and wires the
  operator.
* A separately built stable-ABI wheel installs beside the matching hgraph wheel
  and passes its Python test suite on the newest supported Python.
* Normal core builds do not build, import, or install ``hgraph-analytics``.

Implementation status
---------------------

The proposal branch contains the extension package, native graph overloads,
Python authoring facade, behavioral tests, and installed-CMake consumer. The
RFC remains Proposed until implementation and conformance evidence merge.
