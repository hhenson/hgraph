RFC 0020: Analytics Statistics Ownership
========================================

:Status: Proposed
:Author: Howard Henson
:Created: 2026-08-12
:Target: hgraph-analytics 0.8

Summary
-------

Complete the RFC 0005 analytics boundary by moving ``std``, ``var``,
``rolling_average``, and ``resample`` from the core ``hgraph`` distribution to
``hgraph-analytics``. The first two retain their released shape-dependent
contracts, ``rolling_average`` becomes ``rolling_mean``, and ``resample`` keeps
its scheduled latest-value behavior. No compatibility operator remains in
core.

Motivation
----------

These operators select estimator, warm-up, denominator, or sampling policy.
They are useful well beyond finance, but they are analytical vocabulary rather
than primitives required to express an hgraph program. Keeping them in core
would contradict the compact-kernel boundary in RFC 0005 and split one
analytical family across two distributions.

The existing implementations and downstream momentum work provide the
required experience. This is an ownership migration, not a new statistics
framework: preserving established results is more important than normalizing
all historical overloads in the same change.

Ownership boundary
------------------

Core owns numeric arithmetic, ``sum_`` and the primitive ``mean`` fold,
generic ``lag`` and scheduling, collection shape, and ``to_window``/``TSW``
storage. ``hgraph-analytics`` owns estimators and transforms that assign
statistical meaning to those primitives, including dispersion, rolling means,
and scheduled resampling. Domain packages remain responsible for calendars,
market sessions, price adjustment, return definitions, and missing-data
policy.

Public contract
---------------

Python imports the separately installed extension explicitly:

.. code-block:: python

   import hgraph as hg
   import hgraph_analytics as hga

   running_volatility = hga.std(observations)
   sample_volatility = hga.std(
       hg.to_window(observations, 20, 20), ddof=1
   )
   running_variance = hga.var(observations)
   moving_average = hga.rolling_mean(observations, 20)
   sampled = hga.resample(observations, hg.MIN_TD * 5)

The native markers are ``hgraph::analytics::std_``, ``var_``,
``rolling_mean``, and ``resample``. Applications link
``hgraph::analytics`` and call ``register_analytics_operators()`` after core
registration.

Dispersion semantics
~~~~~~~~~~~~~~~~~~~~

``std`` and ``var`` deliberately retain the released overload policies:

* a scalar numeric stream is a running population estimator and emits from the
  first valid observation;
* ``std(TSW, ddof=0)`` reduces the current ready window using divisor
  ``N - ddof`` and produces NaN when that divisor is not positive;
* tuple, frozenset, frozendict-value, ``TSL``, ``TSS``, ``TSD``, and homogeneous
  ``TSB`` reductions treat current valid members as a sample, divide by
  ``N - 1``, and produce zero for fewer than two members; and
* binary and element-wise structured overloads compute the two-value sample
  estimator at each output position.

Invalid scalar cycles do not advance running state. Collection changes trigger
according to their normal core shape semantics. ``ddof`` is fixed while wiring
and applies only to a typed window. The migrated scalar-container overloads
continue to accept their historical ``default_value`` argument; their numeric
zero/NaN empty-result policies remain authoritative.

Rolling and resampling semantics
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``rolling_mean(ts, period, min_window_period=None)`` accepts either positive
observation counts or durations fixed while wiring. A missing or zero tick
minimum selects full-period warm-up; an explicit minimum permits partial
windows. Duration windows divide by the count of covered ticks. Invalid input
cycles add no observation.

``resample(ts, period)`` schedules at a positive fixed engine-time duration.
At each boundary it emits the latest valid value; after initial validity this
continues even when no new source tick arrives. It is therefore distinct from
``throttle``, which produces nothing during an interval with no source tick.

Errors and exclusions
---------------------

Invalid rolling period/minimum combinations fail while wiring. A non-positive
resampling duration fails during node start. Unsupported value shapes fail
overload resolution. This RFC does not add weighted estimators, NaN filtering,
finite-value reset policy, time-weighted means, covariance, or a windowed
``var`` convenience; each would require an explicit contract rather than an
inference from dataframe behavior.

Compatibility and migration
---------------------------

The source mapping is:

.. list-table::
   :header-rows: 1

   * - Former core Python name
     - Extension name
   * - ``hgraph.std``
     - ``hgraph_analytics.std``
   * - ``hgraph.var``
     - ``hgraph_analytics.var``
   * - ``hgraph.rolling_average``
     - ``hgraph_analytics.rolling_mean``
   * - ``hgraph.resample``
     - ``hgraph_analytics.resample``

The corresponding core C++ markers and registrations are removed. The rename
uses ``mean`` consistently for the reduction being rolled and avoids a
permanent compatibility alias during the 0.x API calibration. Core never
imports, links, or autoloads the extension. The migration table in the user
guide is the durable application-facing note.

Performance, state, and ABI
---------------------------

Running dispersion uses Welford state and is constant-time and constant-space
per tick. Current-collection and window reductions are linear in the number of
valid members and retain no additional history. ``rolling_mean`` composes core
running sums, lag, and count nodes. ``resample`` retains the latest input and
scheduler state. The package exports only analytics markers and its
registration function; core's ABI and dependency set do not acquire an
analytics dependency.

Alternatives considered
-----------------------

Leaving compatibility aliases in core would keep the ownership split and make
an optional package appear to be core surface. Renaming every shape to a
separate function would be clearer in isolation but would turn an ownership
move into a wider source and semantic break. Moving ``mean`` would remove a
primitive fold used by generic collection code, so it remains beside ``sum_``.

Acceptance criteria
-------------------

* Core no longer exports or registers the four former operators.
* The extension exposes documented C++ markers and matching Python functions.
* Native and Python tests cover running, windowed, binary, scalar-container,
  ``TSL``/``TSS``/``TSD``/``TSB``, tick/duration rolling, and resampling
  contracts.
* The distribution audit, installed CMake consumer, and built-wheel tests pass.
* Full core native tests and the newest supported Python installed-wheel suite
  pass without installing or importing ``hgraph-analytics``.
* The downstream momentum strategy consumes ``hgraph_analytics.std`` through
  a separately linked pull request.

Implementation status
---------------------

The proposal branch removes core registrations and Python exports, registers
the migrated overloads in ``hgraph-analytics``, and moves behavioral coverage
and migration documentation with them. The RFC remains Proposed until the
implementation and conformance evidence merge.
