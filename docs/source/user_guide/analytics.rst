Analytics extension
===================

``hgraph-analytics`` is the separately installed, C++-first numerical
analytics package for hgraph.  It provides numerical transforms, trailing
estimators, scheduled sampling, and shaped-array reductions without adding a
second runtime: Python graphs call the same native operators that C++ graphs
wire directly.

Install the core package and extension, then import the extension explicitly:

.. code-block:: console

   pip install hgraph hgraph-analytics

.. code-block:: python

   import hgraph as hg
   import hgraph_analytics as hga

The extension owns analytics policy—warm-up, observation windows, interpolation,
and scheduling.  Core still owns primitive values, ``Array`` types, and generic
window storage (``hg.to_window``).  This division matters in practice: create a
window in core when you need retained observations, then choose an analytics
operator to interpret that window.

Choosing an operator
--------------------

The operators fall into three useful groups:

.. list-table::
   :header-rows: 1
   :widths: 27 37 36

   * - Input and intent
     - Operators
     - Key behavior
   * - One numeric stream; transform each valid tick or retain a small state
     - ``diff``, ``count``, ``clip``, ``ewma``, ``pct_change``
     - ``diff`` and ``pct_change`` wait for prior observations; ``ewma`` and
       ``count`` retain state; ``clip`` is stateless.
   * - One stream over a trailing tick- or time window
     - ``rolling_mean``, ``rolling_window``, ``window_values``, ``quantile``,
       ``std``
     - The source must tick enough times to satisfy the configured warm-up.
       Tick windows have a fixed capacity; duration windows are time based.
   * - Current shaped ``Array`` value
     - ``array_get_item``, ``cumulative_sum``, ``correlation``, ``quantile``,
       ``array_std``
     - Each valid array value is reduced or transformed independently; these
       operators do not retain history.
   * - Latest value on a regular engine-time schedule
     - ``resample``
     - It continues to tick the latest valid input even when the source is
       quiet.
   * - Dispersion selected from input shape
     - ``std``, ``var``
     - A scalar stream is a running population estimator; collections and
       windows use their own documented shape-specific reductions.

All examples use *valid observations*.  An absent source tick does not update
state, enter a tick-count window, or count toward ``pct_change(period=...)``.
This is intentional: ``period`` means observation count, not elapsed time or
dataframe row count.

Quick start: stream features
----------------------------

This graph creates common features from a price stream.  The results are time
series, so downstream graph code receives an output only when the relevant
operator's trigger and warm-up conditions are met.

.. code-block:: python

   import hgraph as hg
   import hgraph_analytics as hga


   @hg.graph
   def price_features(price: hg.TS[float]):
       one_tick_move = hga.diff(price)
       five_observation_return = hga.pct_change(
           price,
           period=5,
           divide_by_zero=hg.DivideByZero.NAN,
       )
       capped_return = hga.clip(five_observation_return, -0.20, 0.20)
       smoothed_return = hga.ewma(capped_return, alpha=0.2)
       moving_average = hga.rolling_mean(price, period=20, min_window_period=5)
       return one_tick_move, smoothed_return, moving_average

``diff`` emits no result for the first valid observation because it is saved
as the prior value.  ``pct_change`` similarly emits no result for its first
``period`` valid observations.  Its output is fractional: ``0.05`` means a
five-percent increase, not ``5.0``.  A zero prior value follows the explicit
``hg.DivideByZero`` policy; the default is ``ERROR``.  Useful alternatives are
``NAN``, ``ZERO``, ``ONE``, and ``NONE`` (do not produce a result).

``ewma`` starts at the first valid input and then applies
``alpha * current + (1 - alpha) * previous``.  The helpers
``center_of_mass_to_alpha(com)`` and ``span_to_alpha(span)`` convert familiar
positive EWMA parameters to ``alpha``:

.. code-block:: python

   alpha = hga.span_to_alpha(20)  # 2 / (20 + 1)
   smoothed_price = hga.ewma(price, alpha)

Use ``count(signal, reset=...)`` when values are irrelevant and you need a
running count of valid ticks.  A same-cycle reset is applied before the source
tick, so that tick becomes one.  A reset with no source tick clears state but
does not emit a count.

.. code-block:: python

   session_updates = hga.count(order_update, reset=session_open)

Trailing windows and warm-up
----------------------------

``rolling_mean`` is the convenient answer when you only need a trailing mean.
It accepts either a positive tick count or a positive ``datetime.timedelta``.
For tick windows, ``min_window_period`` controls the first output: omitted or
zero means wait for the full window, while a smaller positive number allows an
early result.  Duration windows use the observations covered by the duration
as their denominator.

.. code-block:: python

   from datetime import timedelta

   # First output after three valid price observations; thereafter use the
   # latest five valid observations.
   fast_average = hga.rolling_mean(price, period=5, min_window_period=3)

   # Keep the values seen during the last 30 seconds, using the same warm-up
   # rule expressed as a duration.
   short_horizon = hga.rolling_mean(
       price,
       period=timedelta(seconds=30),
       min_window_period=timedelta(seconds=10),
   )

For more than a mean, create a fixed tick window with ``hg.to_window``.  The
window owns retention and warm-up; analytics operators consume its current
contents.  The following graph derives a median, sample volatility, a padded
fixed-size array, and a value/timestamp pair for charting or a native consumer:

.. code-block:: python

   @hg.graph
   def trailing_statistics(price: hg.TS[float]):
       window = hg.to_window(price, 20, 5)
       median = hga.quantile(window, q=0.5, method="linear")
       sample_volatility = hga.std(window, ddof=1)
       padded_values = hga.window_values(window, zero=0.0)
       values_and_times = hga.rolling_window(
           price, period=20, min_window_period=5
       )
       return median, sample_volatility, padded_values, values_and_times

``window_values`` has a fixed-capacity array output and therefore accepts only
fixed tick windows.  During early output it places values in chronological
order and fills the unused suffix with ``zero`` (or the element type's default).
``rolling_window`` instead returns ``RollingWindowResult`` with ``buffer`` and
``index`` fields.  When early output is enabled, the leading array dimension is
the current population; after warm-up it reaches the requested capacity.

``quantile`` accepts a live ``q`` in ``[0, 1]`` and a wiring-time interpolation
method: ``linear``, ``lower``, ``higher``, ``midpoint``, or ``nearest``.  It
returns a scalar and does not support NumPy's ``keepdims`` argument.  ``std``
over a typed tick window accepts ``ddof``; if the population is not greater
than ``ddof``, it returns ``NaN``.  ``array_std`` is the equivalent reduction
when observations already arrive in an array.

Arrays: current-vector and matrix analytics
--------------------------------------------

An ``Array`` declares the numerical leaf type and shape at the graph boundary.
Python values are rectangular ``numpy.ndarray`` objects, while the native
runtime stores the typed, planned array without depending on NumPy internally.

.. code-block:: python

   import numpy as np

   Vector4 = hg.TS[hg.Array[float, hg.Size[4]]]
   Matrix2x3 = hg.TS[hg.Array[float, hg.Size[2], hg.Size[3]]]


   @hg.graph
   def array_features(
       observations: Vector4,
       matrix: Matrix2x3,
   ):
       p90 = hga.quantile(observations, q=0.9, method="higher")
       sample_std = hga.array_std(observations, ddof=1)
       cumulative_rows = hga.cumulative_sum(matrix, axis=-1)
       second_row = hga.array_get_item(matrix, 1)
       correlation_matrix = hga.correlation(matrix, rowvar=True)
       return p90, sample_std, cumulative_rows, second_row, correlation_matrix

``array_get_item`` takes a wiring-time integer or integer tuple.  Negative
components count from the end; selecting fewer dimensions returns the
lower-rank slice.  ``cumulative_sum`` without ``axis`` flattens its output;
with an axis (including a negative axis) it retains the input shape.  Integer
accumulation has defined two's-complement wrapping.

``correlation`` accepts one- or two-dimensional numeric arrays.  With a vector
it returns a scalar correlation; with a matrix it returns a square coefficient
matrix.  Rows are variables by default; pass ``rowvar=False`` to treat columns
as variables.  Constant variables yield ``NaN`` coefficients, and unequal
observation counts or unsupported shapes fail explicitly.

Running and collection dispersion
---------------------------------

``std`` and ``var`` deliberately dispatch on input shape.  This lets a graph
use the same operator family while making the statistical population clear:

.. code-block:: python

   @hg.graph
   def dispersion(
       value: hg.TS[float],
       current_values: hg.TS[tuple[float, ...]],
   ):
       running_population_std = hga.std(value)
       running_population_var = hga.var(value)
       current_sample_std = hga.std(current_values)
       current_sample_var = hga.var(current_values)
       return (
           running_population_std,
           running_population_var,
           current_sample_std,
           current_sample_var,
       )

A scalar stream produces a *running population* estimator.  A scalar
collection reduces its current members using the existing collection contract:
for example, fewer than two current members yield zero.  Fixed lists and
binary inputs use element-wise forms.  A typed tick window is a distinct
``std`` overload, shown in `Trailing windows and warm-up`_; it accepts ``ddof``
because its retained observation population is explicit.

Regular engine-time sampling
----------------------------

``resample`` is a scheduler, not a debounce operator.  It remembers the most
recent valid input and emits it at each positive engine-time interval after
the first valid input, including intervals during which the source is silent.

.. code-block:: python

   from datetime import timedelta

   every_five_seconds = hga.resample(price, timedelta(seconds=5))

Use it when a downstream calculation needs regular graph-time updates from an
irregular source.  If you instead need to limit output frequency *without*
emitting during quiet intervals, use the core throttling operator rather than
``resample``.

Native C++ wiring
-----------------

Native applications link ``hgraph::analytics``, include its public header,
and register the extension's overloads once for each operator registry
lifetime after the core standard operators.  The following is a compact native
wiring pattern; application graph inputs and output use the normal hgraph C++
authoring APIs.

.. code-block:: cpp

   #include <hgraph/analytics/operators.h>
   #include <hgraph/lib/std/operators/registration.h>

   namespace hg = hgraph;
   namespace hga = hgraph::analytics;

   // During application setup, before building graphs that use analytics:
   hg::stdlib::register_standard_operators();
   hga::register_analytics_operators();

   struct FeaturesGraph {
       static constexpr auto name = "features_graph";

       static void compose(hg::Wiring &w, hg::Port<hg::TS<hg::Float>> price) {
           auto change = hg::wire<hga::pct_change, hg::TS<hg::Float>>(
               w, price, hg::Int{5}, hg::stdlib::DivideByZero::Nan);
           auto average = hg::wire<hga::rolling_mean, hg::TS<hg::Float>>(
               w, price, hg::Int{20}, hg::Int{5});
           auto window = hg::wire<hg::stdlib::to_window>(
               w, price, hg::Int{20}, hg::Int{5})
               .as<hg::TSW<hg::Float, 20, 5>>();
           auto sample_std = hg::wire<hga::std_, hg::TS<hg::Float>>(
               w, window, hg::arg<"ddof">(hg::Int{1}));
           static_cast<void>(change);
           static_cast<void>(average);
           static_cast<void>(sample_std);
       }
   };

The native marker for Python's ``hga.std`` is named ``hga::std_`` because
``std`` is already a standard-library identifier.  Other names map directly:
``diff``, ``count``, ``clip``, ``ewma``, ``pct_change``, ``window_values``,
``array_get_item``, ``cumulative_sum``, ``correlation``, ``quantile``,
``array_std``, ``rolling_window``, ``var_``, ``rolling_mean``, and
``resample``.

Migration and limits
--------------------

The core names remain as deprecated, lazy compatibility aliases during the
0.8 migration.  New code should import ``hgraph_analytics`` explicitly so a
normal ``import hgraph`` neither imports nor depends on the extension.  The
complete Python and C++ mapping is in :doc:`analytics_migration`.

The extension is numerical analytics, not a data-frame or market-data policy
layer.  In particular, ``pct_change`` does not infer row ordering, elapsed-time
sampling, trading calendars, adjustment factors, or a financial-return
definition.  Make those policies explicit in the graph that prepares its
numeric input.
