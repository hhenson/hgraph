Data And Analytics
==================

Shaped arrays
-------------

``Array[T, Size[N], ...]`` describes a shaped scalar value. Python code sends
and receives rectangular ``numpy.ndarray`` values:

.. code-block:: python

   from hgraph import Array, Size, TS

   Matrix = TS[Array[float, Size[2], Size[3]]]

Values larger than a declared dimension are rejected rather than flattened or
silently reinterpreted. ``Array[T]`` and ``Array[T, Size[-1]]`` describe an
unbounded one-dimensional array.

The dimensions are retained in the native type record and planned value
layout; the equivalent C++ spelling of the example is
``TS<ArrayOf<Float, 2, 3>>``. Fixed arrays use inline storage with a logical
extent no larger than the declared leading-dimension capacity. This lets a
fixed tick window expose its shorter warm-up prefix without allocating a
dynamic container. The native runtime and operator kernels do not depend on
NumPy.

Scientific operators (``hgraph_analytics``)
-------------------------------------------

Shaped-array analytics live in the separately installed ``hgraph-analytics``
package. The public catalogue delegates to C++ operators:

* ``window_values`` materializes a fixed tick window and pads an early-valid
  window;
* ``array_get_item`` accepts an integer or integer tuple and resolves slice
  shape;
* ``cumulative_sum`` flattens when ``axis`` is omitted and preserves shape for
  an axis;
* ``correlation`` accepts one- or two-dimensional numeric arrays and an
  optional second array;
* ``quantile`` and ``array_std`` provide scalar reductions; and
* ``rolling_window`` returns shaped value and timestamp arrays.

Generic estimators also live in the extension. ``std`` and ``var`` cover
running numeric streams and current collection shapes, while ``std`` also
reduces a core ``TSW`` with an explicit ``ddof``. ``rolling_mean`` computes a
trailing tick- or duration-window mean, and ``resample`` reticks the latest
value at a regular engine-time interval. Core retains ``mean`` as a primitive
fold and ``to_window`` as generic storage; callers opt into the policy-bearing
estimators by importing ``hgraph_analytics``.

The former ``hgraph.numpy_`` module is retired. See
:doc:`analytics_migration` for the complete name mapping.

The numeric kernels support ``int`` and ``float`` leaves. Correlation delegates
to Boost.Math, while cumulative sum follows the hgraph array shape and uses
defined two's-complement wrapping for integer overflow. Quantile and shaped-array
standard deviation now belong to ``hgraph-analytics`` and continue to use Arrow
Compute there.

``hgraph_analytics.quantile`` supports ``linear``, ``lower``, ``higher``,
``midpoint``, and ``nearest`` interpolation; other methods fail explicitly.
Its public result is a scalar, so the obsolete ``keepdims`` compatibility
argument was removed. ``window_values`` is limited to fixed tick windows;
duration windows have no fixed output shape.

The core ``hgraph.nodes`` compatibility surface retains the generic
``rolling_window`` alias for ``window``. The NumPy-prefixed conveniences and
the policy-bearing ``rolling_average`` moved to ``hgraph-analytics`` as
``rolling_window``, ``quantile``, ``array_std``, and ``rolling_mean``; see
:doc:`analytics_migration`. An analytics rolling window whose
``min_window_period`` is smaller than its capacity emits shorter arrays while
warming up. Those fields use an unbounded array dimension so the runtime schema
truthfully describes the values.

Dataframes and series
---------------------

``Frame`` and ``Series[T]`` use Arrow-native storage. Sorting, concatenation,
joins, structural filtering, grouping, ungrouping, column replacement and
projection execute in C++. Python expression filters remain a Python-owned
compatibility path because ``pyarrow.compute.Expression`` is itself a Python
scalar. Series-to-tuple conversion is native and represents Arrow nulls as
unset tuple elements.

Typed frame metadata
~~~~~~~~~~~~~~~~~~~~

``Frame[Rows, Metadata]`` stores frame-level identity and provenance in the
Arrow table's own schema metadata. It is intended for values such as an as-of
time, source, universe definition, or plan version that apply to the complete
table and must not be repeated on every row. The C++ spelling is
``FrameOf<Rows, Metadata>``; the Python value remains an ordinary
``pyarrow.Table``.

The metadata schema must be a named ``CompoundScalar``/``Bundle``. The codec
uses these reserved byte-string entries in the Arrow schema metadata:

* ``hgraph.metadata.schema`` optionally identifies the qualified hgraph
  metadata schema;
* ``hgraph.metadata.version`` identifies the metadata wire format; and
* ``hgraph.metadata.field.<name>`` stores each populated field separately.

Supported atomic fields (``str``, ``int``, ``float``, ``bool``, enums, dates,
datetimes, times, and durations) use their plain string form. Tuple, Bundle,
list, set, and map fields use the existing schema-directed JSON codec. Binary,
opaque Python objects, callables, and other values without that codec are
rejected. Unrelated Arrow schema metadata is retained.

Use ``with_frame_metadata(table, value)`` to return a table with encoded
metadata and ``frame_metadata(table, MetadataType)`` to decode it. The schema
argument is authoritative, so a table remains compliant when its field entries
match that schema but the optional ``hgraph.metadata.schema`` marker is absent.
When the marker is present, ``frame_metadata(table)`` can resolve the registered
schema reflectively and an explicitly supplied schema is checked for
compatibility. Markerless metadata cannot be decoded reflectively and therefore
requires the schema argument. The ``has_frame_metadata`` predicate detects any
reserved hgraph entry;
``without_frame_metadata`` removes only the reserved hgraph entries. The C++
functions have the same names and operate on ``Frame``/``Value``.

Sorting, filtering, and column replacement preserve the Arrow schema metadata.
Concatenation requires equal hgraph metadata on both inputs and preserves it. A
join has no generally correct metadata rule, so metadata-bearing frames do not
match the row-only join overload: choose and implement the result metadata
explicitly. The Python bridge rejects missing or incompatible encoded metadata,
and rejects a metadata-bearing table supplied to ``Frame[Rows]``.
