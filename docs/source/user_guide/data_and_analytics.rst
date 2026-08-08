Data And Analytics
==================

Shaped arrays
-------------

``Array[T, Size[N], ...]`` is a native scalar schema. Each dimension is kept
in the C++ type record and contributes to the planned value layout. For
example, the C++ spelling of ``TS[Array[float, Size[2], Size[3]]]`` is
``TS<ArrayOf<Float, 2, 3>>``. Fixed arrays use inline planned storage with a
logical extent no larger than the declared leading-dimension capacity. This
allows a fixed tick window's value to expose its shorter warm-up prefix without
allocating a dynamic container. C++ writers set that extent with
``MutableListView::resize`` before filling a newly constructed array;
``Array[T]`` and ``Array[T, Size[-1]]`` describe an unbounded one-dimensional
array backed by the compact list representation.

The Python boundary accepts rectangular ``numpy.ndarray`` values and returns
an ndarray. Values larger than a declared dimension are rejected instead of
being flattened or silently reinterpreted. The C++ runtime and operator kernels
do not depend on NumPy.

Scientific operators (``hgraph.numpy_``)
----------------------------------------

The ``hgraph.numpy_`` name retains the familiar vocabulary used by existing
Python code. It is a native scientific-computation surface rather than a
promise to reproduce every NumPy edge-case or historical quirk. The complete
public catalogue delegates to C++ operators:

* ``as_array`` converts a fixed tick window and pads an early-valid window;
* ``get_item`` accepts an integer or integer tuple and resolves slice shape;
* ``cumsum`` flattens when ``axis`` is omitted and preserves shape for an axis;
* ``corrcoef`` accepts one- or two-dimensional numeric arrays and an optional
  second array; and
* ``quantile`` accepts an array or window and a scalar quantile.

The numeric kernels support ``int`` and ``float`` leaves. Quantile and standard
deviation delegate to Arrow Compute, while correlation delegates to Boost.Math;
their documented backend behaviour defines numerical edge cases. Arrow null
results are represented as floating-point NaN because the result time series
has a non-nullable scalar schema. Cumulative sum follows the hgraph array shape
and uses defined two's-complement wrapping for integer overflow.

``quantile`` supports ``linear``, ``lower``, ``higher``, ``midpoint``, and
``nearest`` interpolation; other methods fail explicitly. Its public result
remains a scalar, so ``keepdims`` is accepted for call compatibility but does
not change the result schema. ``as_array`` is limited to fixed tick windows;
duration windows have no fixed output shape.

The ``hgraph.nodes`` compatibility surface also provides native-backed
``np_rolling_window``, ``np_quantile``, ``np_std``, ``pct_change``,
``rolling_window``, and ``rolling_average``. A rolling window whose
``min_window_period`` is smaller than its capacity emits shorter ndarrays while
warming up. Those fields use an unbounded array dimension so the runtime schema
truthfully describes the values; the old Python implementation declared a
fixed dimension while returning shorter arrays.

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
