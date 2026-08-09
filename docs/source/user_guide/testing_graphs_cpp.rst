Testing Graphs in C++
=====================

The C++ testing toolkit mirrors the Python ``eval_node`` model: feed a node (or
graph) a fixed sequence of inputs, run it in simulation, and read back the
sequence of outputs. It is built from two reusable static nodes — ``replay`` (a
source that emits a recorded sequence) and ``record`` (a sink that captures one)
— both layered on the :doc:`GlobalState <authoring_nodes_cpp>` injectable, plus
the :doc:`NodeScheduler <authoring_nodes_cpp>` for multi-cycle ticking.

Most tests use the high-level :ref:`eval_node <eval-node>` harness (below), which
wires ``replay → node-under-test → record`` for you. This page documents that
harness and the ``replay`` / ``record`` building blocks it is made of, including
their shared buffer representation.

.. _eval-node:

``eval_node``
-------------

``eval_node<NodeT>(inputs)`` runs a node over a sequence of per-cycle inputs and
returns its per-cycle outputs — the one call most node tests need:

.. code-block:: cpp

   struct AddOne
   {
       static constexpr auto name = "add_one";
       static void           eval(In<"in", TS<Int>> in, Out<TS<Int>> out) { out.set(in.value() + 1); }
   };

   // One evaluation cycle per element; `none` (= std::nullopt) means no tick that cycle.
   CHECK_OUTPUT(testing::eval_node<AddOne>({Int{1}, none, Int{3}}), {Int{2}, none, Int{4}});

Each input element is fed at successive evaluation cycles from ``MIN_ST`` and the graph
runs until it is **quiescent**; the result is the recorded output, cycle-aligned
(``output[i]`` is the node's tick at cycle ``i``, or ``none`` if it did not tick).
It is padded to **at least** the input length, but is **not truncated** — a node
that emits beyond the input window (e.g. a scheduled follow-up tick) produces a
longer result, and those ticks are kept. Node ``State`` persists across cycles, so
a stateful node accumulates as expected:

.. code-block:: cpp

   CHECK_OUTPUT(testing::eval_node<RunningSum>({Int{1}, Int{2}, Int{3}, Int{4}}),
                {Int{1}, Int{3}, Int{6}, Int{10}});

Arguments may be positional in the node's **eval-parameter order**, or named with
``arg<"name">(…)`` targeting the node's ``In<Name, ...>`` / ``Scalar<Name, ...>``
selectors. A time-series input is a ``std::vector<std::optional<T>>``; a scalar
input is the value itself. Value types are inferred from the node's signature, and
**every scalar value type** is supported (``bool``, the integer widths,
``float``/``double``, ``std::string``, …) — the harness plumbs values type-erased,
so it is generic over the value type.

A node configured by a **scalar** takes the scalar after its inputs:

.. code-block:: cpp

   struct Shift { static void eval(In<"in", TS<Int>> in, Scalar<"delta", Int> d, Out<TS<Int>> o)
                  { o.set(in.value() + d.value()); } };

   CHECK_OUTPUT(testing::eval_node<Shift>({Int{1}, Int{2}, Int{3}}, Int{5}),
                {Int{6}, Int{7}, Int{8}});
   CHECK_OUTPUT(testing::eval_node<Shift>(arg<"in">(values<Int>(1, 2, 3)),
                                          arg<"delta">(Int{5})),
                values<Int>(6, 7, 8));
   CHECK_OUTPUT(testing::eval_node<Shift>(arg<"delta">(Int{5}),
                                          arg<"in">(values<Int>(1, 2, 3))),
                values<Int>(6, 7, 8));

A node with **multiple time-series inputs** takes one sequence per input. The first
input may be a braced list (its type is inferred); later inputs are passed as
``std::vector<std::optional<T>>``:

.. code-block:: cpp

   struct Sum { static void eval(In<"lhs", TS<Int>> a, In<"rhs", TS<Int>> b, Out<TS<Int>> o)
                { o.set(a.value() + b.value()); } };

   std::vector<std::optional<Int>> rhs{10, 20, 30};
   CHECK_OUTPUT(testing::eval_node<Sum>({Int{1}, none, Int{3}}, rhs),
                {Int{11}, Int{21}, Int{33}});  // lhs persists at cycle 1

The first parameter must be a time-series input, and the node must have exactly one
output. ``eval_node`` drives scalar ``TS<T>``, ``SIGNAL``, ``TSS<T>``,
``TSD<K,V>``, ``TSB<...>``, ``TSW<T,Period,MinPeriod>``, and ``TSL<C, N>`` inputs
and outputs. ``TSL`` is recursive in any supported non-``REF`` child ``C``. ``TS<T>`` and
tick-count ``TSW<T,...>`` exchange a bare ``T`` per cycle, ``SIGNAL`` exchanges
``bool`` ticks, and collection inputs/outputs exchange the per-cycle **delta as a
canonical type-erased ``Value``** built by helpers such as ``set_delta`` /
``list_delta`` / ``dict_delta`` / ``tsb_delta`` (see the collection sections
below).

.. _ts-harness:

How the harness dispatches per schema
.....................................

``eval_node`` itself is schema-agnostic: for each input and the output it consults a
``ts_harness<S>`` adapter (in ``<hgraph/lib/testing/eval_node.h>``) that names the
per-cycle **harness element** — ``T`` for ``TS<T>`` and tick-count ``TSW<T,...>``,
``bool`` for ``SIGNAL``, and the canonical delta ``Value`` for a collection
(``TSS`` / ``TSD`` / ``TSL`` / …) — and wires the (single, erased) ``replay`` source
/ ``record`` sink and seeds/reads the cycle-aligned buffer.
Containers flow as canonical ``Value``\ s end to end: ``record`` captures the per-tick
delta via the runtime ``capture_delta``, ``replay`` re-creates ticks via
``apply_delta``, and ``CHECK_OUTPUT`` compares with ``Value::equals``
(order-independent) and renders with ``to_string``. The first input's element type is
what the (braced) first argument
holds; the output's element type is what the returned ``std::vector<std::optional<…>>``
holds. Supporting a new replayable time-series kind is a localised change — a kind
implementation wires the runtime delta hooks on its ``TSDataOps`` table;
``eval_node`` and the erased ``replay`` / ``record`` do not change.

Comparing results: ``CHECK_OUTPUT``
....................................

``CHECK_OUTPUT(actual, {expected...})`` (from
``<hgraph/lib/testing/check_output.h>``) compares an ``eval_node`` result against an
expected per-cycle sequence and, on mismatch, reports a readable delta — Catch2's
default ``==`` stringifies ``std::optional`` as the unhelpful ``{?}``. ``none`` is
the shorthand for "no tick" (bring it in with ``using namespace hgraph::testing;``).

.. code-block:: text

   output mismatch (4 elements):
     actual:   [1, 2, none, 3]
     expected: [1, 5, none, 9]
     > index 1: actual = 2, expected = 5
     > index 3: actual = 3, expected = 9

``CHECK_OUTPUT`` is non-fatal (continues the test); ``REQUIRE_OUTPUT`` aborts on
mismatch. The expected argument is a braced list (element type inferred from
``actual``, so values and ``none`` mix freely) or any existing
``std::vector<std::optional<T>>``. (This is a test-only header — it depends on
Catch2 and is not part of the ``hgraph_core`` library.)

For fixtures that are clearer without repeated typed braces,
``testing::values<T>(...)`` builds the ``std::vector<std::optional<T>>`` form
directly:

.. code-block:: cpp

   auto input = testing::values<Int>(1, none, 3);
   CHECK_OUTPUT(testing::eval_node<AddOne>(input), {2, none, 4});

Evaluating operators
....................

An :doc:`operator <../../developer_guide/operators>` (e.g. ``stdlib::add_``) is not a
node — it is a name standing for a family of implementations, resolved **at wiring
time** from the supplied argument types. ``eval_node<Op>(...)`` wires the operator, lets
dispatch pick the overload, and returns the result **type-erased** as per-cycle
``Value`` deltas (the output schema is whatever dispatch resolved, so the harness does
not name it in the call). Write the expected with the same ``values<T>(...)`` helper
used for the inputs; ``CHECK_OUTPUT`` boxes it and compares with ``Value`` equality:

.. code-block:: cpp

   stdlib::register_standard_operators();
   // operands and result may all differ; a wiring-time scalar follows the inputs.
   CHECK_OUTPUT(testing::eval_node<stdlib::add_>(values<Int>(1, 2, 3), values<Int>(10, 20, 30)),
                values<Int>(11, 22, 33));
   CHECK_OUTPUT(testing::eval_node<stdlib::div_>(values<Int>(1, 1), values<Int>(2, 0),
                                                 stdlib::DivideByZero::Inf),
                values<Float>(0.5, std::numeric_limits<Float>::infinity()));

Arguments are in the operator's call order: a time-series input is a ``values<T>(...)``
sequence (a scalar leaf ``T`` → ``TS<T>``) or — for any other time-series kind — the
usual canonical-delta ``values<Value>(...)`` sequence, with its time-series schema
supplied as an **explicit template argument** in input order (the same convention as
``eval_node<const_, TSL<…>>``); a scalar input is the value itself — including a
wirable function ``fn<X>()`` for the higher-order operators. **Keyword arguments**
work too: wrap any argument — including a sequence — in ``arg<"name">(…)`` and the
harness wires its replay before dispatching by name
(``eval_node<map_, TSD<…>>(fn<F>(), arg<"rhs">(values<Value>(…)), …)``); the schema
template arguments still follow the sequences in call order:

.. code-block:: cpp

   CHECK_OUTPUT((testing::eval_node<stdlib::reduce_, TSL<TS<Int>, 5>>(
                    fn<stdlib::add_>(),
                    values<Value>(list_delta<TS<Int>>({1, 2, 3, 4, 5})))),
                values<Int>(15));

Scope: the operator must have **at least one time-series input and exactly one
output** — sinks (no output) and sources (no time-series input) use the source-style
overloads / a hand-wired graph. A no-match / ambiguous dispatch raises
``OperatorResolutionError`` from the ``eval_node`` call.

Evaluating graphs
.................

A lightweight **graph** with declared inputs and outputs — a struct whose
``compose(Wiring &, Port..., Scalar...)`` returns an output port — runs through the
same harness: ``eval_node<G>(inputs…)`` wires a ``replay`` per declared ``Port``
parameter, passes scalar values straight through, records the returned output, and
reads it back typed. This is the preferred shape for testing wiring compositions
(operator syntax expressions, ``nested_`` wrappers, …) — no hand-wired
``replay`` / ``record`` plumbing in the test graph. Like static-node tests, graph
arguments may be positional or wrapped in ``arg<"name">(…)``; graph input keywords
target explicit ``NamedPort<"name", S>`` parameters and scalar keywords target
``Scalar<"name", T>`` parameters:

.. code-block:: cpp

   struct SyntaxArithmeticGraph
   {
       static constexpr auto name = "syntax_arithmetic_graph";
       static Port<TS<Int>>  compose(Wiring &, Port<TS<Int>> a, Port<TS<Int>> b)
       {
           using namespace hgraph::stdlib::syntax;
           return (a + b * Int{2}).as<TS<Int>>();
       }
   };

   CHECK_OUTPUT(testing::eval_node<SyntaxArithmeticGraph>(values<Int>(1, 2, 3),
                                                          values<Int>(10, 20, 30)),
                values<Int>(21, 42, 63));

The cycle-aligned buffer
------------------------

A replay/record buffer is a value-layer **mutable** ``List<Any>`` stored in the
``GlobalState`` under a string key. The list is **cycle-aligned**:

* index ``i`` corresponds to evaluation time ``MIN_ST + i * MIN_TD`` (the simulation
  starts at ``MIN_ST`` and steps one tick at a time, matching the Python
  ``SimpleArrayReplaySource``);
* element ``i`` is an ``Any``: an **empty** ``Any`` means *no tick* on that cycle,
  and a **non-empty** ``Any`` wrapping a scalar means *tick with that value*.

Using ``Any`` for the element type is what lets a single list express both "no
value this cycle" and "a value of type ``T``", the same role ``None`` plays in the
Python list-of-values. Because both the input and the output use this one shape,
comparing them is an element-wise list compare.

.. note::

   The buffer convention is anchored at ``MIN_ST`` with a ``MIN_TD`` step. This is
   the ``eval_node`` convention (Python anchors at ``start_time``, defaulting to
   ``MIN_ST``); a configurable start time is a future extension.

``replay``
----------

``replay`` is the source node — a **single erased node** (not templated per schema):
it is authored over a deferred output type (``Out<TsVar<"S">>``) and resolved at
wiring. Supply the output type explicitly, which also gives back a typed port:

* ``wire<stdlib::replay_impl, TS<Int>>(w, key)`` for a scalar source.
* ``wire<stdlib::replay_impl, TSS<Int>>(w, key)`` for a set.
* ``wire<stdlib::replay_impl, TSD<Str, TS<Int>>>(w, key)`` for a dict.
* ``wire<stdlib::replay_impl, TSL<TS<Int>, N>>(w, key)`` for a list.
* ``wire<stdlib::replay_impl, TSW<Int, 3, 1>>(w, key)`` for a tick window.

It initiates itself at start via
``schedule_on_start`` (sources are not scheduled by default), reads its buffer from
the ``GlobalState`` under the ``key`` scalar, ticks once per cycle that has a value
(re-creating the tick via the runtime ``apply_delta``), and reschedules itself (via
``NodeScheduler``) until the buffer is exhausted.

.. code-block:: cpp

   // emits the value at the current cycle, then re-arms for the next
   auto src = wire<stdlib::replay_impl, TS<Int>>(w, Str{"in"});

The ``key`` names the ``GlobalState`` entry holding the input buffer; seed it
before running (``gs.set("in", buffer)``). A cycle whose element is an empty
``Any`` is skipped (the output does not tick that cycle).

``record``
----------

``record`` is the sink node — the dual of ``replay``, likewise a **single erased
node** over a deferred input type (``In<"ts", TsVar<"S">>``); its type resolves from
the connected port, so it is wired without a type argument:
``wire<stdlib::dense_record_impl>(w, port, key)``. On ``start`` it creates a fresh
cycle-aligned ``List<Any>`` in the ``GlobalState`` under its ``key``; on each
evaluation where the input ticks it captures the per-tick **delta** (via the runtime
``capture_delta`` — the per-tick event, not the cumulative ``value``; they coincide
for scalar time-series but differ for compound types) at the current cycle offset
(padding any skipped cycles with empty ``Any`` entries). After the run the buffer is
the recorded output, readable from the ``GlobalState``.

.. code-block:: cpp

   wire<stdlib::dense_record_impl>(w, inc, Str{"out"});  // (input port, key)

Worked example
--------------

Wiring ``replay → add_one → record`` and reading the result back:

.. code-block:: cpp

   struct AddOne
   {
       static constexpr auto name = "add_one";
       static void           eval(In<"in", TS<Int>> in, Out<TS<Int>> out) { out.set(in.value() + 1); }
   };

   struct ReplayRecordGraph
   {
       static constexpr auto name = "replay_record_graph";
       static void           compose(Wiring &w)
       {
           auto src = wire<stdlib::replay_impl, TS<Int>>(w, Str{"in"});
           auto inc = wire<AddOne>(w, src);
           wire<stdlib::dense_record_impl>(w, inc, Str{"out"});
       }
   };

   // Seed the input [1, <skip>, 3], run, and read back [2, <skip>, 4].
   GraphBuilder gb = build_graph<ReplayRecordGraph>();
   testing::set_replay_values<Int>(gb.global_state(), "in", {Int{1}, std::nullopt, Int{3}});
   /* ... run an executor over [MIN_ST, MIN_ST + n*MIN_TD) ... */
   auto out = testing::get_recorded_values<Int>(executor.view().graph().global_state(), "out");
   // out == { 2, std::nullopt, 4 }

``set_replay_values`` / ``get_recorded_values`` are convenience helpers that build
a cycle-aligned ``List<Any>`` from a ``std::vector<std::optional<T>>`` and read one
back, so tests deal in ordinary C++ vectors rather than value-layer containers.

Standard helpers (``lib/std``)
------------------------------

A small set of reusable nodes lives in ``<hgraph/lib/std/std_nodes.h>`` (namespace
``hgraph::stdlib``) — the building blocks graphs and tests reach for most:

* ``const_`` — a constant source: emits its ``value`` scalar once at start
  (named with a trailing underscore because ``const`` is a C++ keyword; the Python
  operator is ``const``). Without an explicit output schema it resolves to
  ``TS<T>`` from the scalar value type; pass ``wire<stdlib::const_, S>(...)`` to
  emit a constant value for another time-series shape ``S``.
* ``debug_print<T>`` — a sink that prints ``label: value`` on each tick (a
  diagnostic aid; ``T`` must be ``fmt``-formattable).
* ``null_sink<T>`` — a sink that consumes its input and does nothing (gives a graph
  a terminal sink without side effects).

.. code-block:: cpp

   using namespace hgraph::literals;

   auto c = wire<stdlib::const_>(w, 7_i);              // source emitting 7 at start
   wire<stdlib::debug_print>(w, c, "value"_str);       // prints "value: 7"

Standard scalar aliases and constructors live in
``<hgraph/types/primitive_types.h>``: ``Bool``/``Int``/``Float``/``Str`` map to
Python's ``bool``/``int``/``float``/``str``, and ``bool_``/``int_``/``float_``/
``str_`` construct those exact C++ types from ordinary inputs. The optional
user-defined literals are deliberately kept in ``hgraph::literals``; opt in with
``using namespace hgraph::literals;`` when you want ``7_i``, ``2.5_f`` or
``"name"_str``.

The ``hgraph::stdlib`` namespace also has small value-layer construction helpers in
``<hgraph/lib/std/value_util.h>``. They wrap the standard compact container
builders for scalar element types:

.. code-block:: cpp

   using namespace hgraph::literals;

   Value one   = stdlib::value<Int>(1);
   Value set   = stdlib::make_set<Int>({1_i, 2_i});
   Value list  = stdlib::make_list<Int>({1_i, 2_i, 3_i});
   Value map   = stdlib::make_map<Str, Int>({{"a"_str, 1_i}, {"b"_str, 2_i}});
   Value queue = stdlib::make_queue<Int>({1_i, 2_i, 3_i});

``TypeRegistry::instance()`` automatically registers the common scalar
vocabulary and pre-interns matching ``TS[...]`` / ``TSS[...]`` schemas. The
idempotent utility in ``<hgraph/lib/std/standard_types.h>`` is available when
code needs to seed an explicit registry reference or re-seed after a test-only
registry reset:

.. code-block:: cpp

   stdlib::register_standard_types();

The standard vocabulary is ``bool`` (``Bool``), ``int`` (``Int`` /
``std::int64_t``), ``float`` (``Float`` / ``double``), ``date`` (``Date``),
``datetime`` (``DateTime``), ``timedelta`` (``TimeDelta``), and ``str``
(``Str`` / ``std::string``), plus explicit aliases such as
``int8``/``int16``/``int32``/``int64`` and
``uint8``/``uint16``/``uint32``/``uint64``. In C++ static schema syntax, use
``TS<Int>`` for the standard hgraph ``TS[int]``; ``TS<std::int32_t>`` is the
explicit C++ ``int32`` storage type.

Deltas are canonical ``Value``\ s
---------------------------------

A collection time-series ticks a **delta** each cycle, and that delta is the
**canonical type-erased** ``Value`` whose schema is the runtime
``delta_value_schema``: ``Bundle{added: Set<T>, removed: Set<T>}`` for ``TSS<T>``,
``Bundle{removed: Set<K>, modified: Map<K, delta(V)>}`` for ``TSD<K,V>``, and
``Map<int, delta(C)>`` for ``TSL<C, N>`` (recursive in ``C``). ``TSB<...>`` uses
``Bundle{field: delta(field_schema)...}``; ``std::nullopt`` in ``tsb_delta``
leaves the field at its canonical default delta, typed-null for scalar children
and empty for collection children. Tick-count ``TSW<T,...>`` and ``SIGNAL`` have
scalar deltas, so the harness exchanges plain ``T`` / ``bool`` elements for
those shapes. Tests build collection deltas with
**recursive builder functions** that *produce a standard* ``Value`` *matching the
type-erased signature* (there is no parallel wrapper type — comparison is
``Value::equals``, display is ``to_string``):

.. code-block:: cpp

   set_delta<Int>({Int{1}, Int{2}}, {})                       // -> Bundle{added:{1,2}, removed:{}}
   list_delta<TS<Int>>({{0, Int{10}}, {2, Int{30}}})          // -> Map<int, int>   (sparse: idx 0->10, 2->30)
   list_delta<TS<Int>>({Int{10}, none, Int{30}})              // positional: position is the index, none = skip
   list_delta<TSS<Int>>({{0, set_delta<Int>({Int{1}}, {})}})  // -> Map<int, Bundle>   (TSL of sets)
   list_delta<TSL<TS<Int>,2>>({{0, list_delta<TS<Int>>({{0, Int{1}}})}})   // nested, recursive
   dict_delta<Str, TS<Int>>({{Str{"a"}, Int{1}}}, {Str{"b"}})   // remove b, modify a
   using Quote = TSB<"Quote", Field<"bid", TS<Int>>, Field<"ask", TS<Int>>>;
   tsb_delta<Quote>(Int{101}, std::nullopt)               // bid ticked, ask is typed-null

Signal time-series (``SIGNAL``)
-------------------------------

Author with ``In<Name, SIGNAL>`` (``ticked()``) and ``Out<SIGNAL>`` (``tick()``).
The test harness exchanges ``bool`` elements: ``true`` means "tick this cycle" and
``none`` means "no tick".
In normal graph wiring, ``In<Name, SIGNAL>`` can bind to any time-series output;
the signal input observes whether that upstream output ticked, independent of its
value schema.

.. code-block:: cpp

   struct MirrorSignal { static void eval(In<"s", SIGNAL> s, Out<SIGNAL> out)
                         { if (s.ticked()) out.tick(); } };

   CHECK_OUTPUT(testing::eval_node<MirrorSignal>({true, none, true}), {true, none, true});

Set time-series (``TSS``)
-------------------------

Author with ``In<Name, TSS<T>>`` (``size``/``contains``/``values``/``added``/
``removed``; ``delta()`` is the canonical ``Bundle`` ``ValueView``) and ``Out<TSS<T>>``
(``add``/``remove``/``clear``; the delta accumulates within a cycle). The simplest test
path is :ref:`eval_node <eval-node>`: a ``TSS`` input/output exchanges the per-cycle
delta ``Value`` from ``set_delta``.

.. code-block:: cpp

   struct MirrorSet { static void eval(In<"s", TSS<Int>> s, Out<TSS<Int>> out)
                      { for (Int r : s.removed()) out.remove(r); for (Int a : s.added()) out.add(a); } };

   const std::vector<std::optional<Value>> deltas{
       set_delta<Int>({Int{1}, Int{2}}, {}),         // add 1,2
       set_delta<Int>({Int{3}}, {Int{1}}),           // add 3, remove 1
       set_delta<Int>({}, {Int{2}, Int{3}}),         // remove 2,3
   };
   CHECK_OUTPUT(testing::eval_node<MirrorSet>(deltas), deltas);   // round-trips the deltas

By hand: ``wire<stdlib::replay_impl, TSS<T>>`` re-creates ticks from a delta ``Value``
(remove then add); ``wire<stdlib::dense_record_impl>`` captures the per-tick delta; ``CHECK_OUTPUT`` renders each delta as
``{added: {…}, removed: {…}}`` on mismatch. A constant set source is
``wire<stdlib::const_, TSS<T>>(w, stdlib::make_set<T>({values...}))``.

List time-series (``TSL``) — recursive
--------------------------------------

A fixed-size ``TSL<C, N>`` is ``N`` child time-series whose schema ``C`` is any
supported non-``REF`` time-series — ``TS`` / ``SIGNAL`` / ``TSS`` / ``TSD`` /
fixed or dynamic ``TSL`` / ``TSB`` / ``TSW`` — nested arbitrarily. Author with
``In<Name, TSL<C, N>>`` (``size``, ``operator[](i) -> In<"", C>``,
``modified_items``; ``delta()`` is the canonical ``Map<int, delta(C)>``
``ValueView``) and ``Out<TSL<C, N>>`` (``operator[](i) -> Out<C>``; ``set(i, v)``
is a scalar-child convenience). Children compose recursively — ``out[i]`` is an
``Out<C>``, so a TSL of sets writes via ``out[i].add(...)`` and a TSL of TSL via
``out[i][j].set(...)``.

The simplest test path is :ref:`eval_node <eval-node>`: a ``TSL`` input/output exchanges
the per-cycle delta ``Value`` from ``list_delta`` (recursive in ``C``).

.. code-block:: cpp

   // TSL of scalars
   struct MirrorList { static void eval(In<"l", TSL<TS<Int>, 2>> l, Out<TSL<TS<Int>, 2>> out)
                       { for (std::size_t i = 0; i < l.size(); ++i) if (l[i].modified()) out.set(i, l[i].value()); } };

   const std::vector<std::optional<Value>> deltas{
       list_delta<TS<Int>>({{0, Int{1}}, {1, Int{2}}}),   // both children tick
       list_delta<TS<Int>>({{0, Int{5}}}),                // only child 0 ticks
   };
   CHECK_OUTPUT(testing::eval_node<MirrorList>(deltas), deltas);

The delta builders are **fully recursive** — a TSL-of-sets delta is a ``Map`` whose values
are themselves ``set_delta`` ``Value``\ s, a TSL-of-TSL delta a ``Map`` of ``Map``\ s — and
``Value::equals`` is order-independent at every level (map keys, set elements):

.. code-block:: cpp

   // TSL<TSS<Int>> delta: Map<int, Bundle{added:Set, removed:Set}>
   const Value sd = list_delta<TSS<Int>>({{0, set_delta<Int>({Int{1}, Int{2}}, {})}, {1, set_delta<Int>({Int{9}}, {})}});
   CHECK(sd.equals(list_delta<TSS<Int>>({{1, set_delta<Int>({Int{9}}, {})}, {0, set_delta<Int>({Int{2}, Int{1}}, {})}})));

   // TSL<TSL<TS<Int>,2>> delta: Map<int, Map<int, int>>
   const Value nd = list_delta<TSL<TS<Int>, 2>>({{0, list_delta<TS<Int>>({{0, Int{7}}, {1, Int{8}}})}});

By hand: ``wire<stdlib::replay_impl, TSL<C, N>>`` re-creates ticks by recursively applying
each ``index -> child_delta`` of the buffered delta to the matching child output;
``wire<stdlib::dense_record_impl>`` captures the per-tick delta (only the children that ticked);
``CHECK_OUTPUT`` renders each delta as the map ``{0: 1, 1: 10}`` on mismatch.

.. note::

   The **authoring and delta layers are recursive over any child schema** today —
   ``In``/``Out<TSL<C, N>>`` compose for any ``C`` and ``list_delta``/``set_delta`` build
   the nested canonical ``Value`` for replayable child kinds. TSData storage for fixed
   ``TSL`` now covers the implemented non-``REF`` child kinds: ``TS``, ``SIGNAL``,
   ``TSS``, ``TSD``, fixed and dynamic ``TSL``, ``TSB``, and ``TSW``. Dynamic
   (``N == 0``) ``TSL`` storage is grow-only; tests should not expect shorter-list
   copies to produce a removal delta until ``TSL`` has a structural removal surface.

Dict time-series (``TSD``)
--------------------------

Author with ``In<Name, TSD<K,V>>`` (typed ``contains`` / ``find_slot`` /
``operator[]`` plus ``valid_items`` / ``modified_items`` / ``added_items`` /
``removed_items``) and ``Out<TSD<K,V>>`` (``out[key] -> Out<V>``; ``set(key, v)``
for scalar children). The simplest replay/record path exchanges the canonical
``Bundle{removed: Set<K>, modified: Map<K, delta(V)>}`` ``Value`` from
``dict_delta``.

.. code-block:: cpp

   using IntDict = TSD<Str, TS<Int>>;

   struct MirrorDict
   {
       static void eval(In<"d", IntDict> d, Out<IntDict> out)
       {
           Value delta = capture_delta(d.base());
           apply_delta(out.base(), delta.view());
       }
   };

   const std::vector<std::optional<Value>> deltas{
       dict_delta<Str, TS<Int>>({{Str{"a"}, Int{1}}, {Str{"b"}, Int{2}}}),
       dict_delta<Str, TS<Int>>({{Str{"a"}, Int{5}}}, {Str{"b"}}),
   };
   CHECK_OUTPUT(testing::eval_node<MirrorDict>(deltas), deltas);

Window time-series (``TSW``)
----------------------------

Tick-count ``TSW<T, Period, MinPeriod>`` windows replay and record their per-tick
push value. Author with ``In<Name, TSW<T,...>>`` (``size`` / ``operator[]`` /
``front`` / ``back``) and ``Out<TSW<T,...>>`` (``push`` / ``apply``). The test
harness exchanges plain ``T`` elements.

.. code-block:: cpp

   struct MirrorWindow
   {
       static void eval(In<"w", TSW<Int, 3, 1>> w, Out<TSW<Int, 3, 1>> out)
       {
           if (w.modified()) { out.apply(w.delta()); }
       }
   };

   CHECK_OUTPUT(testing::eval_node<MirrorWindow>({Int{1}, none, Int{3}, Int{4}}),
                {Int{1}, none, Int{3}, Int{4}});
