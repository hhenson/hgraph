Wiring Graphs in C++
====================

A *graph* composes nodes — and other graphs — into a runnable program. In C++ a
graph is a small ``struct`` with a static ``compose`` method: the graph counterpart
of authoring a node (see *Authoring Nodes in C++*). It mirrors a Python
``@graph``: a signature of time-series inputs and outputs whose body wires the
sub-components together.

Wiring runs **at wiring time, not at run time** — ``compose`` executes once to build
the graph, never during evaluation. Graphs also **flatten**: composing graphs
inlines their nodes, and only nodes exist at run time.

.. note::

   **Status.** The core is **implemented**: ``Wiring``, typed ``Port<Schema>``,
   ``wire<T>`` for nodes (including **scalar arguments** to a wired node),
   ``wire<G>`` sub-graph composition (graphs flatten), and ``build_graph<G>(…)`` for
   a top-level graph — including **graph-level scalar parameters** (a top-level
   graph's ``compose`` may take ``Scalar<>`` parameters, supplied through
   ``build_graph<G>(values…)``). Standalone sub-graph building / time-series
   boundary inputs, the higher-order operators (``map_`` / ``reduce`` /
   ``switch_`` / ``mesh_``), ``feedback``, and ``try_except_`` error handling
   have all since landed; see *What's planned* below for what remains (generic
   graphs are the main gap). A graph's body method is named ``compose`` and
   the wiring verb is ``wire`` — distinct names, so inside a ``compose`` body you
   call ``wire<…>`` directly. How it works internally, and how it is shared with
   Python wiring, is in *Developer Guide > Graph Wiring*.


A first graph
-------------

A top-level graph has **no time-series inputs or outputs** — its sources generate
data and its sinks consume it. It *may*, however, take **scalar** inputs: plain
(non-time-series) configuration values that parameterise the graph at wiring time.
Graph-level scalar inputs appear as extra ``compose`` parameters and are supplied
to ``build_graph`` — e.g.
``static void compose(Wiring &w, Scalar<"window", Int> window)`` built with
``build_graph<MyGraph>(Int{20})`` (the typed value is wrapped into the
``window`` parameter). The simplest graph takes no scalars; its ``compose`` body
just adds nodes:

The standard aliases ``Bool``/``Int``/``Float``/``Str`` map to the hgraph/Python
scalar vocabulary. For shorter wiring call sites, opt into
``hgraph::literals`` and use literals such as ``20_i``, ``2.0_f`` and
``"out"_str``; they are not imported by default.

.. code-block:: cpp

   struct PriceGraph
   {
       static constexpr auto name = "price_graph";

       static void compose(Wiring &w)
       {
           auto a = wire<ConstantSource>(w);
           auto b = wire<ConstantSource>(w);
           wire<Print>(w, wire<Sum>(w, a, b));
       }
   };

   GraphBuilder g = build_graph<PriceGraph>();   // runs compose() once, at wiring time

A graph that takes a scalar parameter is built by supplying the value:

.. code-block:: cpp

   struct ScaledPriceGraph
   {
       static constexpr auto name = "scaled_price_graph";

       static void compose(Wiring &w, Scalar<"factor", Float> factor)
       {
           auto px = wire<ConstantSource>(w);
           wire<Print>(w, wire<Scale>(w, px, factor));   // graph scalar forwarded to node scalar
       }
   };

   using namespace hgraph::literals;

   GraphBuilder g = build_graph<ScaledPriceGraph>(2.0_f);   // factor = 2.0

A scalar received as a parameter (here ``factor``) can be passed **straight on** to
a node's or sub-graph's scalar — the wiring layer unpacks it for you, so an explicit
``factor.value()`` is not required (it still works if you prefer it). The parameter
names need not match; only the value type must.

.. code-block:: python

   @graph
   def price_graph():
       a, b = constant_source(), constant_source()
       print_(sum_(a, b))

``wire<T>(w, ports...)`` adds node ``T`` to the graph ``w`` and returns a handle
(a *port*) to its output; passing ports as the inputs of another ``wire<T>``
connects them. You never write node indices or edges by hand, and you never have
to add nodes in any particular order — see *Ordering is automatic*.


Graph scalar parameters
-----------------------

A top-level graph's ``Scalar<>`` parameters are its **build parameters** — supplied
when you build the graph, alongside (conceptually) the run window you later give the
executor. They are consumed at **wiring time**: ``build_graph<G>(values…)`` runs
``compose`` with the supplied values, so the values must be known before the
``GraphBuilder`` is produced (you cannot defer them to the executor, which receives
an already-built graph).

Pass one value per ``Scalar<>`` parameter, **in ``compose``-parameter order**:

.. code-block:: cpp

   struct ReportGraph
   {
       static constexpr auto name = "report_graph";
       //                                   two scalar build parameters
       static void compose(Wiring &w, Scalar<"window", Int> window, Scalar<"factor", Float> factor)
       {
           auto px = wire<ConstantSource>(w);
           wire<Print>(w, wire<RollingMean>(w, px, window, factor));
       }
   };

   // window = 20, factor = 1.5 — positional, in compose order.
   using namespace hgraph::literals;

   GraphBuilder g = build_graph<ReportGraph>(20_i, 1.5_f);

   // Or by Scalar<> name, so call order does not matter.
   GraphBuilder named = build_graph<ReportGraph>(arg<"factor">(1.5_f),
                                                 arg<"window">(20_i));

The arguments are checked at compile time: the count must match the graph's
``Scalar<>`` parameters and each value must be convertible to its parameter's type.
Named arguments are matched to the ``Scalar<Name, T>`` parameter name; positional
arguments must come before named arguments.
A graph with no scalar parameters is simply ``build_graph<G>()``. The resulting
``GraphBuilder`` is handed to a ``GraphExecutor`` exactly as for a scalar-free graph
(see *Running a graph*).

Scalar parameters may have defaults. Add ``static defaults()`` to the node or graph
type, returning a tuple of named defaults:

.. code-block:: cpp

   static auto defaults()
   {
       return std::tuple{arg<"window">(20_i), arg<"factor">(1.5_f)};
   }

For an external type, specialize ``signature_defaults<T>`` with a ``values()``
method returning the same tuple shape.


Ports
-----

A ``Port<TS<Int>>`` is a **wiring-time handle** to a node's output — not a value.
You obtain one from ``wire<T>(...)`` and pass it as an input to another node:

.. code-block:: cpp

   Port<TS<Int>> a = wire<ConstantSource>(w);   // handle to the source's output
   Port<TS<Int>> s = wire<Sum>(w, a, a);        // a is fed into both inputs of Sum

Ports are typed, so passing the wrong port type — or the wrong number of inputs —
to ``wire<T>`` is a **compile error**. (Python catches the same mistakes, but only
when the graph is wired at run time.)

``SIGNAL`` is the exception on the input side: a ``Port<TS<Int>>``,
``Port<TSD<...>>`` or any other time-series output port may be passed to a node or
sub-graph input declared as ``SIGNAL``. The input observes the upstream tick rather
than the upstream value.

For standard operators, a compose body may opt into expression syntax:

.. code-block:: cpp

   using namespace hgraph::stdlib::syntax;

   auto a = wire<stdlib::replay_impl, TS<Int>>(w, "a");
   auto b = wire<stdlib::replay_impl, TS<Int>>(w, "b");
   auto c = (a + b * Int{2}).as<TS<Int>>();

The C++ operators are only syntax for the standard operator registry
(``+`` -> ``stdlib::add_``, ``*`` -> ``stdlib::mul_``, comparisons -> ``TS<Bool>``, and so on).
They return an erased ``Port<void>`` because overload resolution can change the result
type, such as ``int / int -> float``. Use ``.as<Schema>()`` when a graph return or
downstream API needs a typed ``Port<Schema>``.


Packing node collection inputs
------------------------------

``wire<Node>`` can pack call-site ports into one tail collection input, mirroring
the Python ``*args`` / ``**kwargs`` convenience. This is node wiring sugar only;
graph and operator composition should use their normal variadic, reduce, and
explicit collection APIs. The node opts into this conversion by declaring
``Args`` or ``Kwargs``; plain ``TSL`` and ``TSB`` inputs keep their ordinary
whole-input binding behavior.

For an ``Args`` input, surplus positional ports are collected into a runtime
``TSL``. ``Args<>`` defaults the element schema to ``TsVar<"args">`` and uses
the internal size variable ``SIZE<"args_len">``:

.. code-block:: cpp

   struct SumTail
   {
       static void eval(In<"base", TS<Int>> base,
                        In<"values", Args<>> values,
                        Scalar<"offset", Int> offset,
                        Out<TS<Int>> out);
   };

   auto out = wire<SumTail>(w, base, lhs, rhs, arg<"offset">(5_i));
   // Equivalent collection input: values = TSL{lhs, rhs}; args_len = 2.

Use ``Args<TS<Int>>`` when the node should restrict all positional arguments to
one element schema.

For a ``Kwargs`` input, ``Kwargs<>`` defaults the field-pack variable to
``TsVar<"kwargs">`` and resolves as an unnamed runtime ``TSB``. Unknown keyword
ports become fields on the bundle. Mixed positional ports are also accepted; they
become fields named ``_1``, ``_2``, and so on:

.. code-block:: cpp

   struct AddKwargs
   {
       static void eval(In<"values", Kwargs<>> values,
                        Out<TS<Int>> out)
       {
           auto lhs = values.field("lhs");
           auto rhs = values.field("rhs");
           out.set(lhs.value().checked_as<Int>() + rhs.value().checked_as<Int>());
       }
   };

   auto by_name = wire<AddKwargs>(w, arg<"lhs">(lhs), arg<"rhs">(rhs));

   struct FormatPair
   {
       static void eval(In<"values", Kwargs<>> values,
                        Out<TS<Str>> out);
   };

   auto numbered = wire<FormatPair>(w, count, label);   // _1=count, _2=label

Use a plain ``TSL`` or ``TSB`` input when the call site should supply the whole
collection explicitly. A bare ``In<"value", TsVar<"A">>`` remains an ordinary
generic input and is not treated as a collection pack target. If a scalar
parameter follows a packed collection in the node signature, supply that scalar
by name.


Supported standard operator overloads
-------------------------------------

Include ``<hgraph/lib/std/std_operators.h>`` and call
``stdlib::register_standard_operators()`` before wiring graphs that use these operators.
The tables below list the overloads currently registered by that call. Operator markers
outside this table may be declared for catalogue completeness, but they are not wired
until a concrete implementation is registered.

.. note::

   This section is the **single authoritative overload catalogue**. Update it in
   the same change as the registrations in
   ``include/hgraph/lib/std/operators/impl/*.h`` (the developer guide's
   *Operators* page records the framework design and intentionally does not
   duplicate this list).

Notation:

- ``Int`` is the standard hgraph integer scalar (``std::int64_t``), ``Float`` is
  ``double``, ``Str`` is ``std::string``.
- ``Date``, ``DateTime``, and ``TimeDelta`` are the standard hgraph date/time
  scalar aliases.
- Every operand and result below is a time-series unless stated otherwise:
  ``Int + Float -> Float`` means ``TS<Int> + TS<Float> -> TS<Float>``.
- A plain scalar argument may be supplied where an operator expects ``TS<T>``; wiring
  promotes it through an internal const source, so ``a + Int{2}`` is valid.

Arithmetic
~~~~~~~~~~

.. list-table::
   :header-rows: 1

   * - Operator
     - Registered overloads
     - Result
     - Notes
   * - ``add_`` / ``+``
     - ``Int + Int``; ``Float + Float``; ``Int + Float``; ``Float + Int``;
       ``Str + Str``; ``TimeDelta + TimeDelta``; ``DateTime + TimeDelta``;
       ``TimeDelta + DateTime``; ``Date + TimeDelta``
     - same as operands for homogeneous cases; ``Float`` for mixed numeric;
       ``DateTime`` for ``DateTime``/``TimeDelta``; ``Date`` for ``Date``/``TimeDelta``
     - ``Date + TimeDelta`` advances by whole days.
   * - ``sub_`` / ``-``
     - ``Int - Int``; ``Float - Float``; ``Int - Float``; ``Float - Int``;
       ``TimeDelta - TimeDelta``; ``DateTime - TimeDelta``;
       ``DateTime - DateTime``; ``Date - Date``
     - same as operands for homogeneous numeric/``TimeDelta``; ``Float`` for mixed numeric;
       ``DateTime`` for ``DateTime`` minus ``TimeDelta``; ``TimeDelta`` for ``DateTime``/``Date``
       differences
     - ``Date - Date`` returns a whole-day ``TimeDelta``.
   * - ``mul_`` / ``*``
     - ``Int * Int``; ``Float * Float``; ``Int * Float``; ``Float * Int``;
       ``Str * Int``; ``Int * Str``
     - ``Int``; ``Float`` for mixed/float numeric; ``Str`` for string repetition
     - Repeating a string zero or fewer times returns an empty string.
   * - ``div_`` / ``/``
     - ``Int / Int``; ``Float / Float``; ``Int / Float``; ``Float / Int``;
       ``TimeDelta / TimeDelta``
     - ``Float``
     - Numeric overloads have an optional ``DivideByZero`` scalar policy. Duration
       division does not currently take that policy.
   * - ``floordiv_`` / ``floordiv(lhs, rhs)``
     - ``Int // Int``; ``Float // Float``; ``Int // Float``; ``Float // Int``
     - ``Int`` for ``Int // Int``; otherwise ``Float``
     - Uses Python-style floor semantics, not C++ truncation. Optional
       ``DivideByZero`` policy.
   * - ``mod_`` / ``%``
     - ``Int % Int``; ``Float % Float``; ``Int % Float``; ``Float % Int``
     - ``Int`` for ``Int % Int``; otherwise ``Float``
     - Uses Python-style modulo semantics. Optional ``DivideByZero`` policy.
   * - ``pow_`` / ``pow(lhs, rhs)``
     - ``Int ** Int``; ``Float ** Float``; ``Int ** Float``; ``Float ** Int``
     - ``Int`` for ``Int ** Int``; otherwise ``Float``
     - Integer power requires a non-negative exponent. Optional ``DivideByZero``
       policy for ``0 ** negative``; ``NoTick`` remains valid for integer power.
   * - ``neg_`` / unary ``-``
     - ``Int``; ``Float``; ``TimeDelta``
     - same type
     -
   * - ``pos_`` / unary ``+``
     - ``Int``; ``Float``; ``TimeDelta``
     - same type
     -
   * - ``abs_`` / ``abs(ts)``
     - ``Int``; ``Float``; ``TimeDelta``
     - same type
     -
   * - ``sign`` / ``sign(ts)``
     - ``Int``; ``Float``
     - same type
     - Returns ``-1``, ``0`` or ``1``.
   * - ``ln`` / ``ln(ts)``
     - ``Float``
     - ``Float``
     -

``DivideByZero`` policies are wiring-time scalar arguments. The two-argument form of
``div_`` / ``floordiv_`` / ``mod_`` / ``pow_`` defaults to ``DivideByZero::Error``; the
three-argument form accepts a policy value. Numeric ``div_`` accepts all policies
(``Error``, ``Nan``, ``Inf``, ``NoTick``, ``Zero``, ``One``). ``floordiv_`` accepts the
same policies for float/mixed numeric overloads; ``Int // Int`` accepts ``Error``,
``NoTick``, ``Zero`` and ``One``. ``mod_`` accepts ``Nan`` / ``Inf`` / ``NoTick`` for
float/mixed numeric overloads, while ``Int % Int`` only treats ``NoTick`` specially;
the other zero-divisor cases throw. ``pow_`` applies the policy only for
``0 ** negative``.

Comparison
~~~~~~~~~~

.. list-table::
   :header-rows: 1

   * - Operator
     - Registered overloads
     - Result
     - Notes
   * - ``eq_`` / ``==``
     - same-type ``Bool``, ``Int``, ``Float``, ``Str``, ``Date``, ``DateTime``,
       ``TimeDelta``; mixed ``Int``/``Float``
     - ``Bool``
     -
   * - ``ne_`` / ``!=``
     - same-type ``Bool``, ``Int``, ``Float``, ``Str``, ``Date``, ``DateTime``,
       ``TimeDelta``; mixed ``Int``/``Float``
     - ``Bool``
     -
   * - ``lt_`` / ``<``; ``le_`` / ``<=``; ``gt_`` / ``>``; ``ge_`` / ``>=``
     - same-type ``Int``, ``Float``, ``Str``, ``Date``, ``DateTime``, ``TimeDelta``;
       mixed ``Int``/``Float``
     - ``Bool``
     - ``Bool`` ordering is not registered.
   * - ``cmp_`` / ``cmp(lhs, rhs)``
     - same-type ``Int``, ``Float``, ``Str``, ``Date``, ``DateTime``, ``TimeDelta``;
       mixed ``Int``/``Float``
     - ``CmpResult``
     - Returns ``CmpResult::LT``, ``CmpResult::EQ`` or ``CmpResult::GT``.

Logical and bitwise
~~~~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1

   * - Operator
     - Registered overloads
     - Result
     - Notes
   * - ``not_`` / ``!``
     - ``Bool``; ``Int``; ``Float``; ``Str``
     - ``Bool``
     - Empty string is false; non-empty string is true.
   * - ``and_`` / ``&&``; ``or_`` / ``||``
     - ``Bool``/``Bool``; ``Int``/``Int``; ``Float``/``Float``; ``Str``/``Str``;
       mixed ``Int``/``Float``
     - ``Bool``
     - These are graph operators, so C++ ``&&`` / ``||`` syntax does not short-circuit.
   * - ``invert_`` / ``~``
     - ``Int``
     - ``Int``
     -
   * - ``bit_and`` / ``&``; ``bit_or`` / ``|``; ``bit_xor`` / ``^``
     - ``Int``/``Int``; ``Bool``/``Bool``
     - same type
     - ``Bool`` bitwise overloads use logical bool semantics.
   * - ``lshift_`` / ``<<``; ``rshift_`` / ``>>``
     - ``Int`` shifted by ``Int``
     - ``Int``
     - Shift counts must be non-negative and smaller than the number of value bits.

Flow control
~~~~~~~~~~~~

.. list-table::
   :header-rows: 1

   * - Operator
     - Registered overloads
     - Result
     - Notes
   * - ``merge``
     - variadic ``*ts`` with matching time-series schemas
     - same as inputs
     - Forwards the first input to tick in input order; if the selected input
       invalidates, falls back to the most recent still-valid input.
   * - ``all_``
     - variadic ``*args: TS<Bool>``; ``TSD<K, TS<Bool>>``
     - ``TS<Bool>``
     - Empty variadic calls return ``true``. Non-empty variadic calls are packed
       into a fixed ``TSL`` and wired through the boolean reduction path.
   * - ``any_``
     - variadic ``*args: TS<Bool>``; ``TSD<K, TS<Bool>>``
     - ``TS<Bool>``
     - Empty variadic calls return ``false``. Non-empty variadic calls are packed
       into a fixed ``TSL`` and wired through the boolean reduction path.
   * - ``if_true``
     - ``condition: TS<Bool>``; optional scalar ``tick_once_only``
     - ``TS<Bool>``
     - Ticks ``true`` when ``condition`` ticks true; with ``tick_once_only`` set,
       only the first true tick is emitted.
   * - ``if_then_else``
     - ``condition: TS<Bool>``, ``true_value`` and ``false_value`` with matching schemas
     - same as selected inputs
     - Samples the selected input when the condition or selected input ticks.
   * - ``if_cmp``
     - ``cmp: TS<CmpResult>``, plus matching ``lt`` / ``eq`` / ``gt`` inputs
     - same as branch inputs
     - Selects the branch indicated by ``CmpResult::LT`` / ``EQ`` / ``GT``.

Sources, conversions and sinks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1

   * - Operator
     - Registered overloads
     - Result
     - Notes
   * - ``const_``
     - ``const_(value)``; ``const_(value, delay)``
     - Defaults to ``TS<T>`` for scalar value type ``T``; may be explicitly resolved to
       any output whose current-value schema matches the supplied value.
     - Emits one tick at graph start, or at ``start + delay`` for the delayed overload.
   * - ``zero_``
     - explicit output ``TS<Int>``; ``TS<Float>``; ``TS<Str>``
     - selected output schema
     - Emits ``0``, ``0.0`` or empty string once at graph start.
   * - ``debug_print``
     - ``debug_print(label: Str, ts: any time-series)``
     - sink
     - Prints ``label: value`` on each tick.
   * - ``null_sink``
     - ``null_sink(ts: any time-series)``
     - sink
     - Consumes the input and does nothing.


Configuring a node with scalars
-------------------------------

A node can take **scalar** parameters — read-only, non-time-series configuration
fixed at wiring time (see *Authoring Nodes in C++ > Scalar values and arguments*).
You pass them to ``wire<T>`` positionally in eval-parameter order, or by selector
name with ``arg<"name">(value)``. A call supplies a ``Port`` for each ``In`` and a
plain value for each ``Scalar``.

.. code-block:: cpp

   using namespace hgraph::literals;

   // node: eval(In<"in", TS<Int>> in, Scalar<"delta", Int> delta, Out<TS<Int>> out)
   auto src = wire<ConstantSource>(w);   // 41
   auto out = wire<Shift>(w, src, 5_i);   // port for `in`, then 5 for `delta` -> 46
   auto same = wire<Shift>(w, arg<"delta">(5_i), arg<"in">(src));

The scalar value is checked against the node's ``Scalar<>`` type at compile time,
and it becomes part of the node's wiring identity: two ``wire`` calls dedup only
when their scalars are **also** equal (see *Identical nodes are shared*).


Identical nodes are shared
--------------------------

Wiring the **same node with the same inputs** gives you back the **same** node —
the wiring layer interns nodes (wiring-time common-subexpression elimination). Two
calls are equivalent when the node type matches and every input is equal: ports
are equal when they come from the same producing node, scalar inputs when their
values are equal. So you can re-derive a value freely without creating duplicates:

.. code-block:: cpp

   auto x  = wire<Source>(w);
   auto y1 = wire<AddOne>(w, x);
   auto y2 = wire<AddOne>(w, x);   // same inputs as y1 -> the SAME node; y2 == y1

To get two *distinct* nodes, give them distinct inputs — for a source, a
distinguishing scalar input. (In the first graph above, both
``wire<ConstantSource>(w)`` calls likewise refer to one shared node, since
``ConstantSource`` has no distinguishing inputs.)


Graphs with a signature
------------------------

A sub-graph declares inputs and an output, and composes like a node. The ``compose``
parameters after the context are the graph's inputs; the return is its output:

.. code-block:: cpp

   struct Mid
   {
       static constexpr auto name = "mid";
       // logical signature: (TS<Float>, TS<Float>) -> TS<Float>
       static Port<TS<Float>> compose(Wiring &w,
                                      NamedPort<"bid", TS<Float>> bid,
                                      NamedPort<"ask", TS<Float>> ask)
       {
           return wire<Average>(w, bid, ask);
       }
   };

.. code-block:: python

   @graph
   def mid(bid: TS[float], ask: TS[float]) -> TS[float]:
       return average(bid, ask)

The logical signature ``(TS<Float>, TS<Float>) -> TS<Float>`` is the same one
the Python ``@graph`` declares. The leading ``Wiring &w`` is plumbing (always the
first parameter, by convention); it is not part of the logical signature.


Composing graphs
----------------

``wire<G>(w, args...)`` wires a sub-graph. Because graphs flatten, this **inlines**
the sub-graph's nodes into the current graph and returns its output port — there
is no runtime "graph node". A call site treats a node and a graph **the same way**:
you pass a ``Port`` for each of the sub-graph's ``Port`` parameters and a plain
value for each of its ``Scalar`` parameters. Positional arguments fill ``compose``
order; keyword arguments target ``NamedPort<"name", S>`` and ``Scalar<"name", T>``
parameters. Typed ports are checked at compile time; erased generic-source ports
are checked at wiring time and then passed to ``compose`` as the declared port
type. The only difference is whether a runtime node is produced.

.. code-block:: cpp

   static Port<TS<Float>> compose(Wiring &w, Port<TS<Float>> bid, Port<TS<Float>> ask)
   {
       auto m = wire<Mid>(w, bid, ask);   // inline the Mid sub-graph
       return wire<Scale>(w, m);
   }

A sub-graph may take scalar parameters too; pass the value just like a node's
scalar (it is wrapped into the sub-graph's ``Scalar<>`` parameter):

.. code-block:: cpp

   using namespace hgraph::literals;

   // sub-graph: compose(Wiring &, NamedPort<"x", TS<Int>> x, Scalar<"by", Int> by) -> TS<Int>
   auto shifted = wire<ShiftBy>(w, src, 5_i);   // port for `x`, 5 wrapped into `by`
   auto same = wire<ShiftBy>(w, arg<"by">(5_i), arg<"x">(src));


Ordering is automatic
---------------------

You wire nodes in whatever order is convenient. When the graph is built it is
**topologically sorted and ranked** so that each node evaluates after the nodes
it depends on — you never assign or reason about evaluation order yourself. This
is the main difference from assembling a ``GraphBuilder`` by hand, where node
order is the caller's responsibility.


Nested graphs — ``nested_<G>``
------------------------------

``wire<G>`` *inlines* a sub-graph: its nodes are flattened into the parent.
``nested_<G>(w, ports…)`` instead **compiles** ``G`` once and wires it as a
single node that owns a child graph — same call shape, same argument checking:

.. code-block:: cpp

   struct Outer
   {
       static constexpr auto name = "outer";
       static Port<TS<Int>>  compose(Wiring &w, Port<TS<Int>> in)
       {
           return nested_<AddOneSubGraph>(w, in);   // one node owning a child graph
       }
   };

For ordinary composition prefer ``wire<G>`` (no runtime boundary). ``nested_``
is the substrate the higher-order operators below are built on; reach for it
directly when you want the sub-graph to exist as one runtime unit.


Higher-order operators
----------------------

``map_`` / ``switch_`` / ``reduce_`` / ``mesh_`` are ordinary registered
operators (call ``stdlib::register_standard_operators()`` first) that take a
**function argument**: the ``WiredFn`` scalar ``fn<X>()``, where ``X`` is any
node or graph struct. They mirror the Python call shapes.

``map_`` — one child per key
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``map_(func, *args, **kwargs)`` instantiates one child instance of ``func`` per
key of its multiplexed inputs and produces ``TSD<K, OUT>``. Every ``TSD`` in
the argument tail multiplexes (children see the element ``TS``, keyed over the
union key set); non-multiplexed arguments broadcast whole to every child.
Same-size ``TSL`` inputs multiplex per index.

.. code-block:: cpp

   struct AddOneG
   {
       static constexpr auto name = "add_one_g";
       static Port<TS<Int>>  compose(Wiring &w, Port<TS<Int>> ts)
       {
           using namespace hgraph::stdlib::syntax;
           return (ts + Int{1}).as<TS<Int>>();
       }
   };

   // prices: Port<TSD<Str, TS<Int>>>  ->  TSD<Str, TS<Int>> of per-key results
   auto out = wire<stdlib::map_>(w, fn<AddOneG>(), prices).as<TSD<Str, TS<Int>>>();

Each key owns an isolated child instance (per-key ``State<>`` is independent);
keys appearing/disappearing in the input dict create and destroy children.
The Python specials are supported: a function whose **first parameter is named**
``key`` (``NamedPort<"key", TS<K>>``) receives the key (``ndx`` for TSL maps);
``arg<"__key_arg__">(Str{"…"})`` renames or (``""``) disables that detection;
``arg<"__keys__">(tss_port)`` supplies an explicit key set; and
``pass_through(port)`` / ``no_key(port)`` force broadcast / suppress keying for
one argument.

.. code-block:: python

   map_(add_one, prices)   # the C++ call above

``switch_`` — one branch at a time
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``switch_(key, cases, *ts, **kwargs)`` routes through **one** child graph,
selected by the key's current value. A key change tears the old branch down and
builds the new one, sampling the held inputs into it:

.. code-block:: cpp

   auto out = wire<stdlib::switch_>(
                  w, mode,   // Port<TS<Str>>
                  stdlib::switch_cases({{Value{Str{"a"}}, fn<Doubler>()},
                                        {Value{Str{"b"}}, fn<Negator>()}}),
                  input)
                  .as<TS<Int>>();

A trailing ``fn<Default>()`` inside ``switch_cases({…}, fn<Default>())`` is the
fall-through branch for unmatched keys. A branch whose first parameter is named
``key`` consumes the key as an input (branches of different arities may be
mixed). ``switch_cases({…}).reload()`` re-instantiates the active branch every
time the key *ticks*, even to the same value.

``reduce_`` — fold a collection
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``reduce_(func, collection[, zero])`` folds a fixed or dynamic ``TSL``, or a
``TSD``, down to one value with a binary combiner (associative — and, for
``TSD``, commutative, since dict reduction is unordered):

.. code-block:: cpp

   auto total = wire<stdlib::reduce_>(w, fn<stdlib::add_>(), values).as<TS<Int>>();

Omitting ``zero`` means there is no zero: an empty collection produces an
invalid result, a singleton returns its value without calling ``func``, and a
larger collection reduces its values normally. When ``zero`` is supplied, an
empty collection returns it and a singleton evaluates ``func(value, zero)``;
once two or more values are live, zero is not used. Unset fixed-``TSL`` slots
are not values. This intentionally differs from the previous Python engine,
which inferred an operation-specific zero and used it for unset slots.

Over a ``TSD`` the runtime maintains a minimal combiner tree and only
re-computes affected paths when keys tick. The same live-value rule applies to
fixed and dynamic ``TSL`` inputs.

``mesh_`` — instances that read each other
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``mesh_`` is ``map_`` over a ``TSD`` whose per-key instances may read *each
other's* outputs, with instances created **on demand** when an absent key is
referenced (so recursion works) and evaluated in dependency order. Inside the
function, reference a sibling by key with ``stdlib::mesh_ref``:

.. code-block:: cpp

   struct FollowPeer
   {
       static constexpr auto name = "follow_peer";
       static Port<TS<Int>>  compose(Wiring &w, Port<TS<Int>> peer_key)
       {
           return stdlib::mesh_ref<TS<Int>>(w, peer_key);   // mesh_(func)[k] in Python
       }
   };

   auto out = wire<stdlib::mesh_>(w, fn<FollowPeer>(), links).as<TSD<Str, TS<Int>>>();

``stdlib::mesh_keys_ref<K>(w[, "name"])`` observes the mesh's live key set;
``arg<"__name__">(Str{"…"})`` names a mesh so nested meshes can be
disambiguated; ``arg<"__keys__">`` supplies an explicit key set, as with
``map_``. A dependency cycle between instances is a **runtime error**. With no
cross-instance references, ``mesh_`` behaves exactly like ``map_``.
(``mesh_ref`` lives in ``lib/std/operators/impl/higher_order_impl.h``.)


Delayed binding — construction order only
------------------------------------------

Use ``delayed_binding`` when a graph must wire a consumer before the producer is
available, but the final graph is still a DAG:

.. code-block:: cpp

   struct ConsumeBeforeProduce
   {
       static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> in)
       {
           auto late = delayed_binding<TS<Int>>(w);
           auto out  = wire<stdlib::pass_through_node>(w, late()); // read it
           late(in);                                             // bind once
           return out;
       }
   };

The placeholder is resolved before graph ranking. It adds no runtime node and no
tick delay, so its producer still ranks before its consumers. Leaving it unbound,
binding it twice, binding an incompatible schema, or using it to create a cycle
is a wiring error. Fixed ``TSL``/``TSB`` bindings are expanded into delayed
leaves, so both their projections and structural aggregates assembled from
separate ports work without materialization. ``TSD`` key-set projections are
also supported. A dynamic structural ``TSL`` has no cardinality at declaration
time and must be materialized as a peered output before it is bound.


Feedback — the sanctioned back-edge
-----------------------------------

Graphs are wired as DAGs; a value that must flow *backwards* goes through
``feedback``, which delays it by exactly one engine cycle:

.. code-block:: cpp

   struct Accumulate
   {
       static constexpr auto name = "accumulate";
       static Port<TS<Int>>  compose(Wiring &w, Port<TS<Int>> in)
       {
           auto prev = stdlib::feedback<TS<Int>>(w, Int{0});  // optional initial value
           auto sum  = wire<AddInts>(w, in, prev());          // prev() -> delayed port
           prev(sum);                                         // prev(port) binds the producer
           return sum;
       }
   };

``feedback<S>(w[, initial])`` returns a handle: call it **with** a port to bind
the producer, and **without** arguments to obtain the one-cycle-delayed port.
This is the only supported same-graph back-edge — any other backward wire is a
wiring error.


Error handling
--------------

Two levels, mirroring Python (design record: developer guide *Error handling*):

- ``exception_time_series(port)`` activates error capture on the node that
  produces ``port`` and returns ``Port<TS<NodeError>>``. When that node's
  ``eval`` throws, a ``NodeError`` ticks on the error output and the graph
  keeps running.
- For ``Port<TSD<K, V>>`` produced by ``map_``, the same function returns
  ``Port<TSD<K, TS<NodeError>>>``. Errors are isolated per mapped child and an
  error key is removed when that child leaves the map.
- ``try_except_<G>(w, args…)`` wraps a whole sub-graph: the result is a ``TSB``
  with fields ``exception`` (``TS<NodeError>``) and ``out`` (the wrapped
  graph's output); a sink sub-graph yields a bare ``TS<NodeError>``.

.. code-block:: cpp

   auto doubled = wire<MightThrow>(w, x);
   auto err     = exception_time_series(doubled);     // Port<TS<NodeError>>

   auto result  = try_except_<RiskyGraph>(w, x).as<TryIntResult>();  // TSB{exception, out}
   auto erased  = wire<stdlib::try_except>(w, fn<RiskyGraph>(), x)
                      .as<TryIntResult>();
   auto error   = exception_time_series(
                      doubled,
                      ErrorCaptureOptions{.trace_back_depth = 2,
                                          .capture_values = true});

``NodeError`` is a compound scalar of string fields (``error_msg`` and friends)
readable through the normal bundle accessors.

.. code-block:: python

   err    = exception_time_series(doubled)
   result = try_except(risky_graph, x)


Services
--------

Services share one implementation across many client call sites, addressed by
an optional path (design record: developer guide *Services, adaptors, shared
outputs and contexts*). Declare the service as a struct — the aliases it
declares are its **flavour tag** (exactly one set, checked at compile time) —
register an implementation, and consume it with the **ordinary ``wire<>``
verb**. How the flavours (and the adaptor kinds of the next section) differ:

.. list-table::
   :header-rows: 1
   :widths: 16 24 20 20 20

   * - Kind
     - Descriptor
     - Client wires
     - Client receives
     - Implementation sees / produces
   * - **Reference service**
     - ``output_schema``
     - ``wire<S>(w)``
     - the one shared output, read by reference (same cycle)
     - nothing in; produces ``output_schema`` (source-shaped)
   * - **Subscription service**
     - ``key_type`` + ``value_schema``
     - ``wire<S>(w, key)``
     - the value for *its* subscribed key (next cycle)
     - ``TSS<key_type>`` (union of all subscribed keys) in; ``TSD<key_type, value_schema>`` out
   * - **Request/reply service**
     - ``request_schema`` + ``response_schema``
     - ``wire<S>(w, request)``
     - its **own** reply (after request and response cycle boundaries)
     - ``TSD<Int, request_schema>`` keyed by client id in; ``TSD<Int, response_schema>`` keyed by the same id out
   * - **Adaptor**
     - ``: adaptor::interface`` + ``input_schema`` / ``output_schema``
     - ``wire<I>(w, in)``
     - the one shared output stream (all clients see the same)
     - one merged input stream (``from_graph``); publishes one output (``to_graph``)
   * - **Service adaptor**
     - ``: service_adaptor::interface`` + ``input_schema`` / ``output_schema``
     - ``wire<I>(w, in)``
     - its **own** reply
     - ``TSD<Int, input_schema>`` keyed by client id (``from_graph``); replies keyed by the same id (``to_graph``)

The worked examples below are taken from ``tests/cpp/test_service_wiring.cpp``
and run green.

Reference service — publish once, read anywhere
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The implementation is source-shaped (nothing flows *in* from clients), so it
must initiate itself — here via ``schedule_on_start``. Clients read the shared
output by reference, in the same cycle it is published:

.. code-block:: cpp

   struct ReferencePricesService                 // tag: output_schema only
   {
       static constexpr std::string_view name{"reference_prices"};
       using output_schema = TSD<Int, TS<Int>>;
   };

   struct ReferencePricesImplNode                // the implementation: a source
   {
       static constexpr auto name              = "reference_prices_impl_node";
       static constexpr bool schedule_on_start = true;

       static void eval(Out<TSD<Int, TS<Int>>> out)
       {
           auto  mutation = out.begin_mutation(out.evaluation_time());
           Value key_7{Int{7}}, price_7{Int{70}};
           mutation.set(key_7.view(), price_7.view());
       }
   };

   struct ReferencePriceClient                   // register + consume
   {
       static constexpr auto name = "reference_price_client";

       static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> instrument)
       {
           service::register_reference_service<ReferencePricesService, ReferencePricesImplNode>(w);
           auto prices = wire<ReferencePricesService>(w);   // Port<TSD<Int, TS<Int>>>
           return wire<stdlib::getitem_>(w, prices, instrument).as<TS<Int>>();
       }
   };

Subscription service — clients subscribe keys
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Each client subscribes a key; the implementation receives the **union** of all
subscribed keys as a ``TSS<key_type>`` and publishes a keyed dictionary; each
client reads back only its own key's value. The key set is **invalid until the
first subscriber arrives** — declare it ``InputValidity::Unchecked`` and
guard — and keys come and go, so removed keys must be erased from the output:

.. code-block:: cpp

   struct PricesService                          // tag: key_type + value_schema
   {
       static constexpr std::string_view name{"prices"};
       using key_type     = Int;
       using value_schema = TS<Int>;
   };

   struct PricesImplNode                         // the implementation
   {
       static constexpr auto name = "prices_impl_node";

       static void eval(In<"keys", TSS<Int>, InputValidity::Unchecked> keys,
                        Out<TSD<Int, TS<Int>>> out)
       {
           if (!keys.valid()) { return; }        // no subscribers yet

           auto mutation = out.begin_mutation(out.evaluation_time());
           for (Int removed : keys.removed()) { static_cast<void>(mutation.erase(Value{removed}.view())); }
           for (Int key : keys.values())
           {
               Value key_value{key};
               Value price{key * Int{10}};
               mutation.set(key_value.view(), price.view());
           }
       }
   };

   struct PriceClient                            // register + consume
   {
       static constexpr auto name = "price_client";

       static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> instrument)
       {
           service::register_subscription_service<PricesService, PricesImplNode>(w);
           return wire<PricesService>(w, instrument);   // subscribe; value arrives next cycle
       }
   };

A subscription is a round trip — the key reaches the implementation on the
next cycle, so the first value lags the subscription by one engine cycle.

Request/reply service — every client gets its own answer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Each client's request is a **separate** entry in a request dictionary, keyed
by a stable per-client id assigned at wiring; the implementation must reply
keyed by the **same id** — that is how a reply finds its client. The request
dictionary is ``InputValidity::Unchecked`` for the same reason as above; a
request element that ticks **invalid** means that client's request went away,
so its reply is erased:

.. code-block:: cpp

   struct AddOneService                          // tag: request_schema + response_schema
   {
       static constexpr std::string_view name{"add_one"};
       using request_schema  = TS<Int>;
       using response_schema = TS<Int>;
   };

   struct AddOneImplNode                         // the implementation
   {
       static constexpr auto name = "add_one_impl_node";

       static void eval(In<"requests", TSD<Int, TS<Int>>, InputValidity::Unchecked> requests,
                        Out<TSD<Int, TS<Int>>> out)
       {
           if (!requests.modified()) { return; }

           auto mutation = out.begin_mutation(out.evaluation_time());
           for (const auto &[request_id, request] : requests.removed_items())
           {
               static_cast<void>(mutation.erase(request_id));
           }
           for (const auto &[request_id, request] : requests.modified_items())
           {
               if (!request.valid()) { static_cast<void>(mutation.erase(request_id)); continue; }
               Value response{request.value() + Int{1}};
               mutation.set(request_id, response.view());
           }
       }
   };

   struct AddOneClient                           // register + consume
   {
       static constexpr auto name = "add_one_client";

       static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> request)
       {
           service::register_request_reply_service<AddOneService, AddOneImplNode>(w);
           return wire<AddOneService>(w, request);      // this client's own delayed reply
       }
   };

Two clients of the same service are two independent request-dictionary
entries; the implementation sees both requests in one cumulative delta and
each client receives only its own reply. The runtime selects transport after
the implementation is wired. This request-driven implementation defers the
request by one engine cycle and publishes its response directly, so one
request produces ``[None, response]``. A decoupled external sink/push-source
implementation needs no engine-imposed delay; an implementation that calls a
service or adaptor retains the full feedback form required for recursion.

Paths and other service mechanics
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``service::path("…")`` addresses independent instances of the same service and
is always the **first** argument after ``w`` (for registration and consumption
alike — e.g. ``wire<S>(w, service::path("premium"), request)``); registration
and consumption are separate, so a library can consume a service its host
application registers.

Paths may be **qualified with scalar arguments** —
``service::path("prices", arg<"tier">(Str{"premium"}))`` — and each distinct
argument set is an independent service instance (the implementation's
``Scalar<"path", Str>`` receives the full qualified string). A descriptor may
set ``static constexpr std::string_view default_path`` to change its default.
Registering two implementations on the **same** path is rejected at build time
(``std::invalid_argument``). Service descriptors may also be **templates** —
each instantiation (``TemplateAddService<Int>``) binds as its own concrete
interface; use a qualified path to keep instantiations apart.

An implementation may be a node (as above) or a graph (``compose`` taking the
flavour input as a port), and may declare ``Scalar<"path", Str>`` to receive
the path it was registered under. One graph can implement **several services
at once** — register with ``service::register_services<Impl, Services…>(w,
path)`` and, inside its ``compose``, fetch each interface's input with
``service::impl_input<S>(w, path)`` and publish each output with
``service::impl_output<S>(w, path, port)``. The design record, including the
multi-interface worked example, is the developer guide's *Services, adaptors,
shared outputs and contexts* page.


Adaptors
--------

An adaptor is the boundary to the outside world: one implementation owns the
external interaction and client graphs exchange time-series with it through an
interface (see the comparison table at the top of *Services* for how the two
adaptor kinds relate to the service flavours). The descriptor derives from
``adaptor::interface`` and declares
``input_schema`` (what clients send) and/or ``output_schema`` (what clients
receive) — omit one for a sink-only or source-only adaptor. Clients use the
ordinary ``wire<>`` verb; the implementation is a graph that reaches the
client-facing boundary with ``adaptor::from_graph`` / ``adaptor::to_graph``:

.. code-block:: cpp

   struct LoopbackAdaptor : adaptor::interface
   {
       static constexpr std::string_view name{"loopback"};
       using input_schema  = TS<Int>;
       using output_schema = TS<Int>;
   };

   struct LoopbackAdaptorImpl
   {
       static void compose(Wiring &w)
       {
           auto input  = adaptor::from_graph<LoopbackAdaptor>(w);  // what clients sent
           auto output = wire<EchoNode>(w, input);
           adaptor::to_graph<LoopbackAdaptor>(w, output);          // what clients receive
       }
   };

   // consuming graph:
   adaptor::register_adaptor<LoopbackAdaptor, LoopbackAdaptorImpl>(w);
   auto out = wire<LoopbackAdaptor>(w, some_input);

``adaptor::path("…")`` (first argument after ``w``) addresses independent
instances and supports the same **scalar-qualified** form as service paths
(``adaptor::path("typed", arg<"side">(Str{"primary"}))``); an implementation
may take ``Scalar<"path", Str>`` to receive its registered path, and one
implementation can serve multiple interfaces via
``adaptor::register_adaptors<Impl, Interfaces…>(w, path)``. Duplicate
registrations on one path are rejected at build time. External-world
machinery — push sources for real-time events, connection lifecycle — belongs
inside the adaptor implementation, never in client graphs.

A plain adaptor gives every client the **same merged stream**. When each client
needs its **own reply** (request/reply over an adaptor), use a **service
adaptor** instead: derive the descriptor from ``service_adaptor::interface``.
Clients still call ``wire<Interface>(w[, path], input)`` and each receives its
own output; the implementation sees all client inputs as a dictionary keyed by
a stable per-client id and must reply keyed by the **same id** (the same
contract as a request/reply service implementation):

.. code-block:: cpp

   struct AddTwentyServiceAdaptor : service_adaptor::interface
   {
       static constexpr std::string_view name{"add_twenty_adaptor"};
       using input_schema  = TS<Int>;
       using output_schema = TS<Int>;
   };

   struct AddTwentyServiceAdaptorImpl
   {
       static void compose(Wiring &w, Scalar<"path", Str> path)
       {
           const auto custom   = service_adaptor::path(path.value());
           auto       requests = service_adaptor::from_graph<AddTwentyServiceAdaptor>(w, custom);
           // requests: Port<TSD<Int, TS<Int>>> — every client's input, keyed by client id
           auto replies = wire<AddTwentyServiceAdaptorImplNode>(w, requests).as<TSD<Int, TS<Int>>>();
           service_adaptor::to_graph<AddTwentyServiceAdaptor>(w, custom, replies);
       }
   };

   // consuming graph:
   const auto custom = service_adaptor::path("multi_client");
   service_adaptor::register_service_adaptor<AddTwentyServiceAdaptor,
                                             AddTwentyServiceAdaptorImpl>(w, custom);
   auto reply = wire<AddTwentyServiceAdaptor>(w, custom, request);


Contexts
--------

A context is a **wiring-scoped named port**: publish a time-series under a
name for a scope, and anything wired inside consumes it without threading the
port through every intermediate signature (``types/context_wiring.h``):

.. code-block:: cpp

   struct Consumer
   {
       static constexpr auto name = "consumer";

       // An ordinary input, auto-wired from the nearest enclosing
       // ``context::scope<"price">`` — callers do not pass it.
       static void eval(Context<"price", TS<Float>> price, Out<TS<Float>> out)
       {
           out.set(price.value() * 2.0);
       }
   };

   struct PricingGraph
   {
       static void compose(Wiring &w, Port<TS<Float>> price)
       {
           context::scope<"price"> ctx{w, price};   // publish for this scope
           wire<Consumer>(w);                        // ``price`` resolves here
       }                                             // scope pops
   };

The rules:

- Scopes **nest**; the nearest publication with a matching name wins
  (an inner ``context::scope<"price">`` shadows an outer one).
- A ``Context<"name", S>`` input behaves exactly like ``In`` at evaluation
  time and accepts the same activity/validity policies. Generic schemas work:
  ``Context<"price", TS<ScalarVar<"T">>>`` binds whatever is published.
- Passing ``arg<"name">(port)`` explicitly **overrides** the ambient context
  for that one call; positional arguments never touch context parameters.
- ``context::get<TS<Float>>(w, "price")`` / ``context::has(w, "price")`` are
  the function forms for ``compose`` bodies.
- A missing context is a **wiring-time error** naming the context.

Contexts do not yet cross compiled sub-graph boundaries (``map_`` /
``switch_`` / ``nested_`` children) — that import/export step is on the
roadmap, and the wiring surface will not change when it lands.


Lowering a graph over Arrow frames
----------------------------------

``stdlib::lower`` turns a reactive graph into one ordinary Arrow-frame call.
It wires each input ``Frame`` through the native ``from_data_frame`` source,
invokes the graph through ``WiredFn``, snapshots output ticks with
``to_data_frame``, and concatenates those snapshots into one result frame. No
record/replay backend or process-global frame store is involved.

Input frames follow the graph's time-series ``Port`` parameter order. Scalar
``TS<T>`` uses ``date`` and ``value`` columns by default; ``TSD<K, TS<T>>``
also uses ``key``; a ``TSB`` uses its field names. The same structural rules
are used for the output. Standard operators must be registered before wiring,
as for any other stdlib graph:

.. code-block:: cpp

   #include <hgraph/lib/std/lower.h>
   #include <hgraph/lib/std/operators/registration.h>

   struct AddFrames
   {
       static Port<TS<Int>> compose(
           Wiring &w, NamedPort<"lhs", TS<Int>> lhs,
           NamedPort<"rhs", TS<Int>> rhs)
       {
           return wire<stdlib::add_>(w, lhs, rhs).as<TS<Int>>();
       }
   };

   stdlib::register_standard_operators();
   std::array<Frame, 2> inputs{lhs_frame, rhs_frame};
   std::optional<Frame> result = stdlib::lower<AddFrames>(inputs);

A sink graph returns ``std::nullopt``. ``LowerOptions`` changes the
``date_column``, ``key_column``, and ``value_column``, bounds execution with
``start_time`` / ``end_time``, and accepts a lifecycle observer. As-of support
is deliberately explicit: set ``no_as_of_support = false`` to require the
configured as-of column on every input. Input versions after the invocation
as-of are discarded and the latest visible row per ``(date, key)`` is replayed;
unkeyed inputs group by date alone. The output includes that one fixed
invocation as-of value (set ``as_of`` to choose it).

``prepare_lower`` returns a move-only ``LowerExecution`` when an embedding
needs to retain the prepared executor, inspect its graph-local
``GlobalState``, or control when ``run`` occurs. An active ``GlobalContext`` is
copied into the graph before execution and receives the final graph state after
the run; the private collected frame never leaks into that state.


Running a graph
---------------

``build_graph<G>()`` produces a ``GraphBuilder``; run it through a
``GraphExecutor`` in simulation mode (see *Authoring Nodes in C++ > Assembling
and running a graph*):

.. code-block:: cpp

   GraphExecutorBuilder ex;
   ex.graph_builder(build_graph<PriceGraph>()).start_time(MIN_ST).end_time(end);
   ex.make_executor().view().run();

.. code-block:: python

   run_graph(price_graph, run_mode=EvaluationMode.SIMULATION, start_time=start, end_time=end)

**Real-time mode** aligns evaluation with the wall clock: the executor waits
for each scheduled time, external threads can inject values through push
sources (see *Authoring Nodes in C++ > Node kinds*), and ``request_stop()``
wakes and stops a sleeping executor:

.. code-block:: cpp

   GraphExecutorBuilder ex;
   ex.graph_builder(std::move(gb))
       .mode(GraphExecutorMode::RealTime)
       .start_time(start_time)
       .end_time(start_time + TimeDelta{5'000'000});

   GraphExecutorValue executor = ex.make_executor();
   auto               view     = executor.view();
   // view.run() blocks until end_time or request_stop(); run it on a worker
   // thread when the controlling thread needs to keep going.
   view.request_stop();   // callable from another thread; wakes the executor

Construct one ``GraphExecutorValue`` per run. Distinct executors may call
``run()`` concurrently on different threads: each owns its graph, scheduler,
clock, and ``GlobalState``, so a blocked engine does not hold an execution lock
needed by another engine. ``run()`` is not re-entrant on the *same* executor,
and callbacks must synchronise any external state they deliberately share.
Concurrent engines do not promise aligned wall-clock times or evaluation ticks;
only the ordering produced inside each graph is defined.

.. code-block:: python

   run_graph(price_graph, run_mode=EvaluationMode.REAL_TIME, ...)

Push sources require a real-time **root** graph, and real-time executors also
enable wall-clock scheduler alarms (``NodeScheduler(..., on_wall_clock=true)``).


Graph global state
------------------

A graph carries a shared, mutable ``string -> value`` store — the
``GlobalState`` — created on the ``GraphBuilder`` at wiring time and copied onto
each ``GraphValue`` it builds. Seed it before building, and read it back after a
run; a node sees the same store via the ``GlobalStateView`` injectable (see
*Authoring Nodes in C++ > Injected services*):

.. code-block:: cpp

   GraphBuilder gb = build_graph<MyGraph>();
   gb.global_state().set("seed", Value{Int{20}});   // seed at wiring time

   GraphExecutorBuilder ex;
   ex.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(end);
   GraphExecutorValue executor = ex.make_executor();
   executor.view().run();

   // read results the nodes wrote into the store
   const Int total = executor.view().graph().global_state().get_as<Int>("total");

A ``compose`` body can also seed the store **during wiring**, via
``Wiring::global_state()`` — ``finish`` carries those entries onto the graph, so a
value set in ``compose`` is visible to nodes at run time (and can be modified by
their ``eval``):

.. code-block:: cpp

   struct CounterGraph
   {
       static constexpr auto name = "counter_graph";
       static void compose(Wiring &w)
       {
           w.global_state().set("counter", Value{Int{100}});   // set at wiring time
           wire<BumpCounter>(w);                           // a node whose eval bumps "counter"
       }
   };
   // after running: global_state().get_as<Int>("counter") == 101

Each built graph gets its own copy seeded with the wiring-time entries, so the
builder stays reusable. Values are heterogeneous (a mutable ``Map<string, Any>``
under the hood). This is the store the testing toolkit's replay/record use.

Opt-in polymorphic compound-scalar pooling
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Polymorphic compound scalars use the existing inline closed-union layout by
default.  A graph which replicates many keys or values from a wide hierarchy
can opt into per-leaf stable pools through its wiring-time ``GlobalState``:

.. code-block:: cpp

   #include <hgraph/types/metadata/type_realization.h>

   GlobalState state;
   set_pooled_compound_scalar_storage(state.view());
   GlobalContext context{state};
   GraphBuilder gb = build_graph<MyGraph>();

The context must remain active until wiring finishes so scalar and time-series
bindings, the immutable realisation snapshot, and the erased graph layout all
capture the same policy.  Disable it explicitly with
``set_pooled_compound_scalar_storage(state.view(), false)``.  A disabled graph
has no pool owner/view field in its planned storage and pays no pool lifecycle
or per-tick scope cost.

The Python surface selects the same C++ policy:

.. code-block:: python

   state = GlobalState()
   with GlobalContext(state):
       set_pooled_compound_scalar_storage()
       run_graph(my_graph)

Off-thread push values remain self-contained and move into graph-local pools
only after reaching the graph evaluation thread.


What's planned
--------------

Node wiring, sub-graph composition (with the same scalar-literal ergonomics as
nodes), node-level scalar arguments and top-level graph-level scalar parameters work
today — as do standalone sub-graph building / boundary binding (the substrate for
non-flattening nested graphs), the higher-order operators ``map_`` / ``reduce`` /
``switch_`` / ``mesh_`` and ``feedback`` (see the developer guide's *Nested
graphs* and *Mesh* pages), and ``try_except_`` error handling (see *Error
handling*). Generic ``TsVar`` / ``ScalarVar`` signatures, including constrained
variables, resolve from concrete graph inputs through the same C++ type-pattern
machinery used by nodes and operators. Beyond those the following are deferred
(and map onto Python features):

- **multiple outputs** — a graph returning a ``TSB`` becomes a bundle ``Port``
  with ``.field<"x">()`` to take a sub-port; as syntactic sugar a graph's outputs
  may instead be returned as an array;
- **dynamic-TSL multiplexing** in ``map_`` / ``mesh_``, and sink maps /
  all-sink switches.


C++ ↔ Python cheat sheet
------------------------

.. list-table::
   :header-rows: 1
   :widths: 50 50

   * - C++
     - Python
   * - ``struct G { static Out compose(Wiring&, In...){} };``
     - ``@graph def g(in...) -> Out``
   * - ``wire<T>(w, ports...)`` (node)
     - calling a node — ``t(ports...)``
   * - ``wire<G>(w, ports...)`` (sub-graph, inlined)
     - calling a sub-graph — ``g(ports...)``
   * - ``Port<TS<Int>>``
     - a wiring-time time-series handle
   * - ``build_graph<G>()`` → ``GraphExecutor``
     - wiring the ``@graph`` + ``run_graph(...)``
   * - ``nested_<G>(w, ports...)`` (compiled child graph)
     - (implicit — Python nests via operators)
   * - ``wire<stdlib::map_>(w, fn<F>(), tsd...)``
     - ``map_(f, tsd...)``
   * - ``wire<stdlib::switch_>(w, key, switch_cases({...}), ts...)``
     - ``switch_(key, {...}, ts...)``
   * - ``wire<stdlib::reduce_>(w, fn<F>(), coll)``
     - ``reduce(f, coll[, zero])``
   * - ``wire<stdlib::mesh_>(w, fn<F>(), tsd...)`` / ``mesh_ref<S>(w, key)``
     - ``mesh_(f, tsd...)`` / ``mesh_(f)[k]``
   * - ``stdlib::feedback<S>(w, init)``; ``fb(port)`` / ``fb()``
     - ``feedback(TS[...], init)``; ``fb(port)`` / ``fb()``
   * - ``delayed_binding<S>(w)``; ``late(port)`` / ``late()``
     - ``delayed_binding(TS[...])``; ``late(port)`` / ``late()``
   * - ``exception_time_series(port)`` / ``try_except_<G>(w, ...)``
     - ``exception_time_series(ts)`` / ``try_except(g, ...)``
   * - ``service::register_*_service<S, Impl>(w)`` + ``wire<S>(w, ...)``
     - ``@service_impl`` / ``register_service`` + calling the service
   * - ``service::impl_input<S>(w)`` / ``service::impl_output<S>(w, port)``
     - the impl function's service parameters / return
   * - ``adaptor::register_adaptor<I, Impl>(w)`` + ``wire<I>(w, in)``
     - ``@adaptor`` / ``@adaptor_impl`` + calling the adaptor
   * - ``adaptor::from_graph<I>(w)`` / ``adaptor::to_graph<I>(w, port)``
     - ``adaptor.from_graph`` / ``adaptor.to_graph``
   * - ``service_adaptor::interface`` + ``register_service_adaptor<I, Impl>(w)``
     - ``@service_adaptor`` / ``@service_adaptor_impl``
   * - ``service::path("p", arg<"k">(v))`` (scalar-qualified path)
     - path with typed parameters
