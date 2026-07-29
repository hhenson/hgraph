Authoring Nodes in C++
======================

HGraph lets you write nodes and graphs directly in C++. A node is an ordinary,
stateless C++ ``struct`` with a static ``eval`` function; its parameters declare
what the node consumes and produces. The runtime reflects that signature at
compile time and builds the node for you — there is no base class to inherit and
no virtual dispatch in your code.

This page is the C++ counterpart of writing ``@compute_node`` / ``@generator`` /
``@sink_node`` functions in the Python ``hgraph`` package. Every section shows
the C++ form next to the equivalent Python so you can carry your mental model
across.

.. note::

   **Feature status.** This project is C++-first and still being built out. Each
   feature below is tagged:

   - **Available** — compiles and runs in the current build.
   - **Planned** — shown for completeness so the full model is visible. The
     syntax is provisional and **does not compile yet**; planned C++ snippets
     are marked with a ``// Planned`` comment.

   A complete matrix is in `Feature status`_ at the end.

To use the node-authoring API, include one header:

.. code-block:: cpp

   #include <hgraph/types/static_node.h>   // selectors + NodeBuilder::implementation<T>()
   #include <hgraph/runtime/runtime.h>      // GraphBuilder, GraphExecutor


Your first node
---------------

**Available.** A compute node that adds two integer time-series:

.. code-block:: cpp

   struct Add
   {
       static constexpr auto name = "add";

       static void eval(In<"lhs", TS<Int>> lhs, In<"rhs", TS<Int>> rhs, Out<TS<Int>> out)
       {
           out.set(lhs.value() + rhs.value());
       }
   };

The Python equivalent:

.. code-block:: python

   from hgraph import compute_node, TS

   @compute_node
   def add(lhs: TS[int], rhs: TS[int]) -> TS[int]:
       return lhs.value + rhs.value

The correspondence is direct:

- the C++ ``struct`` ↔ the decorated Python function,
- each ``In<Name, TS<T>>`` parameter ↔ a Python time-series parameter,
- the single ``Out<TS<T>>`` parameter ↔ the Python ``-> TS[int]`` return
  annotation (C++ writes the result with ``out.set(...)`` instead of
  ``return``).


The shape of a C++ node
-----------------------

A node implementation must be:

- a **stateless** ``struct`` / ``class`` (empty — it holds no members; all state
  lives in ``State`` selectors, see `Node-local state`_),
- with a ``static void eval(...)`` function,
- whose parameters are **selectors** (``In`` / ``Out`` / ``State`` / …).

The selector **type** of each parameter identifies its role, so the ``In`` /
``Out`` / ``State`` / service parameters may sit in **any position** in the C++
signature — the runtime classifies each by its type, not by where it sits. (This
type-matching of the *annotations* is the only thing that is position-free, and
is what distinguishes a marker from an argument.)

The node's **argument schema is ordered**, exactly like the equivalent Python
node. The relative order of the ``In<>`` (and scalar) parameters defines the
node's arguments, and a caller wires them **positionally, or by keyword** using
the ``In<>`` name — the same calling convention Python uses, and the two schemas
must match. Inputs and scalar arguments are supplied by the caller / wiring;
outputs, state and injected services are supplied by the runtime. By convention,
place the ``Out`` parameter **last**, so a signature reads its inputs first and
its output last.

.. code-block:: cpp

   // 'Out' is identified by its type so it may sit anywhere, but by convention it
   // goes last. The input order (lhs, rhs) is the argument schema.
   static void eval(In<"lhs", TS<Int>> lhs, In<"rhs", TS<Int>> rhs, Out<TS<Int>> out);

By default, ``eval`` defines the complete argument schema. ``start`` and ``stop``
are **not** the argument schema: each lists only the parameters it needs, matched
**by name** (the ``In<>`` / ``Scalar<>`` name, or the selector for state and
services) and validated by type. See `Lifecycle: start / eval / stop`_ for the
explicit-signature form where ``eval`` may also request a subset.

All hooks (``eval`` / ``start`` / ``stop``) return ``void``.

A ``static constexpr auto name`` member is optional and sets the node's display
name. The **node kind** (compute, source, sink) is inferred from which selectors
are present; see `Node kinds`_.


Reading inputs — ``In<>``
-------------------------

**Available (scalar ``TS<T>``).** An input selector exposes the tick contract of
a time-series:

.. list-table::
   :header-rows: 1
   :widths: 30 35 35

   * - Concept
     - C++ (``In<"x", TS<T>>``)
     - Python (``x: TS[T]``)
   * - current value
     - ``x.value()``
     - ``x.value``
   * - was modified this cycle
     - ``x.modified()``
     - ``x.modified``
   * - has a value yet
     - ``x.valid()``
     - ``x.valid``
   * - delta of this tick
     - ``x.delta_value()`` *(planned typed accessor)*
     - ``x.delta_value``

.. code-block:: cpp

   struct GateOnChange
   {
       static constexpr auto name = "gate_on_change";

       static void eval(In<"in", TS<Int>> in, Out<TS<Int>> out)
       {
           if (in.modified()) { out.set(in.value()); }
       }
   };

.. code-block:: python

   @compute_node
   def gate_on_change(in_: TS[int]) -> TS[int]:
       if in_.modified:
           return in_.value


Producing output — ``Out<>``
----------------------------

**Available (scalar ``TS<T>``).** ``Out<TS<T>>::set(v)`` writes the value and
ticks the output at the current evaluation time. A node has **at most one**
``Out`` parameter; emit multiple values by making the output a bundle (see
`Bundles and compound shapes`_).

.. code-block:: cpp

   struct Increment
   {
       static void eval(In<"in", TS<Int>> in, Out<TS<Int>> out) { out.set(in.value() + 1); }
   };

.. code-block:: python

   @compute_node
   def increment(in_: TS[int]) -> TS[int]:
       return in_.value + 1

In Python you ``return`` the value; in C++ you call ``out.set(...)``. Not calling
``set`` (Python: returning ``None``) leaves the output unticked for that cycle.


.. _node-local state:

Node-local state — ``State<>``
------------------------------

**Available (scalar state).** ``State<T>`` is a typed handle to per-node state
that persists across evaluations. Use ``start`` to initialise it.

.. code-block:: cpp

   struct Counter
   {
       static constexpr auto name = "counter";

       static void start(State<Int> state) { state.set(Int{0}); }

       static void eval(State<Int> state, Out<TS<Int>> out)
       {
           const Int next = state.get() + Int{1};
           state.set(next);
           out.set(next);
       }
   };

.. code-block:: python

   from hgraph import compute_node, generator, TS, STATE
   from dataclasses import dataclass

   @dataclass
   class CounterState:
       n: int = 0

   @compute_node
   def counter(_state: STATE[CounterState] = None) -> TS[int]:
       _state.n += 1
       return _state.n

**Planned:** named state ``State<TSchema, "name">`` and bundle/compound state
(today ``State<T>`` is a single scalar slot).


Node kinds
----------

The kind is **always determined from the node's shape** — which selectors and
hooks are present. There is deliberately no override: the kind has a single
source of truth, the code.

.. list-table::
   :header-rows: 1
   :widths: 22 26 28 24

   * - Kind
     - Inferred when
     - C++
     - Python
   * - **Compute** *(available)*
     - ``eval`` has ``In`` and ``Out``
     - ``eval(In<…>, Out<…>)``
     - ``@compute_node``
   * - **Pull source** *(available)*
     - ``eval`` has ``Out``, no ``In``
     - ``eval(Out<…>)``
     - ``@generator``
   * - **Sink** *(available)*
     - ``eval`` has ``In``, no ``Out``
     - ``eval(In<…>)``
     - ``@sink_node``
   * - **Push source** *(available — runtime builder)*
     - built by ``make_push_source_node``
     - specialized node owns queue + ``PushSourceSender``
     - ``@push_queue``

A sink node (side effect, no output):

.. code-block:: cpp

   struct Print
   {
       static constexpr auto name = "print";
       static void eval(In<"in", TS<Int>> in) { std::printf("%lld\n", static_cast<long long>(in.value())); }
   };

.. code-block:: python

   @sink_node
   def print_(in_: TS[int]) -> None:
       print(in_.value)

A push source receives messages from outside the graph (**available** via the
runtime builder; static-node authoring sugar is still planned). It uses a
specialized node/builder rather than the ordinary static-node
``implementation<T>()`` path: ``make_push_source_node``
(``runtime/push_source_node.h``) owns the message storage, hands a thread-safe
``PushSourceSender`` to user code during ``start``, and delivers values through
the normal node evaluation path when the real-time engine is woken by queued
messages. Two delivery policies exist — **Queue** (FIFO; drains one value per
engine cycle) and **Conflating** (a delta-merging accumulator; delivers the
merged state) — and push sources require a **real-time root graph** (they are
rejected in simulation mode and inside nested graphs):

.. code-block:: cpp

   // Runtime builder — see tests/cpp/test_realtime_execution.cpp
   graph_builder.add_node(make_push_source_node(
       *ts_int,                                            // output schema: TS<Int>
       make_push_source_queue_policy(*ts_int->value_schema),
       [](PushSourceSender sender) {
           // runs at start; hand `sender` to a producer thread.
           // sender.send(Int{42}) enqueues a value and wakes the executor.
       }));

.. code-block:: python

   @push_queue(TS[int])
   def from_queue(sender: Callable[[int], None]):
       ...  # register `sender`; call sender(value) from another thread to inject ticks

A static-node authoring shape (a sender parameter on ``start`` plus a
message-application hook) is still **planned**. The important runtime
constraint stands either way: push-source behavior belongs to the specialized
node implementation and the real-time evaluator; it must not add a generic
message-application operation to all nodes.


Lifecycle: ``start`` / ``eval`` / ``stop``
------------------------------------------

**Available.** Besides ``eval``, a node may define ``static void start(...)`` and
``static void stop(...)``, taking the same kind of selectors. ``start`` runs once
when the graph starts (good for initialising ``State``), ``stop`` once at
teardown.

.. code-block:: cpp

   struct WithLifecycle
   {
       static void start(State<Int> s) { s.set(Int{0}); }
       static void eval(In<"in", TS<Int>> in, State<Int> s, Out<TS<Int>> out)
       {
           s.set(s.get() + in.value());
           out.set(s.get());
       }
       static void stop(State<Int> s) { /* flush / release */ (void) s; }
   };

.. code-block:: python

   @compute_node
   def with_lifecycle(in_: TS[int], _state: STATE = None) -> TS[int]:
       _state.total += in_.value
       return _state.total

   @with_lifecycle.start
   def _start(_state: STATE):
       _state.total = 0

   @with_lifecycle.stop
   def _stop(_state: STATE):
       ...  # flush / release

Normally ``eval`` declares the complete node schema. For a node with
configuration used only by ``start`` or ``stop``, an optional
``signature_args`` tuple can declare that canonical schema explicitly. Every
hook then requests only the selectors it uses; named input and scalar selectors
are located in the canonical tuple, so their wiring order remains stable:

.. code-block:: cpp

   struct LifecycleConfigured
   {
       using signature_args = std::tuple<
           Scalar<"value", Int>, Scalar<"flush_limit", Int>,
           State<Int>, Out<TS<Int>>>;

       static void start(Scalar<"flush_limit", Int> limit, State<Int> state)
       {
           state.set(limit.value());
       }

       // flush_limit is not constructed on the hot evaluation path.
       static void eval(Scalar<"value", Int> value, State<Int> state,
                        Out<TS<Int>> out)
       {
           out.set(value.value() + state.get());
       }

       static void stop(Scalar<"flush_limit", Int> limit, State<Int> state)
       {
           /* flush using limit */ (void)limit; (void)state;
       }
   };

Use this form only when the complete schema cannot be expressed efficiently by
``eval`` itself. ``signature_args`` is authoritative for node-kind inference,
input/scalar order, output, state, and injected runtime components; each hook's
parameters must be a valid subset of it.


Time-series type vocabulary
---------------------------

Schemas are expressed as compile-time **marker types** that mirror the Python
time-series types. All markers exist today; the C++ ``In`` / ``Out`` selectors
cover the supported non-``REF`` time-series kinds.

.. list-table::
   :header-rows: 1
   :widths: 26 30 26 18

   * - Kind
     - C++ marker
     - Python
     - ``In``/``Out`` selector
   * - scalar time-series
     - ``TS<T>``
     - ``TS[T]``
     - **available**
   * - signal (valueless tick)
     - ``SIGNAL``
     - ``SIGNAL``
     - **available**
   * - bundle (named fields)
     - ``TSB<"Name", Field<…>…>``
     - ``TSB[Schema]``
     - **available**
   * - list (fixed / dynamic)
     - ``TSL<T, N>``
     - ``TSL[T, Size[N]]``
     - **available**
   * - set
     - ``TSS<T>``
     - ``TSS[T]``
     - **available**
   * - dict (keyed)
     - ``TSD<K, V>``
     - ``TSD[K, V]``
     - **available**
   * - rolling window
     - ``TSW<T, Period>``
     - ``TSW[T, ...]``
     - **available** (tick-count windows)
   * - reference
     - ``REF<TSchema>``
     - ``REF[...]``
     - **available** (opaque token surface; see *References and signals*)

The marker types compose exactly like the Python generics — for example a dict
of bundles keyed by string:

.. code-block:: cpp

   using PriceTick = TSB<"PriceTick",
                         Field<"bid", TS<Float>>,
                         Field<"ask", TS<Float>>>;

   using QuoteFeed = TSD<Str, PriceTick>;

.. code-block:: python

   from hgraph import TimeSeriesSchema, TS, TSB, TSD

   class PriceTick(TimeSeriesSchema):
       bid: TS[float]
       ask: TS[float]

   QuoteFeed = TSD[str, TSB[PriceTick]]


Bundles and compound shapes
---------------------------

**Available.** A bundle groups named time-series. It is how a node takes several
related inputs as one parameter, or returns several outputs.

.. code-block:: cpp

   using Quote = TSB<"Quote", Field<"bid", TS<Float>>, Field<"ask", TS<Float>>>;

   struct MidPrice
   {
       static void eval(In<"q", Quote> q, Out<TS<Float>> out)
       {
           out.set((q.field<"bid">().value() + q.field<"ask">().value()) / Float{2.0});
       }
   };

   struct MakeQuote
   {
       static void eval(In<"px", TS<Float>> px, Out<Quote> out)
       {
           out.field<"bid">().set(px.value());
           out.field<"ask">().set(px.value() + Float{0.01});
       }
   };

.. code-block:: python

   @compute_node
   def mid_price(q: TSB[Quote]) -> TS[float]:
       return (q.bid.value + q.ask.value) / 2.0

Internally the top-level input of every node is already a bundle — each ``In``
parameter becomes one field of it — which is why a node's inputs and a ``TSB``
share the same structure.

The typed ``TSB`` selectors work for authored nodes and for erased
replay/record. A ``TSB`` delta is the canonical
``Bundle{field: delta(field_schema)...}``; unchanged scalar fields remain typed
null. Build expected test deltas with ``tsb_delta<Schema>(...)`` in schema field
order. ``std::nullopt`` leaves a field at its canonical default delta: typed-null
for scalar children, empty delta for collection children.

Visiting an erased endpoint
---------------------------

When an extension receives a type-erased ``TSInputView`` or ``TSOutputView``,
use ``hgraph::visit`` to recover its semantic time-series shape without
repeating a ``TSTypeKind`` switch:

.. code-block:: cpp

   #include <hgraph/types/time_series/visitor.h>

   void describe(const TSInputView &input)
   {
       visit(
           input,
           [](TSDInputView dict) {
               // Keyed child selection remains an explicit algorithm policy.
               for (auto &&[key, child] : dict.items()) { /* ... */ }
           },
           [](TSBInputView bundle) {
               for (auto &&[name, child] : bundle.items()) { /* ... */ }
           },
           [](TSInputView leaf_or_other_collection) {
               // Role-level fallback for every shape not handled above.
           });
   }

The selected view is borrowed and move-only. The call visits exactly one
endpoint: it neither follows ``REF`` values nor recursively walks children.
All handlers return ``void`` in this example; value-returning handlers must all
produce the same non-reference type. Invalid-current-value and unbound inputs
still dispatch from their schema, while a default view with no schema throws.


Collections — ``TSS`` / ``TSL`` / ``TSD`` / ``TSW``
---------------------------------------------------

Collection selectors **derive from the type-erased collection view** for their
kind (``In<…, TSS<T>> : TSSInputView``, ``In<…, TSL<C,N>> : TSLInputView``, and the
``Out`` duals — see *Schemas > Static Schema > Selector wrappers*), so they inherit
the full view API and add typed sugar. The per-cycle **delta** is the canonical
type-erased ``Value`` (``In<…>::delta()`` is the inherited ``delta_value()``).

**Set (``TSS<T>``) — available.** Read membership / this tick's changes; write with
``add`` / ``remove`` / ``clear``:

.. code-block:: cpp

   struct AddedCount
   {
       static void eval(In<"s", TSS<Int>> s, Out<TS<Int>> out)
       {
           out.set(static_cast<Int>(s.added().size()));   // s also has removed()/values()/contains()
       }
   };

**List (``TSL<C, N>``) — available and recursive.** A fixed-size list of ``N``
children whose schema ``C`` is any supported non-``REF`` time-series type -
``TS<T>``, ``SIGNAL``, ``TSS<T>``, ``TSD<K,V>``, fixed or dynamic ``TSL``,
``TSB<...>``, or ``TSW<T,...>`` - nested arbitrarily. ``in[i]`` yields the child
input selector ``In<"", C>`` and ``out[i]`` the child output ``Out<C>``, so you
compose recursively:

.. code-block:: cpp

   // TSL of scalars: sum the children
   struct SumList
   {
       static void eval(In<"l", TSL<TS<Int>, 3>> l, Out<TS<Int>> out)
       {
           Int total = 0;
           for (std::size_t i = 0; i < l.size(); ++i) total += l[i].value();
           out.set(total);
       }
   };

   // TSL of sets: forward each child's added elements (out[i] is an Out<TSS<Int>>)
   struct FanIn
   {
       static void eval(In<"l", TSL<TSS<Int>, 2>> l, Out<TSL<TSS<Int>, 2>> out)
       {
           for (auto &&[i, child] : l.modified_items())
               for (int e : l[i].added()) out[i].add(e);
       }
   };

A ``TSL`` delta is the canonical ``Map<int, delta(C)>`` ``Value`` (recursive in
``C``); build one for tests with ``list_delta`` (see *Testing Graphs in C++*).

The selector composition above is recursive over **any** child today, and the TSData
runtime supports fixed ``TSL`` children across the implemented non-``REF`` kinds:
``TS``, ``SIGNAL``, ``TSS``, ``TSD``, fixed and dynamic ``TSL``, ``TSB``, and ``TSW``. A
``TSL<TSS<Int>, N>`` such as ``FanIn`` owns each child set's slot storage inside the
fixed list and projects the parent value from those child views. Dynamic ``TSL``
storage is grow-only: output indexing can allocate new children, but shorter-list
value copies are rejected until the ``TSL`` delta schema can represent removals.

**Dict (``TSD<K, V>``) — available and recursive.** ``In`` derives from
``TSDInputView`` and adds typed key lookup. ``contains(key)`` and
``find_slot(key)`` are typed; ``at(key)`` / ``operator[](key)`` return
``In<"", V>`` for an existing child. Iteration helpers such as ``valid_items()``,
``modified_items()``, ``added_items()``, and ``removed_items()`` return
``(ValueView key, In<"", V> child)`` pairs. ``Out`` derives from
``TSDOutputView``; ``out[key]`` creates the child if needed and returns
``Out<V>``, with ``set(key, value)`` / ``apply(key, value_view)`` as scalar-child
conveniences.

.. code-block:: cpp

   // TSD of scalar TS values
   struct SumValues
   {
       static void eval(In<"d", TSD<Str, TS<Int>>> d, Out<TS<Int>> out)
       {
           Int total = 0;
           for (auto &&[key, v] : d.valid_items()) total += v.value();
           out.set(total);
       }
   };

   struct SetValue
   {
       static void eval(In<"key", TS<Str>> key, In<"value", TS<Int>> value,
                        Out<TSD<Str, TS<Int>>> out)
       {
           out[key.value()].set(value.value());
       }
   };

``TSD`` replay/record uses the canonical
``Bundle{removed: Set<K>, modified: Map<K, delta(V)>}`` delta. Build expected
test deltas with ``dict_delta<K, V>``; ``V`` can itself be any supported
non-``REF`` time-series, including ``TSB``.

**Window (``TSW<T, Period, MinPeriod>``) — available for tick-count windows.**
``In`` derives from ``TSWInputView`` and adds typed ``at(i)``, ``operator[](i)``,
``front()``, and ``back()`` accessors. ``Out`` derives from ``TSWOutputView`` and
adds ``push(value)`` / ``apply(value_view)``. The per-tick delta is the scalar
element pushed in the current cycle.

.. code-block:: cpp

   struct PushWindow
   {
       static void eval(In<"x", TS<Int>> x, Out<TSW<Int, 3, 1>> out)
       {
           out.push(x.value());
       }
   };

   struct WindowSum
   {
       static void eval(In<"w", TSW<Int, 3, 1>> w, Out<TS<Int>> out)
       {
           Int total = 0;
           for (std::size_t i = 0; i < w.size(); ++i) total += w[i];
           out.set(total);
       }
   };


References and signals
----------------------

``SIGNAL`` is available. It is a valueless tick: a node depends only on *that
something changed*, not on a value. ``In<..., SIGNAL>::ticked()`` reports whether
the signal ticked in the current cycle, and ``Out<SIGNAL>::tick()`` emits one.
Wiring treats ``SIGNAL`` specially on the input side: any time-series output can
be connected to an ``In<..., SIGNAL>``, and the input observes the upstream
modified/ticked state without reading the upstream value.

``REF<TSchema>`` selectors are available as an opaque reference-token surface. A
reference passes a handle to a time-series rather than its value, used to rebind
what an input points at without copying data. Node authoring code may read the
``TimeSeriesReference`` token from ``In<..., REF<TSchema>>::value()`` and may write
one with ``Out<REF<TSchema>>::set(...)``.

Direct dereference of a ``REF`` output is intentionally not part of the public C++
node-authoring API. For now, dereferencing is owned by the internal time-series
alternative binding machinery: when an input expects ``TSchema`` and an output is
``REF<TSchema>``, the output alternative code exposes the referenced shape and
keeps the binding updated. This decision may be revisited if a valid user-code
case appears that cannot be expressed through normal wiring.

.. code-block:: cpp

   struct CountTicks
   {
       static void eval(In<"trigger", SIGNAL> trigger, State<Int> n, Out<TS<Int>> out)
       {
           if (trigger.ticked())
           {
               n.set(n.get() + 1);
               out.set(n.get());
           }
       }
   };

.. code-block:: python

   @compute_node
   def count_ticks(trigger: SIGNAL, _state: STATE = None) -> TS[int]:
       _state.n += 1
       return _state.n


Injected services — global state, clock and scheduler
-----------------------------------------------------

A node can ask for runtime services by listing them as parameters, exactly as
Python injects ``_clock`` / ``_scheduler``. Injectables are **not** part of the
node's data contract — they add no input, output, scalar or state, and do not
affect node-kind inference; the node simply receives them at evaluation.
``GlobalStateView``, ``EvaluationClockView``, ``EngineControlView``,
``LoggerView`` and ``NodeScheduler`` are implemented:

.. list-table::
   :header-rows: 1
   :widths: 34 33 33

   * - Service
     - C++ selector
     - Python injectable
   * - global state
     - ``GlobalStateView`` *(available)*
     - ``_global_state: GlobalState``
   * - node scheduler
     - ``NodeScheduler`` *(available)*
     - ``_scheduler: SCHEDULER``
   * - one-shot scheduler
     - ``SingleShotScheduler`` *(available, C++ only)*
     - —
   * - current time
     - ``DateTime`` *(available)*
     - ``_clock.evaluation_time``
   * - evaluation clock
     - ``EvaluationClockView`` *(available)*
     - ``_clock: CLOCK``
   * - engine control
     - ``EngineControlView`` *(available)*
     - ``_engine: EvaluationEngineApi``
   * - run logger
     - ``LoggerView`` *(available)*
     - ``logger: LOGGER``

``EvaluationClockView`` is a borrowed read-only view over the active evaluation
clock. It exposes ``evaluation_time()``, ``now()``, ``cycle_time()`` and
``next_cycle_evaluation_time()``. ``DateTime`` remains available as a shorthand
injectable for ``clock.evaluation_time()``.

``EngineControlView`` is a borrowed projection over the root executor. It
exposes the evaluation mode, start/end bounds, evaluation clock, stop state,
and ``request_stop()``. The registered C++ ``stop_engine`` sink uses this view;
a request completes the current evaluation cycle before ending the run.

``LoggerView`` borrows the executor-owned spdlog logger. Configure it with
``GraphExecutorBuilder::logger``; the executor retains shared ownership while
root and nested graphs cache only the raw pointer. Injecting or writing a log
does not add node storage or perform reference counting on the evaluation path.

``GlobalStateView`` is a borrowing **view** over the graph's shared, mutable
``string -> value`` store. The root graph owns the run-time state, initialized by
copying the builder's wiring-time seed. A node that declares the view can read and
write that graph-owned copy during evaluation:

.. code-block:: cpp

   struct EmitSeed
   {
       static void eval(GlobalStateView gs, Out<TS<Int>> out)
       {
           out.set(gs.get_as<Int>("seed"));   // read a value seeded at wiring time
           gs.set("emitted", Value{Int{1}});       // ...and write back into the store
       }
   };

The store is seeded at wiring time through ``GraphBuilder::global_state()`` /
``Wiring::global_state()`` (and read back after a run via
``GraphView::global_state()``); see *Wiring Graphs in C++*. Values are
heterogeneous — each key may hold a differently-typed value (it is a mutable
``Map<string, Any>`` under the hood).

``GlobalContext`` can begin the seed lifetime before top-level wiring is created.
It selects one caller-owned seed on the current thread; a new ``Wiring`` or direct
``GraphBuilder`` copies it and the graph then follows the same ownership and copy
rules described above. Contexts do not nest and are not retained by the graph.

``NodeScheduler`` re-arms the **current** node for a future cycle. It mirrors the
Python ``SCHEDULER`` interface and is, like ``GlobalStateView``, a **value/view
split**: the persistent per-node footprint (a ``NodeSchedulerState`` — the pending
``(time, tag)`` events and the ``tag -> time`` index) lives on the node, while
``NodeScheduler`` is the borrowing **view** that is constructed on demand when the
parameter is injected (so a node that never schedules carries no scheduler context
in memory). A source that reschedules itself is how a graph ticks over simulated
time (the data-driven, multi-cycle counterpart to a one-shot constant source).

.. important::

   **Nodes are not scheduled by default.** A node only evaluates when something
   schedules it: a compute/sink node when one of its inputs ticks, and a **source
   when it schedules itself at start**. There are three ways to do the initial
   scheduling, from simplest to most capable:

   1. ``static constexpr bool schedule_on_start = true;`` — a declarative
      attribute; the framework schedules the node for the start cycle. No hook, no
      injectable, no per-node state. This is the right choice for almost every
      source (it just means "tick me at start").
   2. ``start(SingleShotScheduler s)`` — a lightweight, stateless one-shot
      scheduler for when the *initial* schedule needs a specific time:
      ``s.schedule_now()``, ``s.schedule(delta)`` or ``s.schedule(when)``. No
      cancellation, no tags, no query, no per-node state.
   3. ``NodeScheduler`` — the full, stateful scheduler (tags, cancellation,
      rescheduling each cycle). Use it in ``eval`` for a source that re-arms over
      time; it allocates a per-node ``NodeSchedulerState``.

   These compose: a multi-cycle source typically uses ``schedule_on_start`` for the
   first tick and ``NodeScheduler`` in ``eval`` to re-arm. (During ``start`` the
   scheduler also allows scheduling the current cycle; during ``eval`` it is
   future-only. This matches 2603, where the generator's framework ``start`` does
   the initial scheduling.)

.. code-block:: cpp

   struct Ticker
   {
       static constexpr bool schedule_on_start = true;   // first tick at start
       static void eval(NodeScheduler sched, State<Int> n, Out<TS<Int>> out)
       {
           out.set(n.get());
           n.set(n.get() + 1);
           sched.schedule(MIN_TD);   // re-arm for the next cycle (delta from now)
       }
   };

.. code-block:: python

   @generator
   def ticker(_scheduler: SCHEDULER = None) -> TS[int]:
       n = 0
       while True:
           yield _scheduler.next_tick(), n
           n += 1

The full interface:

.. list-table::
   :header-rows: 1
   :widths: 42 58

   * - Member
     - Meaning
   * - ``now()``
     - the current evaluation time.
   * - ``schedule(when[, tag][, on_wall_clock])``
     - schedule at an absolute ``DateTime`` ``when`` (must be in the future).
   * - ``schedule(delta[, tag][, on_wall_clock])``
     - schedule ``delta`` (``TimeDelta``) after ``now()``.
   * - ``is_scheduled()`` / ``is_scheduled_now()``
     - whether anything is pending / the earliest event is *exactly* this cycle.
   * - ``next_scheduled_time()``
     - the earliest pending time (``MIN_DT`` when empty).
   * - ``has_tag(tag)`` / ``tag_time(tag[, default])``
     - whether / when a tagged event is registered.
   * - ``tag_is_scheduled_now(tag)``
     - whether a tagged event is due this cycle.
   * - ``pop_tag(tag[, default])``
     - remove a tagged event and return its time.
   * - ``un_schedule(tag)`` / ``un_schedule()``
     - cancel a tagged event / the earliest event.
   * - ``reset()``
     - cancel everything.

A ``tag`` names an event so it can be replaced (re-scheduling the same tag moves
it rather than adding a second event), queried, or cancelled. When a node fires
because its timer was due (``is_scheduled_now``), the runtime consumes the events
that fired this cycle and re-arms the node at the next pending time; when it fires
for another reason (an input ticked) it simply re-arms the next timer — so an
``eval`` only needs to (re)schedule future work. This matches the authoritative
Python ``SCHEDULER`` semantics. Wall-clock alarms (``on_wall_clock = true``) are
supported on **real-time** graph executors, where engine time is
wall-clock-aligned; a simulation executor rejects them, because simulated time
cannot be advanced by host time. Calling a mutating method on a node that did
not declare a ``NodeScheduler`` throws.

The interface follows the Python ``SCHEDULER`` contract (the authoritative
reference). ``tag_time`` / ``tag_is_scheduled_now`` are convenience accessors over
the same tag index; there is no ``schedule_immediate`` (a node re-arms itself with
a future ``schedule``).


Scalar values and arguments
---------------------------

**Available as ``eval`` arguments.** Not every value in HGraph is a time-series.
A *scalar* is a plain value (an ``Int``, ``Float``, ``Str``, …).
``Scalar<"name", T>`` is the scalar analog of ``In`` — like ``In`` it carries a
**name and type** (read it with ``.value()``) — and it is a first-class node
parameter alongside ``In`` / ``Out`` / ``State``. Scalars are **read-only**
per-instance configuration: they are fixed when the node is built and do not
change during evaluation.

It covers scalar **arguments** fixed at wiring time (the C++ counterpart of an
ordinary, non-time-series Python function argument). Push-source message
application is planned as part of the specialized push-source node/builder, not
as a generic static-node scalar argument path.

.. code-block:: cpp

   struct Scale
   {
       static void eval(In<"in", TS<Float>> in, Scalar<"factor", Float> factor, Out<TS<Float>> out)
       {
           out.set(in.value() * factor.value());
       }
   };

Each ``Scalar<>`` parameter becomes a field of the node's compound scalar
configuration; the values are supplied when the node is built. The scalars are
**not** part of the input TSB, so they never affect node-kind inference (a node
with only ``Scalar`` inputs and an ``Out`` is still a pull source). You supply the
values when wiring the node — ``wire<T>(w, ports…, scalars…)`` (see *Wiring Graphs
in C++ > Configuring a node with scalars*) — and equal scalars fold into the wiring
intern key, so nodes that differ only in a scalar value stay distinct.

.. code-block:: python

   @compute_node
   def scale(in_: TS[float], factor: float) -> TS[float]:
       return in_.value * factor

For a scalar that is a **Python object** (for Python user nodes), the sibling
``PythonScalar<"name", Type<"my.module.MyType">>`` carries the raw Python value
and names its expected Python type as a string, so the wiring layer can
type-check it — the Python-typed counterpart of ``Scalar<"name", T>``. If the
type is omitted (``PythonScalar<"name">``), it defaults to ``object`` — any
Python object, i.e. un-typed / generic.


Recordable state
----------------

``RecordableState<TSchema>`` is available as node-local state backed by a hidden
time-series output. It is feedback-like state that wraps the node: the node may
read and update it during evaluation, while system-level record/replay code can
observe and restore it. It is not part of the normal output contract and it does
not participate in scheduling or input readiness for the owning node. A node uses
either ``State<T>`` or ``RecordableState<TSchema>``, not both; recordable state is
the node's state when record/replay visibility is required.

The selector uses typed field access for structured state. For a bundle-shaped
state, access fields with ``field<"...">()`` and update scalar fields with
``set(...)``.

C++ system wiring can deliberately extract the hidden output with
``recordable_state(port)``. The related ``error_output(port)`` helper exposes a
node's hidden error output. Both helpers create special edge source roots; they
do not treat hidden outputs as ordinary child paths. Automatic Python-style
record/replay attachment using the node's recordable id is still planned.

.. code-block:: cpp

   using LastSeen = TSB<"LastSeen", Field<"last", TS<Int>>>;

   struct PreviousValue
   {
       static void eval(In<"in", TS<Int>> in,
                        RecordableState<LastSeen> state,
                        Out<TS<Int>> out)
       {
           auto last = state.field<"last">();
           out.set(last.valid() ? last.value().checked_as<Int>() : Int{-1});
           last.set(in.value());
       }
   };

.. code-block:: python

   @compute_node(recordable_id="previous_value")   # recordable_id is optional
   def previous_value(in_: TS[int], _state: RECORDABLE_STATE[LastSeen] = None) -> TS[int]:
       out = _state.last if _state.last is not None else -1
       _state.last = in_.value
       return out


Activity and validity policies
------------------------------

**Available (policy flags on ``In``).** By default a compute node evaluates when
an *active* input ticks, and only once all top-level inputs are *valid*. These
policies can be tuned per input — the C++ form attaches policy flags to ``In``,
the Python form lists them on the decorator.

``InputActivity::Passive``
   The input may be read by the node, but ticks on that input do not by
   themselves schedule evaluation. Omitting an activity flag is
   ``InputActivity::Active``.

Calling ``make_active()`` or ``make_passive()`` only changes the input's
subscription state. It never schedules the node and never changes the input's
``modified()`` result. Static C++ nodes with an active ``REF`` input receive one
separate startup sample because the initial reference binding is established
before graph start; later active REF rebinds notify through the normal input
subscription.

``InputValidity::Unchecked``
   The input does not participate in the readiness check. The node body must
   check ``valid()`` before reading its value.

``InputValidity::AllValid``
   The readiness check requires the input to be recursively valid, for example
   every child of a collection or bundle must be valid. Omitting a validity flag
   is ``InputValidity::Valid``.

The flags are order-independent, and at most one activity flag and one validity
flag may be supplied per input. Python ``active=()`` is expressed by marking
every input ``InputActivity::Passive``. Python ``valid=()`` is expressed by
marking every input ``InputValidity::Unchecked``.

.. code-block:: cpp

   struct Sample
   {
       // 'signal' drives evaluation; 'value' is read but does not by itself trigger.
       static void eval(In<"signal", SIGNAL> signal,
                        In<"value", TS<Int>,
                           InputActivity::Passive,
                           InputValidity::Unchecked> value,
                        Out<TS<Int>> out)
       {
           (void) signal;
           if (value.valid()) { out.set(value.value()); }
       }
   };

.. code-block:: python

   @compute_node(active=("signal",), valid=("value",))
   def sample(signal: SIGNAL, value: TS[int]) -> TS[int]:
       if value.valid:
           return value.value


Generics and type variables
---------------------------

HGraph nodes can be generic over a scalar type or a time-series type, resolved at
wiring time. The Python package uses the type variables ``SCALAR``,
``TIME_SERIES_TYPE``, ``K``, ``V`` (and friends). The C++ markers are
``ScalarVar<"Name">`` and ``TsVar<"Name">``.

**Available:** authoring a node over ``ScalarVar`` / ``TsVar`` and resolving the
variables at wiring time. A node is written **once** (no per-type instantiation);
``wire<>`` resolves each variable — unifying input selectors against the connected
port's schema, inferring a scalar variable from the configured value, or taking an
explicit output type — and builds the concrete node. A ``TsVar`` ``In`` / ``Out``
*is* the erased view (it has no typed ``value()`` / ``set()`` — there is no concrete
element type yet), so the body is driven by the runtime ``capture_delta`` /
``apply_delta`` (``<hgraph/types/time_series/ts_delta.h>``). The framework's own
``replay`` / ``record`` / ``const_`` / ``debug_print`` / ``null_sink`` are authored
exactly this way. ``REF`` is a separate binding surface and is not part of erased
value replay.

A passthrough generic over any time-series type:

.. code-block:: cpp

   struct passthrough
   {
       static void eval(In<"in", TsVar<"T">> in, Out<TsVar<"T">> out)
       {
           Value delta = capture_delta(in.base());  // 'T' resolved at wiring
           apply_delta(out, delta.view());
       }
   };

.. code-block:: python

   from hgraph import compute_node, TIME_SERIES_TYPE

   @compute_node
   def passthrough(in_: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
       return in_.delta_value

A variable may name its closed set of accepted schemas.  Constraint lists are
enforced identically when a static node is wired directly and when its pattern
participates in operator dispatch:

.. code-block:: cpp

   using SupportedSeries = TsVar<"S", TS<Int>, TS<Str>>;
   using SupportedScalar = ScalarVar<"T", Int, Str>;

   struct constrained_passthrough
   {
       static void eval(In<"in", SupportedSeries> in, Out<SupportedSeries> out)
       {
           Value delta = capture_delta(in.base());
           apply_delta(out, delta.view());
       }
   };

.. code-block:: python

   from typing import TypeVar

   SupportedSeries = TypeVar("SupportedSeries", TS[int], TS[str])
   SupportedScalar = TypeVar("SupportedScalar", int, str)

   @compute_node
   def constrained_passthrough(ts: SupportedSeries) -> SupportedSeries:
       return ts.delta_value

   @compute_node
   def constrained_scalar(ts: TS[SupportedScalar]) -> TS[SupportedScalar]:
       return ts.value

A node generic over a scalar type and a source output type (this is exactly
``stdlib::const_``) — the scalar variable ``T`` is inferred from the configured
value. If the caller does not supply an explicit output schema, the node's
default resolver binds ``S`` to ``TS<T>``; if the caller writes
``wire<stdlib::const_, SomeTS>(...)``, ``S`` is that explicit output schema and
the value must match ``SomeTS``'s current-value schema:

.. code-block:: cpp

   struct const_
   {
       static constexpr bool schedule_on_start = true;

       static void resolve_default_types(ResolutionMap& resolution)
       {
           const auto* value_schema = resolution.scalar("T");
           const auto* output_schema = resolution.find_ts("S");
           if (output_schema == nullptr)
           {
               resolution.bind_ts("S", TypeRegistry::instance().ts(value_schema));
           }
           else if (output_schema->value_schema != value_schema)
           {
               throw std::logic_error("const value schema does not match output value schema");
           }
       }

       static void eval(Scalar<"value", ScalarVar<"T">> value, Out<TsVar<"S">> out)
       {
           out.apply(value.value());   // erased copy of the configured value
       }
   };
   // wire<stdlib::const_>(w, Int{42});        // defaults to TS<Int>
   // wire<stdlib::const_, TSS<Int>>(w, stdlib::make_set<Int>({Int{1}, Int{2}}));

.. code-block:: python

   from hgraph import generator, TS, SCALAR

   @generator
   def const(value: SCALAR) -> TS[SCALAR]:
       yield MIN_ST, value

A dict-keyed generic uses ``ScalarVar`` / ``TsVar`` in C++ and ``K`` / ``V`` in
Python. The resolver/unifier recursion handles such composites (``K`` and ``V``
bind from the connected ``TSD`` port). Generic nodes that only need to forward
the per-tick delta can stay erased through ``capture_delta`` / ``apply_delta``:

.. code-block:: cpp

   struct dict_passthrough
   {
       static void eval(In<"d", TSD<ScalarVar<"K">, TsVar<"V">>> d,
                        Out<TSD<ScalarVar<"K">, TsVar<"V">>> out)
       {
           Value delta = capture_delta(d.base());
           apply_delta(out.base(), delta.view());
       }
   };

.. code-block:: python

   @compute_node
   def dict_passthrough(d: TSD[K, V]) -> TSD[K, V]:
       return d.delta_value


Operator resolution helpers
---------------------------

Most C++ nodes do not need custom resolution hooks: their ``In``/``Out`` and
``Scalar`` selectors are enough. Stdlib-style operator overloads sometimes need
``requires_`` and ``resolve_default_types`` hooks when the output depends on the
normalised call shape, for example ``TSL`` element type, ``TSD`` key type, or an
explicitly requested output schema.

Use ``<hgraph/lib/std/operators/impl/type_resolution_helpers.h>`` for these
hooks. The helpers keep the resolver and predicate null-safe and consistent.
Every extractor returns a pointer or ``nullptr``. ``nullptr`` means either the
argument is missing, the argument has the wrong surface shape, or the extracted
schema does not match the requested pattern. A resolver should return quietly on
``nullptr`` because ``resolve_default_types`` runs before ``requires_``.

The common extraction patterns are:

- ``arg_at(context, index)`` extracts the raw normalised ``WiringArg`` at a
  positional index. It returns ``nullptr`` when the call has fewer arguments.
- ``time_series_arg_at(context, index)`` extracts the raw argument only when it
  is a time-series port. It returns the ``WiringArg`` so callers can inspect
  call metadata such as ``from_variadic_tail``.
- ``scalar_arg_at(context, index)`` extracts the raw argument only when it is a
  scalar argument.
- ``time_series_schema_at(context, index)`` extracts the schema of a
  time-series argument. By default it dereferences ``REF<T>`` and returns
  ``T``; pass ``SchemaRefMode::Direct`` when the overload needs to distinguish
  an actual ``REF`` surface from its target.
- ``time_series_arg_of_kind(context, index, kind)`` extracts a time-series
  schema and keeps it only when ``schema->kind == kind``. This is the usual
  shape gate for ``TS``, ``TSS``, ``TSD``, ``TSL`` and ``TSB`` overloads.
- ``fixed_tsl_arg(context, index)`` extracts ``TSL[...]`` only when the list has
  a positive fixed size. Dynamic ``TSL`` and non-``TSL`` inputs return
  ``nullptr``.
- ``same_fixed_tsl_size(context, lhs, rhs)`` extracts two fixed ``TSL`` schemas
  and returns ``true`` only when both exist and have the same fixed size.
- ``all_time_series_args_of_kind(context, kind)`` extracts every supplied
  argument as a time-series schema of the requested kind. It returns ``false``
  for an empty call.
- ``ts_value_schema(ts_schema)`` extracts the current-value schema ``T`` from a
  plain ``TS<T>``. Collection time-series shapes return ``nullptr``.
- ``ts_map_value_schema(ts_schema)`` extracts the scalar value schema from
  ``TS<Map[K, V]]``. Non-map ``TS`` values and non-``TS`` schemas return
  ``nullptr``.

Output binding has two variants. Use the one that matches how the overload was
registered:

- Static-node overloads registered with ``register_overload`` normally declare
  an output such as ``Out<TsVar<"O">>``. Use
  ``local_output_bound(resolution, "O")`` and
  ``bind_local_output(resolution, output_schema, "O")``. This defaults only the
  local output variable.
- Graph overloads registered with ``register_graph_overload`` often return an
  erased ``WiringPortRef`` or ``Port<void>``. Their declared output pattern is
  the graph-output sentinel ``__out__``. Use ``output_bound(resolution)`` and
  ``bind_output(resolution, output_schema, "V")``. The optional local variable
  name also binds the graph's internal generic such as ``V``.

The bind helpers intentionally call ``ResolutionMap::bind_ts`` for local output
variables. If the caller supplied an explicit output type and the computed
default disagrees, the resolver throws, the candidate is rejected, and overload
resolution can continue.

Static-node example: default ``keys_(TS<Map[K, V]>)`` to
``TS<Set[K]>`` unless the caller has already requested a compatible output:

.. code-block:: cpp

   #include <hgraph/lib/std/operators/impl/type_resolution_helpers.h>

   namespace detail = hgraph::stdlib::operator_impl_detail;

   struct keys_map_scalar_like
   {
       static bool requires_(const ResolutionMap &resolution,
                             OperatorCallContext)
       {
           return detail::ts_map_value_schema(resolution.find_ts("S")) != nullptr;
       }

       static void resolve_default_types(ResolutionMap &resolution,
                                         OperatorCallContext)
       {
           if (detail::local_output_bound(resolution, "O")) { return; }

           const auto *map = detail::ts_map_value_schema(resolution.find_ts("S"));
           if (map == nullptr) { return; }

           auto &registry = TypeRegistry::instance();
           detail::bind_local_output(
               resolution,
               registry.ts(registry.set(map->key_type)),
               "O");
       }

       static void eval(In<"ts", TsVar<"S">> ts, Out<TsVar<"O">> out)
       {
           // ...
       }
   };

Graph-overload example: accept a fixed ``TSL`` and default the graph output to
the element time-series type:

.. code-block:: cpp

   #include <hgraph/lib/std/operators/impl/type_resolution_helpers.h>

   namespace detail = hgraph::stdlib::operator_impl_detail;

   struct reduce_tsl_like
   {
       static bool requires_(const ResolutionMap &, OperatorCallContext context)
       {
           return detail::fixed_tsl_arg(context, 0) != nullptr;
       }

       static void resolve_default_types(ResolutionMap &resolution,
                                         OperatorCallContext context)
       {
           if (detail::output_bound(resolution)) { return; }
           const auto *tsl = detail::fixed_tsl_arg(context, 0);
           if (tsl == nullptr) { return; }
           detail::bind_output(resolution, tsl->element_ts(), "V");
       }

       static auto compose(Wiring &w, NamedPort<"ts", TSL<TsVar<"V">>> ts)
       {
           // ...
       }
   };

The important pattern is to use the same extractor in ``requires_`` and
``resolve_default_types``. The resolver handles the early, possibly mismatched
call shape without throwing; the predicate makes the final overload decision
once default type variables have been filled.


Assembling and running a graph
------------------------------

**Available.** Nodes are assembled with ``GraphBuilder`` and connected with
``GraphEdge`` (an edge runs from a source node's output to a field of a target
node's input). The graph runs under a ``GraphExecutor`` in simulation mode.

.. code-block:: cpp

   #include <hgraph/runtime/runtime.h>
   #include <hgraph/types/static_node.h>

   using namespace hgraph;

   GraphBuilder builder;
   builder.add_node(NodeBuilder{}.label("src").implementation<ConstantSource>())   // -> TS<Int>
          .add_node(NodeBuilder{}.label("inc").implementation<Increment>())
          .add_edge(GraphEdge{.source_node = 0, .source_path = {},
                              .target_node = 1, .target_path = {0}});   // src -> inc."in"

   GraphExecutorBuilder executor;
   executor.graph_builder(std::move(builder))
           .start_time(MIN_ST)
           .end_time(MIN_ST + TimeDelta{10});

   executor.make_executor().view().run();

.. code-block:: python

   from hgraph import graph, run_graph, EvaluationMode

   @graph
   def my_graph():
       inc = increment(constant_source())
       # ... sinks, etc.

   run_graph(my_graph, run_mode=EvaluationMode.SIMULATION,
             start_time=start, end_time=end)

In Python, wiring is implicit: calling ``increment(constant_source())`` *is* the
edge. In C++ today you name nodes and edges explicitly; a higher-level fluent
wiring layer that infers edges from call structure is planned.

.. note::

   **Source ticks over time.** A source that injects ticks across simulated time
   declares a ``NodeScheduler`` (or ``SingleShotScheduler``) injectable and
   schedules its next evaluation from ``start`` / ``eval``. The runtime consumes
   the scheduled event and re-arms the node through the scheduler state.


Feature status
--------------

.. list-table::
   :header-rows: 1
   :widths: 50 25 25

   * - Feature
     - C++
     - Python
   * - Compute / pull-source / sink nodes
     - available
     - available
   * - ``In<TS<T>>`` (value / modified / valid)
     - available
     - available
   * - ``Out<TS<T>>`` (set / tick)
     - available
     - available
   * - ``State<T>`` (scalar) + ``start`` / ``stop``
     - available
     - available
   * - Node-kind inference (from shape, no override)
     - available
     - available
   * - ``GraphBuilder`` / ``GraphEdge`` / ``GraphExecutor`` (simulation + real-time)
     - available
     - available
   * - Schema markers (``TS``/``TSS``/``TSD``/``TSL``/``TSW``/``TSB``/``REF``/``SIGNAL``)
     - available
     - available
   * - ``ScalarVar`` / ``TsVar`` markers + descriptors
     - available
     - available
   * - Source tick injection over time (self-rescheduling source)
     - available
     - available
   * - Set selectors (``In``/``Out`` over ``TSS``)
     - available
     - available
   * - Container selectors (``In``/``Out`` over ``TSB``/``TSL``/``TSD``/``TSW``)
     - available
     - available
   * - ``SIGNAL`` selectors
     - available
     - available
   * - ``REF`` selectors (opaque token; no public direct dereference)
     - available
     - available
   * - ``GlobalStateView`` injectable (shared ``string -> value`` store)
     - available
     - available
   * - ``NodeScheduler`` injectable (self-reschedule / multi-cycle sources)
     - available
     - available
   * - ``EvaluationClock`` injection
     - available as ``EvaluationClockView``
     - available
   * - Engine control / ``stop_engine``
     - available as ``EngineControlView`` and the native sink
     - available as ``EvaluationEngineApi`` and native-operator wiring
   * - ``Scalar<"name", T>`` (named scalar arguments)
     - available
     - available
   * - ``RecordableState`` selector
     - available
     - available
   * - Explicit ``recordable_state(port)`` / ``error_output(port)`` C++ helpers
     - available
     - Python uses ``__state__`` / ``__error__``
   * - Automatic recordable-state recording
     - planned
     - available
   * - Activity / validity policy flags
     - available
     - available
   * - Generic node resolution (``TsVar`` / ``ScalarVar`` in signatures)
     - available
     - available
   * - Push-source node/builder + sender handle
     - available (runtime builder; static-node sugar planned)
     - available
   * - Named state ``State<S, "name">``
     - planned
     - available
   * - Fluent / implicit edge wiring
     - planned
     - available


C++ ↔ Python cheat sheet
------------------------

.. list-table::
   :header-rows: 1
   :widths: 50 50

   * - C++
     - Python
   * - ``struct N { static void eval(...){} };``
     - ``@compute_node`` / ``@generator`` / ``@sink_node`` function
   * - ``In<"x", TS<Int>> x`` → ``x.value()``
     - ``x: TS[int]`` → ``x.value``
   * - ``x.modified()`` / ``x.valid()``
     - ``x.modified`` / ``x.valid``
   * - ``Out<TS<Int>> out`` → ``out.set(v)``
     - ``-> TS[int]`` → ``return v``
   * - ``State<Int> s`` → ``s.get()`` / ``s.set(v)``
     - ``_state: STATE[...]`` → ``_state.field``
   * - ``EvaluationClockView``
     - ``_clock: EvaluationClock``
   * - ``NodeScheduler sched`` → ``sched.schedule(...)``
     - ``_scheduler: SCHEDULER``
   * - ``Scalar<"f", Float>``
     - plain arg ``f: float``
   * - ``TsVar<"T">`` / ``ScalarVar<"T">`` (resolved at wiring)
     - ``TIME_SERIES_TYPE`` / ``SCALAR``
   * - ``eval`` with ``Out`` and no ``In`` (kind inferred)
     - ``@generator``
   * - ``make_push_source_node`` + ``PushSourceSender``
     - ``@push_queue`` message + ``sender`` callable
   * - ``NodeBuilder{}.implementation<N>()``
     - the decorator applied to the function
   * - ``GraphEdge{...}`` / ``GraphBuilder::add_edge``
     - calling one node's output into another (implicit)
   * - ``GraphExecutor`` + ``run()`` (simulation / real-time)
     - ``run_graph(..., run_mode=EvaluationMode.SIMULATION|REAL_TIME)``
