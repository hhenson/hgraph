Writing nodes
=============

Rules for authoring a node implementation (an ``*_impl`` struct). Each one
exists because breaking it has a cost that is not obvious at the call site —
the cost is recorded with the rule, so a future change can weigh it rather
than rediscover it.

This is the *authoring* layer. :doc:`operators` covers the operator model and
overload ranking; :doc:`graph_wiring` covers how a wired graph is built.

Declare defaults as a ``std::tuple`` of ``arg<>``
-------------------------------------------------

.. code-block:: cpp

   // Do this.
   static auto defaults()
   {
       return std::tuple{arg<"key">(Str{"out"}), arg<"mode">(ToTableMode::Tick)};
   }

   // Not this.
   static std::vector<std::pair<std::string_view, Value>> defaults()
   {
       return {{"key", Value{Str{"out"}}}, {"mode", Value{ToTableMode::Tick}}};
   }

**Why.** Both forms are accepted by ``apply_param_defaults``, so the runtime
operator registry does not care. Direct static-node wiring does:

.. code-block:: cpp

   wire<stdlib::replay_impl, TS<Int>>(w, std::string{"in"});

``wire<Impl>`` resolves which parameters have defaults **at compile time**,
through ``call_args_detail::default_arg_index``, which needs
``std::tuple_size_v`` over the defaults type. A ``std::vector`` has no
``tuple_size``, so the whole ``constexpr`` path collapses and the error arrives
as a wall of template substitution failures in ``graph_wiring.h`` — nowhere near
the ``defaults()`` that caused it.

The consequence is that the vector form silently makes an impl **un-wirable
directly**. Nothing declares that restriction, and an impl only reachable
through operator dispatch today may need direct wiring tomorrow. The tuple form
works in both paths, so there is no case where the vector form is the better
choice.

Two details worth knowing:

* ``arg<"name">(x)`` stores ``x`` and the registry wraps it as ``Value{x}``. A
  computed ``Value`` works too — ``arg<"names">(empty_names())`` — because
  ``Value`` is copy-constructible.
* ``Value{}`` is a deliberately **empty** default, meaning Python's ``None``
  (an unwired time-series source). Keep it as ``arg<"opt">(Value{})``; do not
  collapse it to ``arg<"opt">({})``.

Every scalar in ``start`` must also appear in ``eval``
------------------------------------------------------

A scalar parameter read in ``start`` but absent from ``eval`` fails node
registration with *"static node hook scalar is absent from the eval
signature"*. If ``eval`` does not use it, take it and discard it:

.. code-block:: cpp

   static void eval(Scalar<"frame_prefix", Str> frame_prefix, /* ... */)
   {
       // Resolved into the recorder's shape at start; the row walk reads that
       // shape from the handle, not from the argument.
       static_cast<void>(frame_prefix);
   }

**Why.** The hook signatures are the single description of a node's parameters.
Allowing them to disagree would mean a parameter's existence depended on which
hook you read.

A scalar default must not be empty
----------------------------------

The operator registry rejects an unset default outright. An unset value means
Python's ``None``, which only carries meaning for a time-series parameter — so
for a scalar it is a mistake rather than a value. For a container-typed scalar,
build the real empty container:

.. code-block:: cpp

   // tuple[str, ...] meaning "rename nothing".
   arg<"partition_names">(empty_names())

A generic time-series parameter binds the DEREFERENCED type
-------------------------------------------------------------

``REF`` is always explicit. A parameter declared as a generic time-series —
``TIME_SERIES_TYPE``, ``TsVar<"S">``, or an unconstrained ``**kwargs`` — binds
the type with every reference followed, recursively through container schemas.
A consumer that wants the reference token itself says so: ``REF[TS[int]]``,
``REF[TIME_SERIES_TYPE]``.

.. code-block:: cpp

   In<"ts", TsVar<"S">>        // the VALUE, however many refs reach it
   In<"ts", REF<TsVar<"S">>>   // the reference token

**Why.** A reference is a routing detail — how a value is reached, not what it
is. An operator that consumes values (formats, serialises, compares, records)
and is handed a ref token produces *plausible nonsense* rather than failing:
``log_`` printed the token, and ``combine[TS[JSON]]`` serialised ``"<ref>"``
where the value belonged. Nothing raises, and the output looks like data.

**Where the rule is applied.** Where a generic is RESOLVED and where
arguments are BOUND, not in each consumer:

* generic resolution — the matcher's variable case (``ts_pattern_match``,
  and the named-``TSB`` schema variable of ``input_ts_pattern_match``) and
  the static unifier's ``TsVar`` bind ``TypeRegistry::dereference(concrete)``,
  the *deep* dereference: a ``TSD[str, REF[TS[int]]]`` (a ``map_`` output)
  binds ``TSD[str, TS[int]]``, and input binding then installs the from-REF
  link per element. Whenever a generic is resolved, everything is
  dereferenced (owner ruling 2026-09-24, #847). Before this, the matcher
  stripped only the outer reference, so ``debug_print`` over a ``map_``
  output printed each element's reference token instead of its value;
* ordinary inputs — ``adapt_source_for_input`` installs the adaptation. A
  peered *reference* source (a top-level ``REF``) is described by its value
  schema whenever the declared input contains no ``REF``, so a hand-wired
  consumer that builds its node schema from the port (``reduce``, ``mesh_``)
  sees the value it will observe; input binding installs the from-REF link.
  That description rewrites only the top-level reference: a value
  collection whose elements are references is described as supplied, and a
  declared input that resolved a generic already names the dereferenced
  elements (previous bullet). The rule follows structural ``TSL``/``TSB``
  sources down to their leaves but stops at a declared ``REF``: the children
  of a source adapted to ``REF[X]`` keep their reference identity, which is
  how ``race`` observes a candidate going invalid. Before this was made
  structural (#649, 2026-09-04) such consumers saw ``REF[TSD]`` and either
  rejected it at wiring or failed at runtime on the ops kind;
* variadic tails — ``operator_dispatch_detail::value_argument`` dereferences
  unless the declared schema is ``REF<...>``;
* an UNTYPED ``VarKwIn<Name>`` — nothing in a bare collector could ask for a
  reference, so every collected port is dereferenced.

Putting it there is what keeps it from being rediscovered one operator at a
time. Two consumers had already been fixed individually before the rule was
made structural, and a third (``combine[TS[JSON]]``) was still wrong.

A **typed** ``VarKwIn<Name, Schema>`` is the exception, and for the same reason
the rule exists: the declaration wins. Its pack schema has already been matched
at dispatch against the supplied keywords, so a ``REF`` field in that pack is an
explicit request. Dereferencing there would strip it and leave output resolution
describing a reference the implementation never receives — the rule's own
failure mode, inverted.

**If you replace ``OperatorImpl::wire``, you own the rule.** An overload may
substitute a hand-written wire closure for the generated one — ``apply_`` does,
because the resolved output schema has to reach the packed runtime node. That
closure receives the raw ``WiringArg`` span, so ``operator_dispatch`` never
binds its arguments and nothing applies the rule for it. Call
``operator_dispatch_detail::value_argument`` on what the signature it stands in
for declares:

.. code-block:: cpp

   // apply_value_callable_signature declares VarIn<"args", TsVar<"S">>.
   using apply_args = VarIn<"args", TsVar<"S">>;
   positional.push_back(
       operator_dispatch_detail::value_argument<apply_args::schema_type>(as_port(args[index])));

This is the only category of exemption. ``log_`` / ``print_`` / ``format_``
declare ordinary ``VarIn`` / ``VarKwIn`` selectors and are covered — they each
used to dereference by hand, and those calls were removed once the rule was
structural.

**Code that depends on a reference expresses it.** A generic never binds
one by accident, so every site that needs the token says so, in one of these
ways:

* a ``REF`` pattern -- ``REF<TsVar<"S">>``, ``TSD<K, REF<TsVar<"V">>>`` --
  binds the variable under the reference;
* an explicit schema -- a variable bound up front (an initial resolution,
  ``wire(..., __resolutions__=...)``) is the caller stating the schema, so
  the matcher accepts the supplied port as it is -- a bare variable and a
  ``TSB`` schema variable alike, a top-level ``REF`` included. The static
  unifier does the same for a variable an explicit output schema bound
  first (``wire<passthrough, REF<TS<Int>>>(w, ref_port)``). The Python node wrapper
  does this: its native ``args`` is a generic pack, and it pre-binds the
  pack to the node's declared inputs (``_declared_args``), so a Python node
  that declares ``REF[TS[int]]`` receives the reference;
* a requested output -- ``output_ts_pattern_match`` keeps a requested
  schema that contains a ``REF`` at any depth verbatim
  (``nothing[TSD[str, REF[TS[int]]]]`` produces what it names) and binds a
  ``TSB`` schema variable as requested; ``ts_output_unifier`` does both for
  the static path's explicit output schema
  (``wire<replay_impl, TSD<Str, REF<TS<Int>>>>``);
* a structural projection -- an operator that selects part of a port
  without consuming it takes an erased port (``Port<void>``, or
  ``NamedPort<"ts", void>`` to keep a public parameter name), which
  resolves no generic and is passed the port as supplied. ``getitem_`` and
  ``getattr_`` on a ``TSB`` do this, so ``tsb["x"]`` and
  ``getattr_(tsb, "x")`` on a field declared ``REF[TS[int]]`` return that
  reference, as Python's ``tsb.x`` fast path does.

The ``map_`` / ``switch_`` / ``mesh_`` machinery and ``tsb_itemwise`` route
references deliberately: they build their schemas from the ports as supplied
and do not resolve a generic over them.

**The matcher and unifier contract.** A generic is resolved in two places
that must agree: the runtime matcher (``type_pattern.cpp``: operator
dispatch, type arguments, Python wiring) and the static unifier
(``type_resolution.h``: ``wire<X>``). Both implement these rules, and a
change to one side is made to the other in the same change:

.. list-table::
   :header-rows: 1
   :widths: 30 35 35

   * - Case
     - Runtime matcher
     - Static unifier
   * - A variable (bare or ``TSB`` schema variable, at any depth) binds a
       supplied input
     - ``TypeRegistry::dereference`` of the supplied schema
     - the same (``ts_unifier``)
   * - The variable is already bound (an initial resolution; statically, an
       explicit output schema bound first)
     - matches the schema as supplied, a top-level ``REF`` included, or
       dereferenced; constraints and the ``TSB`` kind are still checked
     - the same: only the dereference is skipped
   * - A requested output whose top-level pattern is a variable (bare or
       ``TSB`` schema variable)
     - binds the requested schema verbatim, a ``REF`` at any depth kept; an
       earlier binding must be schema-equivalent to it
       (``output_ts_pattern_match``)
     - the same (``ts_output_unifier``); a conflicting earlier binding throws
   * - A variable nested in a structural requested output
     - binds dereferenced, as an input's does
     - the same (``ts_output_unifier`` falls back to ``ts_unifier``)

``tests/cpp/test_operators.cpp`` ("resolving a generic dereferences
everything at every depth (#847)") covers each row on both sides.

**The owners.** Binding applies the rule to the argument it binds; an
operator that reasons about a schema binding never rewrites (a nested graph's
output, a collection element, the raw ports a ``compose`` receives) does not
dereference for itself either. It asks the owner of that question (RFC 0036,
``operators.rst`` "REF transparency"):

* what will this parameter observe -- ``NamedPort::observed()``, or the
  ``Dereference``-mode argument helpers of ``operator_type_resolution.h``;
* what is the element of this ``TSD`` / ``TSL`` -- ``TypeRegistry::value_element_ts``;
* are these two schemas the same value -- ``time_series_value_equivalent``;
* the referenced output behind a ``REF`` output, for a structural hop --
  ``TSOutputView::through_reference()``; whether a link's bound output can
  move -- ``TSInputView::bound_target_is_reference()``, recorded at bind;
* a reference to a possibly-referenced schema -- ``TypeRegistry::ref``,
  which is idempotent.

The ``stdlib-ref-dereference`` and ``paired-dereference-comparisons``
ratchets (``testing.rst``) hold the std operators at zero copies and the
comparison at its one owner; the runtime's and the Python wiring's copies
are RFC 0036's remaining PRs.

Guard overloads through one resolution point
--------------------------------------------

When several overloads of an operator select on the same piece of state, they
must all decide against the **same** answer, resolved in one place — see
``record_replay::call_model``.

**Why.** Overload guards have to stay mutually exclusive. If one guard consults
a call-site override and another reads the graph configuration directly, a call
supplying that override matches both overloads or neither, and overload
resolution reports the symptom without the cause.

Keep the per-tick path free of locks and ``shared_ptr``
--------------------------------------------------------

The ruling of 2026-07-02: value, time-series and runtime ops invoked during
evaluation are lock-free and ``shared_ptr``-free. Build-time machinery —
interning, plan and ops synthesis, registries — **may** use mutexes; that is
sanctioned rather than drift. Push-source senders and the real-time executor
condition variable remain the only cross-thread runtime boundary.

Resolve once in ``start``, read per tick
-----------------------------------------

Anything derivable from the resolved schema — a layout, a converter, a column
projection — is resolved in ``start`` and carried in ``State``, not recomputed
per tick. This is the lifecycle form of the builder pattern: compose once, read
many times.

**Value bindings in particular.** Resolving a value binding
(``value_type_for_active_realization``, ``ValuePlanFactory::type_for``,
``compact_list_type`` and friends) consults the realization snapshot or interns
a record under a counted type-system mutex, and so do the builders' plain
``build()`` calls, which re-intern the result type per call. A std operator
does neither: its bound output already carries everything it needs. In
``start`` it reads the ``ResolvedBindings`` helpers of
``lib/std/value_util.h`` off the output view -- ``resolve_list_bindings`` /
``resolve_set_bindings`` / ``resolve_map_bindings`` take the ``TSOutputView``
and answer the portable value type of an output that carries one
(``output_value_binding``: a TS, TSB or fixed TSL output's realized
binding, or the owning type a graph-local representation published when it
was realized; a TSS, TSD or dynamic TSL output's value surface is a
projection resolved per call, so it is refused) plus the element / key /
value bindings a compact container's plan carries
(``compact_element_binding`` / ``compact_map_bindings``); a TSS output's
element is its set layout's key binding; a fixed-array output answers its
element from its ops and publishes through a compact source list; a bundle's
field bindings come from ``BundleBuilder::field_binding``; a delta shape comes
from the layout's ``canonical_delta_binding`` -- and publishes per tick
through ``finish_list`` /
``finish_set`` / ``finish_map`` (``build_storage()`` plus the cached result
type). Read the state with ``State::ref()``; ``get()`` copies. A node whose
state already holds a queue or buffer keeps the bindings in the same struct
(one ``State`` per node). The ``stdlib-active-realization`` ratchet holds
the whole std library at zero realization lookups; the 2026-08-15 audit found
nine ``eval`` bodies (the tuple / frozenset / dict arithmetic, the
throttle's set netting, ``window`` and ``batch``) still paying it per tick,
and ``test_registry_snapshot.py``'s lock matrix now guards each of them. The
type layer's own ``capture_delta`` had the same flaw one level down
(``ts_delta.cpp`` resolved its bindings per call); it now builds through the
layout's canonical delta binding (:doc:`python_bridge`, "Delta capture is
registry-free"), guarded by the throttle families of the matrix.

**Why.** The 0.8.15 regression was exactly a per-tick resolution (see
:doc:`python_bridge`, "Per-tick application is registry-free"): a lock and a
hash lookup per value on every cycle, invisible to correctness tests. The
lock matrix is the guard because the cost is a *count*, not a failure.
