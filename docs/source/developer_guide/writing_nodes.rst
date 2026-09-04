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

**Where the rule is applied.** At the point arguments are BOUND, not in each
consumer:

* ordinary inputs — ``adapt_source_for_input`` installs the adaptation. A
  peered *reference* source (a top-level ``REF``) is described by its value
  schema whenever the declared input contains no ``REF``, so a hand-wired
  consumer that builds its node schema from the port (``reduce``, ``mesh_``)
  sees the value it will observe; input binding installs the from-REF link.
  Only the top-level reference is rewritten: a value collection whose
  elements are references (a ``map_`` output) is left as supplied, because
  its element links dereference on access and describing it by element
  values would install a from-REF link per element for a consumer that may
  only hold tokens (an interleaved A/B against ``main`` showed no runtime
  difference either way; the narrower rewrite is the minimal adaptation).
  The rule follows structural ``TSL``/``TSB``
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

Structure-preserving packing is different: ``tsb_itemwise`` and the
``map_`` / ``switch_`` / ``mesh_`` machinery pass references deliberately —
they route values rather than consuming them — so they are *not* covered by
this rule.

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
