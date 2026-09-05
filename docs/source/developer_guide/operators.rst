Operators (overload dispatch)
=============================

An **operator** is a named operation with a *general* signature that collects
many concrete **implementations** and, when wired with concrete ports/scalars,
selects the **best-matching** one. It is the C++ counterpart of Python
``hgraph``'s ``@operator`` (and ``@compute_node(overloads=…)`` /
``@graph(overloads=…)``): one logical name (``add_``) stands for a family of
implementations, and the wiring layer resolves which to use.

This is the multi-candidate generalisation of ordinary node wiring. A plain
``wire<Node>(w, …)`` names one implementation directly; ``wire<add_>(w, a, b)``
names an operator and lets the **operator registry** choose among the
implementations registered under ``"add"``, ranked by how *specific* their
declared types are to the supplied arguments. Resolution is entirely
**wiring-time** — exactly like *Graph Wiring*, there is no runtime dispatch; the
chosen implementation is baked into the graph.

.. note::

   **Status.** Phase 1 is **implemented**: the runtime ``TypePattern`` interpreter
   (``include/hgraph/types/type_pattern.h`` + ``.cpp``), the ``OperatorRegistry`` /
   ``OperatorImpl`` / ``WiringArg`` and the ``wire<>`` operator arm
   (``include/hgraph/types/operator_dispatch.h`` + ``.cpp``), with the reset-listener
   hook — proven by ``tests/cpp/test_operators.cpp`` (specific-beats-generic, generic
   fallback, no-match and ambiguity errors). The ``TypePattern`` AST and its
   ``match`` / ``rank`` / ``resolve`` cover every static schema kind used by the C++
   wiring path (``TS`` / ``TSS`` / ``TSL`` / ``TSD`` / ``TSW`` / ``TSB`` / ``REF`` /
   ``Signal`` and scalar variants), including constrained ``TsVar`` /
   ``ScalarVar`` leaves. Phases 2–3 are **also implemented**:
   scalar-argument matching, scalar-to-time-series auto-const promotion,
   scalar-aware ``requires_`` / ``resolve_default_types`` hooks, graph overloads,
   sink operators, explicit output schema resolution, and a ``lib/std`` operator family
   in ``include/hgraph/lib/std/std_operators.h`` — covering scalar arithmetic,
   comparison, logical / bitwise, string operators (``match_`` / ``replace`` /
   ``substr`` / ``split`` / ``join`` / ``format_``), ``str_``, const / zero,
   collection container basics and TSS set algebra (``len_`` covers every
   sized shape — str / TSS / TSD / TSL of any element kind / TSB field count
   / TSW buffer length / sized container scalars; issue #81), stream basics
   (``sample`` / ``filter_`` / ``take`` / ``drop`` / ``step`` / ``slice_`` /
   ``dedup``), with numerical stream analytics supplied by the separate
   ``hgraph-analytics`` package,
   flow-control basics (``merge`` / ``all_`` / ``any_`` / ``if_true`` /
   ``if_then_else`` / ``if_cmp``), date / datetime / time / timedelta
   attribute operators (the upstream ``getattr_`` tables — ``year`` /
   ``month`` / ``day`` / ``hour`` / ``minute`` / ``second`` /
   ``microsecond`` / ``weekday`` / ``isoweekday`` / ``timestamp`` over
   ``TS<Date>`` / ``TS<DateTime>`` / ``TS<Time>``, ``days`` / ``seconds`` /
   ``microseconds`` / ``total_seconds`` over ``TS<TimeDelta>``; issue #82),
   time-series-property operators, and
   simple IO sink overloads, including
   homogeneous, mixed, heterogeneous-temporal, result-differs and optional-scalar
   (``DivideByZero`` policy) overloads — proven by
   ``tests/cpp/test_std_operators.cpp`` and the scalar / auto-const / ``requires`` /
   nested-collection / sink / graph-overload / alignment cases in
   ``tests/cpp/test_operators.cpp``. Still to come (see *Roadmap*): Phase 4 —
   the Python implementation path (behind ``HGRAPH_ENABLE_PYTHON_USER_NODES``). (The
   operator path's scalar-configuration
   bundle is assembled by the same ``BundleBuilder`` + ``scalar_schema(map)``
   mechanism the generic ``wire<>`` path uses — an erased-``Value`` field source on
   the operator side, a typed one on ``wire<>``'s; one mechanism, not a second
   resolver.)


One runtime model, not a second resolver
----------------------------------------

The guardrail for this subsystem (*CLAUDE.md* §3) is **one runtime model, no
parallel abstraction**. The operator registry is therefore an **index over
candidates plus a ranking loop** — it adds selection, *not* a second type
resolver or graph-building model:

- a candidate **resolves** into the **same** ``ResolutionMap``
  (``include/hgraph/types/type_resolution.h``) the generic ``wire<>`` path uses;
- a static-node candidate **builds** through the **same**
  ``NodeBuilder::implementation<Impl>(map)`` call and ``Wiring::add_node``
  interning path (*Wiring*, ``include/hgraph/types/static_node.h``);
- a graph candidate calls its ``compose`` function directly, so it flattens into
  the same graph wiring structure as an ordinary graph.

A single-implementation generic node (``const_``, ``replay``, a passthrough) is
**not** an operator and stays on the ordinary ``wire<>`` path; an operator is the
case where *more than one* implementation competes for one name. Conceptually a
non-overloaded node is just an operator with exactly one candidate and no ranking
step — the two share the same resolution and wiring primitives and differ only in
whether the ranking loop runs.

What the operator subsystem genuinely adds: a name → candidates **index**, a
**matcher/ranker** that can compare a candidate's (possibly generic) declared
types against the supplied *runtime* schemas, and a **selection** rule.


Why runtime dispatch (and why it is Python-ready)
-------------------------------------------------

``wire<Node>`` is a compile-time dispatcher: the implementation type is known at
the call site. An operator cannot be: the candidate set is **open** (registered
from anywhere, including a future Python module), the winner depends on the
**runtime-interned** schemas of the supplied ports, and an operator may be wired
with an **erased** ``Port<void>`` whose schema is only known at wiring time. So
operator matching/ranking is a **runtime** computation over interned metadata
pointers.

That single decision is what makes Python drop-in (Phase 4): because matching,
ranking and wiring are expressed over **runtime data** behind a type-erased
candidate, a Python-defined implementation registers as just another candidate —
the dispatcher cannot tell it from a C++ one. The C++ compile-time reflection
only *feeds* the runtime representation; it is not on the resolution path.


``TypePattern`` — the one matcher
---------------------------------

Matching needs to compare a candidate's declared type — which may contain type
**variables** (``TsVar`` / ``ScalarVar``) — against a concrete interned schema.
A ``TypePattern`` is the runtime form of such a (possibly generic) schema, and a
single set of runtime functions over it is the **one** matcher/ranker shared by
C++ and Python candidates:

.. code-block:: text

   TypePattern   = Var(name, constraints...)        # a TsVar / TIME_SERIES_TYPE variable
                 | Concrete(const TSValueTypeMetaData*)   # a fully-interned TS leaf
                 | TS(ScalarPattern)  | TSS(ScalarPattern)
                 | TSL(TypePattern, N-or-SizeVar) | TSD(ScalarPattern key, TypePattern value)
                 | TSW(ScalarPattern, period, min_period)
                 | TSB(fields...) | REF(TypePattern) | Signal
   ScalarPattern = ScalarVar(name, constraints...) | ScalarConcrete(const ValueTypeMetaData*)

Three runtime functions are the source of truth:

- ``bool match(pattern, const TSValueTypeMetaData* concrete, ResolutionMap&)`` —
  walks the pattern against a concrete schema. A ``Var`` / ``ScalarVar`` leaf
  **binds** into the map (reusing ``ResolutionMap::bind_ts`` / ``bind_scalar``,
  which already reject an inconsistent re-bind — so a shared variable such as
  ``add(TS<S>, TS<S>)`` is enforced for free). Constrained variables only bind
  schemas accepted by their constraint set. A ``Concrete`` leaf is **verified**
  (``time_series_schema_equivalent`` / pointer identity), which is exactly the
  check the compile-time ``ts_unifier`` deliberately omits (it trusts the
  static path). Composite kinds check ``concrete->kind`` then recurse, short-
  circuiting on failure.
- ``int rank(pattern)`` — an integer **specificity** score; **lower is more
  specific** and wins. See *Ranking*.
- ``const TSValueTypeMetaData* resolve(pattern, const ResolutionMap&)`` —
  substitutes bound variables to produce the concrete meta. Used to compute (and
  verify the resolvability of) a candidate's output schema, and by the Phase-4
  Python wiring path.

C++ candidates obtain their patterns by **lowering** their selector schema types
at compile time: ``to_pattern<S>()`` / ``to_scalar_pattern<T>()`` mirror the
shape of ``ts_resolver`` but emit ``Var`` nodes for ``TsVar`` / ``ScalarVar`` and
``Concrete(schema_descriptor<S>::ts_meta())`` for concrete leaves. Python
candidates build the same ``TypePattern`` directly from their type metadata. One
pattern representation, one matcher — no drift.

``TSL`` has one additional generic axis: a named size variable. C++ schemas write
this as ``TSL<TS<ScalarVar<"T">>, SIZE<"N">>``. Matching binds both ``T`` and
``N`` into ``ResolutionMap``; repeated uses of ``SIZE<"N">`` must see the same
concrete list size, and resolving a schema with that size variable substitutes
the bound runtime size.

Note the division of labour: the ``TypePattern`` interpreter is the *matcher*
across candidates. Building the chosen static-node C++ candidate still goes through
``NodeBuilder::implementation<Impl>(map)``, whose internal ``ts_resolver`` is the
*builder's* compile-time schema substitution for that one known ``Impl`` — it is
not a second matcher.

**REF transparency.** ``REF[X]`` is type-compatible with ``X`` (Python parity):
both the runtime matcher (``ts_pattern_match``) and the static unifier
(``ts_unifier``) look *through* a reference schema unless the pattern asks for a
``REF`` explicitly — a type variable always binds the **dereferenced** type. A
port whose producer computes a ``REF`` output (e.g. ``default``) keeps that
computed schema — the result schema is **never rewritten**; consumers bind
through the reference at runtime, and matching simply treats the two shapes as
the same type.

**SIGNAL input compatibility.** In input position, ``SIGNAL`` follows the same
rule as ordinary node / graph wiring: an ``In<..., SIGNAL>`` or ``Port<SIGNAL>``
overload accepts any time-series source and observes only its tick / modified
state. Output matching remains concrete — a ``SIGNAL`` output is not treated as
``TS<T>``.


``WiringArg`` — erased arguments
--------------------------------

The operator-dispatch entry point erases every ``wire<Operator>`` argument into a
``std::vector<WiringArg>`` — **the only compile-time step**; everything after is
runtime and type-erased (which is what lets Python candidates participate):

.. code-block:: text

   WiringArg = TimeSeries(WiringPortRef)            # carries const TSValueTypeMetaData* schema
             | ScalarValue(Value, const ValueTypeMetaData*)   # an owned, self-describing value

Scalars are erased to an owned ``Value`` immediately, so they are self-describing.
A scalar **variable** parameter binds to the argument's own schema. A scalar
**concrete** parameter accepts the argument when it can be converted to that
schema, and the chosen candidate writes the converted ``Value`` into the scalar
bundle. This mirrors the ordinary ``wire<>`` scalar path: standard numeric
arguments can be supplied as narrower explicit C++ types and are coerced into the
implementation's declared scalar type.

When a candidate expects a time-series input and the call supplies a scalar value,
the matcher tries the Python-style auto-const rule: the scalar value must match the
candidate's current-value schema, and
the selected candidate wires an internal one-shot const source to supply that input.
For example ``wire<add_>(w, price, Int{3})`` selects the same ``TS<Int>`` overload as
``wire<add_>(w, price, const_3)``.

Native atomic auto-const matches are exact: the scalar's interned schema must
equal the candidate's current-value schema. Template matching performs no
numeric widening or narrowing, so an ``int32`` does not match ``TS<Int>``, a
``Float`` does not match ``TS<float>``, and an int does not match
``TS<Float>``. This prevents a more-specific overload from changing the
argument's type while it is being matched; for example ``dedup(2)`` selects the
generic ``TS<Int>`` overload and cannot reach the float-tolerance overload.
Nominal opaque Python values retain ordinary subtype covariance: an instance of
a registered subclass may lift to ``TS[Base]`` and is reboxed at that declared
base schema after overload selection. Inheritance distance ranks an exact or
nearer overload ahead of a wider base. Unrelated Python classes remain
incompatible. Existing structural current-value compatibility, including
declared bundle inheritance, is also preserved. Scalar (configuration)
parameters are unaffected and keep the standard numeric conversions of the
ordinary ``wire<>`` scalar path.

**Nominal ancestry.** There is one ancestry walk, ``TypeRegistry::value_is_a``,
with ``value_inheritance_distance`` computed over the same descent, so
acceptance and ranking can never disagree: both first descend the covariant
containers the two schemas share (a ``Frame`` row, variadic-tuple elements)
and then follow the registered parent links of the named-Bundle or
opaque-Python hierarchy. Parent links are fixed when a schema is registered
(only the child list grows later), so the walk takes no registry lock and
allocates nothing; that is what lets dispatch selection, ``accepts_source``
and target-link checks use it per tick. The bundle-only ``bundle_is_a`` and
the std-operator copy ``dispatch_bundle_is_a`` were removed on 2026-09-05
(they were the second and third walker the retrospective counted, and the
distance function had drifted from the acceptance function on tuples).


Variadic ``**kwargs`` candidates and pack patterns (issue #224)
---------------------------------------------------------------

A candidate declaring ``**kwargs`` collects every named argument that matches
no fixed parameter. When the collector carries a ts annotation (canonically
``TSB[TS_SCHEMA]``), that pattern is retained on the candidate
(``OperatorImpl::kwargs_pattern``) and matched at dispatch against the
**synthesized un-named TSB of the supplied keywords**: field names are the
keyword names in call order, port arguments contribute their schema verbatim
(no REF dereference — upstream takes ``output_type`` as-is), and plain values
contribute ``TS[inferred]`` per the const-lift rule. This match is what binds
pack-level schema variables, letting a generic output (``-> TSB[TS_SCHEMA]``)
resolve. A zero-keyword call deliberately binds nothing: the collector is not
selected on an empty pack. The direct (non-operator) python call path applies
the same rule by matching the packed group against the declared annotation at
wiring (``_PyNode._check_binding``), which also covers ``*args`` packs.

``OperatorImpl`` — a type-erased candidate
------------------------------------------

A candidate is a runtime record — constructible from runtime data, **not only**
from a C++ template (this is the load-bearing Python seam):

.. code-block:: cpp

   struct OperatorImpl {
       std::string                name;
       std::string                label;          // signature text, for error messages
       std::vector<TypePattern>   params;          // one per In / Scalar position
       bool                       has_output;
       TypePattern                output;
       int                        rank;            // precomputed from params
       std::function<void(ResolutionMap&, OperatorCallContext)>             default_resolver;
       std::function<bool(const ResolutionMap&, OperatorCallContext)>       requires_predicate;
       std::function<OperatorWireResult(Wiring&, const ResolutionMap&,
                                        std::span<const WiringArg>)>        wire;
       enum class Source { Cpp, Python };
       Source                     source;
   };

- ``make_operator_impl<Impl>()`` lowers a static-node implementation's
  ``StaticNodeSignature<Impl>`` selectors into ``params`` via ``to_pattern``,
  precomputes ``rank``, and wraps a ``wire`` closure that assembles the scalar
  bundle, materialises any scalar-to-TS auto-const inputs, and delegates to
  ``NodeBuilder::implementation<Impl>(map)`` + ``Wiring::add_node``.
- ``make_operator_graph_impl<Impl>()`` lowers a sub-graph implementation's
  ``StaticGraphSignature<Impl>`` ``Port`` / ``Scalar`` compose parameters into the
  same ``params`` representation and wraps a ``wire`` closure that calls
  ``Impl::compose`` directly. Register these with ``register_graph_overload``.
- The registry's match step runs ``match`` for each parameter, then runs
  ``Impl::resolve_default_types(map)`` or
  ``Impl::resolve_default_types(map, OperatorCallContext)`` if present (the same
  hook the generic ``wire<>`` path uses, with an optional scalar-aware context),
  then checks the **output** pattern resolves. A still-
  unbound output variable is a **non-match** (reject with a reason), never an
  exception — an operator overload whose output is a free variable with no
  resolver can never produce a typed port and is ill-formed.
  ``requires_`` has the same map-only and context-aware forms.
- A **Python** candidate (Phase 4) fills the *same* struct: ``params`` built
  directly as ``TypePattern``s over shared interned metas, and a ``wire`` closure
  that can create a Python-backed runtime node. The registry and dispatcher are
  unchanged.


``OperatorRegistry`` and resolution
-----------------------------------

A process-wide singleton maps an operator name to its candidates:

.. code-block:: cpp

   class OperatorRegistry {
     public:
       static OperatorRegistry &instance();                    // plain static; single-threaded, no locks
       void register_overload(OperatorImpl);
       OperatorProviderHandle register_installer(key, fn);
       void activate_provider(const OperatorProviderHandle&);  // run exactly this pending installer
       bool remove_provider(const OperatorProviderHandle&);    // rejects live graph/plan leases
       // Pick the unique best candidate and the ResolutionMap it produced.
       std::pair<const OperatorImpl*, ResolutionMap>
            resolve(std::string_view name,
                    std::span<const WiringArg> args,
                    std::optional<bool> output_required = std::nullopt,
                    const TSValueTypeMetaData *expected_output = nullptr) const;
       void reset();                                           // test isolation
   };

``resolve`` follows the earlier
``OverloadedWiringNodeHelper.get_best_overload`` algorithm:

1. **Filter by arity** — drop candidates whose parameter count does not match.
2. **Try-match each** — match every parameter, apply any default resolver,
   requested output schema, and ``requires_`` predicate. Survivors carry their
   precomputed ``rank`` and the ``ResolutionMap`` they produced. Rejected
   candidates keep a human-readable reason.
3. **Rank** — sort survivors ascending by ``rank``.
4. **Select** — the unique lowest-rank survivor wins. **No survivor** → an error
   listing the rejected candidates and why. **A tie at the lowest rank** → an
   ambiguity error listing the tied candidates and their ranks.

Resolution is deterministic: rank is a precomputed total order, the candidate
vector preserves registration order, and no decision is ever made from
hash-map iteration order. Rich rejected-candidate messages are produced from
Phase 1 — operator misuse is the most common wiring error, and a bare "no
overload" message is hostile.

Installer callbacks also establish candidate provenance. Every overload
registered while a keyed installer runs belongs to that provider generation.
The opaque handle returned by ``register_installer`` can remove that exact
generation without affecting direct registrations, another provider, or a
later provider which reuses the same key. Removal erases both active candidates
and installer intent, so reset cannot replay a removed provider. A throwing
installer rolls back candidates contributed by its failed attempt before the
next retry.

``activate_provider`` runs only the installer identified by the supplied
generation-stable handle. Generated and dynamically loaded modules use it to
stage a provider without also activating unrelated pending installers, then
retain the handle for transactional removal or replacement. Targeted
activation may nest inside an aggregate installer: the registry restores the
enclosing provider context after the nested callback, so each candidate keeps
the provider which actually registered it. A non-running nested provider may
also be removed while its enclosing installer is active; removal of the
currently running provider remains an error.

Selecting a provider-owned overload retains one lease on the wiring result.
That lease is carried from ``Wiring`` into the reusable ``GraphBuilder`` and
then into every ``GraphValue`` made from it. ``remove_provider`` throws
``OperatorProviderInUseError`` while any such plan or graph is live; a failed
removal leaves the provider fully active. The registry performs logical
operator removal only. A native module manager must separately coordinate
other registries and may keep the image mapped after removal.

An indirect wiring helper must therefore resolve through ``wire_operator`` or
pass its active ``Wiring`` to ``OperatorRegistry::resolve``. This includes a
helper which retains only a selected ``LiftedKernel`` pointer in a node plan;
the pointer still names provider code. A schema-only probe may omit the wiring
only when no candidate callback or metadata pointer escapes the probe.


Ranking (specificity)
----------------------

Lower rank = more specific = preferred. The integer model reproduces the
**partial order** of the earlier implementation (``_generic_rank_util.py`` +
``_calc_rank``)
without its floating-point constants:

.. code-block:: text

   rank(Concrete scalar leaf) = 0
   rank(Concrete TS | Signal) = rank(value)            # 0 for a concrete leaf
   rank(TS(p))                = 1 + rank(p)
   rank(TSS(p))               = 1 + rank(p)
   rank(TSL(p, N))            = 1 + rank(p)
   rank(TSD(k, v))            = 1 + rank(k) + rank(v)
   rank(TSW(p, period, min))   = 1 + rank(p)
   rank(TSB(fields...))        = 1 + Σ rank(field)
   rank(REF(s))               = rank(s)
   rank(Var)                  = var_rank                # a variable costs its CURRENT var_rank

   candidate rank = structural rank + per-variable min(rank)

``var_rank`` is a budget that **decays as you descend**, which is what makes an
inner generic more specific than an outer one. It starts at:

.. code-block:: text

   10000   a top-level Input (TS) parameter
     100   the scalar payload of a TS / TSS / TSW / TSD key
       1   a standalone Scalar parameter

and then, at each step down, ``var_rank -> max(1, var_rank / 2)``. A variable
carrying **constraints** also pays the halved rate rather than the full one,
since a constraint is itself a narrowing.

Composite SCALARS rank too, by the same shape as time-series containers — this
is easy to miss, because the table above reads as though only the TS side has
structure:

.. code-block:: text

   rank(tuple[T, ...] | set | map | series | frame | array | bundle)
       = 1 + Σ rank(child)        # children at max(1, var_rank / 2)

   rank(bundle with a schema variable) = 1 + var_rank/2

The practical consequence, and the reason this is worth stating: **adding a
concrete or composite scalar parameter to an overload is essentially free.**
A ``Scalar<"policy", SomeEnum>`` contributes 0; a
``Scalar<"names", HomogeneousTuple<Str>>`` contributes 1 (its own structure,
plus 0 for the concrete element). Neither can shift dispatch away from a
sibling overload that differs on its time-series arguments, because those are
separated by the 10000-scale. Do not avoid a well-typed scalar argument out of
a fear of destabilising overload resolution — and equally, do not expect one
to *win* a contest it should win on TS grounds.

This yields ``TS<Int>`` < ``TS<ScalarVar>`` < a bare ``TsVar``, recursively
(``TSL<TS<Int>,N>`` < ``TSL<TS<ScalarVar>,N>`` < ``TSL<Var,N>``) — the
earlier rule that a top-level generic is always less specific than a generic
*inside* a collection. The abstract operator signature is **not** a candidate
(it has no ``eval``), so there is no need for the earlier implementation's
sentinel "never win" rank.

The candidate rank intentionally de-duplicates repeated generic variables by name
using their minimum contribution. That mirrors the earlier shared-variable
ranking rule: ``TS<T>, TS<T>`` ranks ahead of ``TS<A>, TS<B>`` for two equal
operands, because a repeated variable is a real alignment constraint rather than
two independent generic choices.


Defining and registering an operator
-------------------------------------

An operator is a marker struct carrying a name and an abstract (documentary)
signature; implementations are ordinary stateless node structs registered under
that operator:

.. code-block:: cpp

   // The operator: a name + general signature. Not executable (no eval). The three
   // variables L / R / O are *independent* (different names) — operands and result
   // may all differ.
   struct add_ : Operator<"add",
                          In<"lhs", TsVar<"L">>, In<"rhs", TsVar<"R">>, Out<TsVar<"O">>> {};

   // Homogeneous implementation: a single ``T`` repeated across both operands and the
   // result — the repeated name *aligns* them (both operands must be the same type).
   template <typename T> struct add_same {
       static void eval(In<"lhs", TS<T>> a, In<"rhs", TS<T>> b, Out<TS<T>> o) { o.set(a.value() + b.value()); }
   };
   // Heterogeneous implementation: operands and result differ.
   template <typename L, typename R, typename O> struct add_binary {
       static void eval(In<"lhs", TS<L>> a, In<"rhs", TS<R>> b, Out<TS<O>> o) { o.set(a.value() + b.value()); }
   };

   void register_standard_operators() {
       register_overload<add_, add_same<Int>>();                                    // int + int -> int
       register_overload<add_, add_binary<Int, Float, Float>>();                    // int + float -> float
       register_overload<add_, add_binary<DateTime, TimeDelta, DateTime>>();      // DateTime + TimeDelta
   }

**The operator signature is a suggestion, not a rule.** An implementation is **not**
required to structurally conform to the operator's abstract signature; that signature
only documents the operator and supplies its name and nominal arity for error
messages. Matching is by rank against the supplied arguments, using each
*implementation's* own signature. This is why one name (``add_``) covers homogeneous,
mixed, and heterogeneous cases.

**Aligned vs independent type variables.** A type variable that appears more than once
in one implementation's signature is a *constraint*: the matcher binds it on the first
occurrence and **rejects** any later occurrence that resolves to a different type
(``ResolutionMap::bind_ts`` / the ``Var`` consistency check in ``ts_pattern_match``).
So ``add_same<T>`` (the repeated ``T``) accepts ``(int, int)`` but rejects
``(int, float)``; ``add_binary<L, R, O>`` (distinct names) accepts both because the
operands are independent. The **result type can differ from the operands**, which is
the same machinery viewed from the output: ``div_: int / int -> float`` (aligned
operands, independent result) and ``sub_: DateTime - DateTime -> TimeDelta`` (result
differs from *both* operands) are ordinary overloads.

**C++ note.** Python expresses the homogeneous case as a *single* generic overload
gated by a ``requires`` predicate (e.g. ``add_scalars(TS[SCALAR], TS[SCALAR])`` with
``requires=lambda m: hasattr(m[SCALAR].py_type, "__add__")``), because Python's ``+``
dispatches at runtime. A C++ ``eval`` is concrete, so the homogeneous case is a
*template* instantiated per addable type (``add_same<Int>``, ``add_same<Float>``,
``add_same<TimeDelta>``, …) — the registration list plays the role of the
``requires`` capability gate.

**Registration is explicit, never static-init.** The test harness wipes every
registry between cases (see *Test isolation*), so a static-initialiser overload
would register once and vanish after the first test; and a candidate's patterns
reference interned schema pointers that exist only after the standard types are
seeded. Standard operators are therefore registered by an explicit
``register_standard_operators()`` — called by an application / the Python module at
startup, or by a test that needs them. Unlike ``register_standard_types`` (seeded
for *every* test, as it is foundational), the standard operators are **not**
auto-seeded by the reset listener: doing so would collide with a test's own ad-hoc
operator registered under the same name (e.g. ``"add"``). Tests register the
overloads they want in the test body (after reset, before wiring).


Calling an operator — the ``wire<>`` operator arm
-------------------------------------------------

``wire<X>`` dispatches on the shape of ``X`` (see *Graph Wiring*); operators add a
third arm:

.. code-block:: text

   wire<X>:  X has compose  ->  inline sub-graph (flatten)                  (existing)
             X has eval     ->  add node (concrete or generic resolution)   (existing)
             else (Operator) ->  erase args -> registry.resolve -> winner.wire

The operator arm erases the call's arguments to ``std::vector<WiringArg>``, calls
``OperatorRegistry::resolve(name, args, output_required, expected_output)``, then
invokes the winner's type-erased ``wire`` closure. A static-node overload lowers
through ``Wiring::add_node`` so it inherits ordinary interning and ranking; a graph
overload calls ``compose`` directly and contributes whatever nodes that graph wires.
Scalar arguments supplied for time-series inputs are materialised as internal const
sources before the selected implementation is wired.

The public return is shaped by the operator marker. An output operator returns an
erased ``Port<void>`` by default, or a typed ``Port<OutSchema>`` when the caller
supplies an explicit output schema such as ``wire<zero_, TS<Int>>(w)``. A sink
operator, whose marker has no ``Out<>`` selector, returns ``void`` and rejects an
explicit output schema.

The standard library also provides opt-in compose-time expression syntax in
``<hgraph/lib/std/std_operators.h>`` under ``hgraph::stdlib::syntax``. A compose body can
write:

.. code-block:: cpp

   using namespace hgraph::stdlib::syntax;

   auto a = wire<stdlib::replay_impl, TS<Int>>(w, "a");
   auto b = wire<stdlib::replay_impl, TS<Int>>(w, "b");
   auto c = (a + b * Int{2}).as<TS<Int>>();

The overloaded C++ operators are only sugar for ``wire<stdlib::add_>`` /
``wire<stdlib::mul_>`` / etc.; overload selection still goes through the same
``OperatorRegistry``. Expression results are erased ``Port<void>`` values because the
selected overload may change the result schema (for example ``int / int -> float``).
Use ``port.as<Schema>()`` when a typed C++ return is required; it validates the runtime
schema before producing ``Port<Schema>``. Binary expression overloads verify that two
port operands belong to the same ``Wiring`` instance before adding nodes.

**Testing.** An operator (≥1 time-series input, one output) is evaluated through the
``eval_node<Op>(...)`` harness: it wires the operator, lets dispatch resolve the
overload at wiring time, and returns the result **type-erased** as per-cycle ``Value``
deltas, checked with ``Value`` equality and the ``values<T>(...)`` helper. See
*User Guide > Testing Graphs in C++ > Evaluating operators*.


Test isolation
--------------

``OperatorRegistry`` is a process-wide singleton holding **borrowed** interned
schema pointers inside its candidates' patterns. The Catch2 reset listener
(``tests/cpp/registry_test_listener.cpp``) must:

1. call ``OperatorRegistry::instance().reset()`` **before**
   ``TypeRegistry::instance().reset()`` — clear the borrowers before the owner,
   the same ordering the other registries already follow (and the
   ``plan-registries-clear-on-reset`` rule).

It does **not** re-seed standard operators (that would collide with a test's own
ad-hoc operators); a test that needs the ``lib/std`` family calls
``register_standard_operators()`` itself, after the reset, before wiring.
Installer intent and provider identity survive reset, but a provider removed by
its handle does not. Tests which install a temporary provider remove it before
captured callbacks or native code can leave scope.


The Python implementation path (Phase 4)
----------------------------------------

Python overloads are drop-in because the candidate is type-erased and wired from
runtime data. A Python overload supplies the **same** ``OperatorImpl``:

- **patterns** — built directly as ``TypePattern``s over schemas interned in the
  shared ``TypeRegistry``;
- **rank** — computed by the same operator ranking helper as C++ candidates;
- **wire** — returns an ``OperatorWireResult``. A Python static-node candidate can
  produce a ``NodeBuilder`` whose ``NodeCallbacks::evaluate`` (a type-erased
  ``std::function``) acquires the GIL, marshals the ``NodeView`` inputs to Python,
  calls the retained callable, and writes the result back through the
  ``TSOutputView`` — behind ``HGRAPH_ENABLE_PYTHON_USER_NODES``.

**Cross-boundary type identity is the precondition.** Matching is pointer
equality of interned metadata, so a Python ``TS[int]`` and a C++ ``TS<Int>`` must
resolve to the **same** ``const TSValueTypeMetaData*``. That holds only if there
is exactly **one canonical scalar per logical type** and every name a Python type
uses is an **alias** onto it (not a separately-interned synthetic). The standard
types registration (and primitive aliases) must guarantee
``value_type("int") == register_scalar<int>("int")`` (same pointer), and the
standard-types seed must run before any overload is registered on either path.
See *Python Integration*.

The default build never depends on Python: Phases 1–3 include no nanobind header,
and the Python candidate path is compiled only under the opt-in flag.


Standard operator catalogue
---------------------------

The standard library operator **definitions** (the abstract ``Operator<>`` markers) live
under ``include/hgraph/lib/std/operators/``, grouped by family and pulled together by
``operators/operators.h``:

- ``arithmetic.h`` — ``add_`` / ``sub_`` / ``mul_`` / ``div_`` / ``floordiv_`` / ``mod_`` /
  ``divmod_`` / ``pow_`` / ``neg_`` / ``pos_`` / ``abs_`` / ``sign`` / ``ln`` (and the
  ``DivideByZero`` policy enum);
- ``comparison.h`` — ``eq_`` / ``ne_`` / ``lt_`` / ``le_`` / ``gt_`` / ``ge_`` / ``cmp_`` /
  ``min_`` / ``max_`` (and the ``CmpResult`` enum);
- ``logical.h`` — ``and_`` / ``or_`` / ``not_`` / ``invert_`` and the bitwise
  ``bit_and`` / ``bit_or`` / ``bit_xor`` / ``lshift_`` / ``rshift_``;
- ``container.h`` — ``getitem_`` / ``getattr_`` / ``setattr_`` / ``contains_`` / ``len_`` /
  ``index_of`` / ``is_empty``;
- ``collection.h`` — core aggregations (``sum_`` / ``mean``), set ops
  (``union_`` / ``intersection_`` / ``difference_`` / ``symmetric_difference_``) and ``TSD``
  re-shaping (``keys_`` / ``values_`` / ``rekey`` / ``flip`` / ``partition`` / … );
- ``conversion.h`` — ``const_`` / ``convert`` / ``combine`` / ``collect`` / ``emit`` /
  ``cast_`` / ``downcast_`` / ``downcast_ref`` / ``str_`` / ``type_`` / ``zero_`` /
  ``nothing`` / ``default_``;
- ``string.h`` — ``match_`` / ``replace`` / ``substr`` / ``split`` / ``join`` / ``format_``;
- ``stream.h`` — ``sample`` / ``lag`` / ``filter_`` / ``filter_by`` /
  ``until_true`` / ``freeze`` / ``throttle`` / ``take`` / ``drop`` / ``gate`` / ``window`` /
  ``to_window`` / …;
- ``extensions/analytics/include/hgraph/analytics/operators.h`` — the separately
  registered numerical transforms, statistical estimators, rolling means,
  resampling, and shaped-array analytics;
- ``control.h`` — ``merge`` / ``race`` / ``all_`` / ``any_`` / ``if_`` / ``if_then_else`` /
  ``if_cmp`` / ``route_by_index`` / ``if_true``;
- ``temporal.h`` — date components (``year`` / ``month_of_year`` / …) and time-series
  introspection (``valid`` / ``modified`` / ``last_modified_time`` / …);
- ``io.h`` — sinks and record/replay (``debug_print`` / ``null_sink`` / ``print_`` /
  ``log_`` / ``assert_`` / ``record`` / ``replay`` / ``compare``).

The marker's signature is documentary (a *suggestion*); each operator names independent
type variables where operands and result may differ, mirroring the Python ``hgraph``
catalogue under ``release/0.5:hgraph/_operators/`` (the canonical Python reference). Each
operator's **registry name string matches the Python operator name exactly** — including
its trailing underscore where Python has one (``Operator<"add_">`` / ``"eq_">`` /
``"sum_">``, but ``"sign">`` / ``"mean">`` / ``"zero">``) — so the name is the shared key
for the future Python bridge. Deferred from the catalogue: the JSON / table / data-frame
family (it needs scalar value types the value layer does not model yet).

Some operator overloads deliberately depend on an explicit output schema. For example,
the C++ fixed-``TSL`` ``split`` overload is authored as
``Out<TSL<TS<Str>, SIZE<"N">>>``. The ``N`` variable does not appear in any input, so
ordinary input unification cannot infer it. Supply the requested output shape at the
call site:

.. code-block:: c++

   struct SplitToPairGraph
   {
       static constexpr auto name = "split_to_pair_graph";

       static Port<TSL<TS<Str>, 2>> compose(Wiring &w, Port<TS<Str>> s)
       {
           return wire<stdlib::split, TSL<TS<Str>, 2>>(w, s, Str{","});
       }
   };

Calling ``wire<stdlib::split>(w, s, Str{","})`` is intentionally unresolved for the
fixed-``TSL`` overload: there is no input-side fact from which to derive ``N``.

**Generic target resolution** (``convert_target.h``). A caller may request a
*generic* output — a bare ``TSD``/``TSS``/``TSB``, an unparameterised
``TS[Tuple]``/``TS[Set]``/``TS[Mapping]`` — as a **type pattern**. The
pattern-matching layer completes it from the input port schemas before
overload selection, so the registry always sees a bound ``__out__``:

- ``resolve_convert_target(pattern, inputs[, keys])`` — the ``convert`` rules
  (single-input element/key/value derivation; the ``(keys, values)`` TSD zip
  pair).
- ``resolve_collect_target(pattern, inputs)`` — the accumulating shapes
  (element streams grow collections; ``(key, value)`` pairs grow a TSD).
- ``resolve_combine_target(pattern, inputs)`` — the compositional shapes:
  ``TSS`` = the common element of N scalar series; tuple-shaped ``TS`` (and
  concrete *homogeneous* tuple targets — hgraph emits the concrete row) = a
  fixed tuple of the N port elements; ``TSL`` = a fixed list of N same-typed
  ports; ``TSD`` = the ``(keys, values)`` zip pair (TS-of-tuple or ticking
  TSL forms). Other patterns fall through to the convert rules.

These resolvers are the ONLY place generic targets become concrete types —
the Python bridge routes every ``convert[TO]``/``collect[TO]``/``combine[TO]``
subscript through them (``_resolve_requested_target``) and never inspects
type names or labels; the wiring shape then follows from the RESOLVED
handle's properties (``_TsExpr.from_ts``).

**Implementations** are a parallel tree under ``include/hgraph/lib/std/operators/impl/``:
each definition file ``<family>.h`` has a matching ``impl/<family>_impl.h`` holding the
concrete overloads and a ``register_<family>_operators()`` function, and
``impl/operators_impl.h`` aggregates them into ``register_standard_operators()`` —
which since RFC 0025 checkpoint 3 records the whole list as the keyed
installer ``"hgraph.stdlib"`` on ``OperatorRegistry`` and then runs every
registered installer: a registry reset keeps installer intent, so one
rebuild call replays core and every extension alike, idempotently
between resets. Keyed installers now also own the provenance and graph-plan
leases needed for provider-scoped removal; process-lifetime callers may ignore
the returned handle, while unloadable module managers retain it. The
implemented subset currently covers scalar arithmetic (``impl/arithmetic_impl.h``),
scalar comparison (``impl/comparison_impl.h``), scalar logical / bitwise operators
(``impl/logical_impl.h``), string operators (``match_`` / ``replace`` /
``substr`` / ``split`` / ``join`` / ``format_`` in ``impl/string_impl.h`` plus string
container overloads in ``impl/container_impl.h``), ``const_`` / ``zero_`` /
``str_`` (``impl/conversion_impl.h``), date components and time-series
properties (``impl/temporal_impl.h``) and ``debug_print`` / ``null_sink``
(``impl/io_impl.h``), TSD ``keys_`` and TSS set algebra
(``impl/collection_impl.h``), stream basics and scalar analytics
(``impl/stream_impl.h``), plus the current
higher-order subset (``reduce``, ``switch_`` and ``map_`` in
``impl/higher_order_impl.h``). Further families gain their
``impl/<family>_impl.h`` (and a registration call) as they land. The
``<hgraph/lib/std/std_operators.h>`` umbrella pulls in both the definitions and the
implementations, plus opt-in expression sugar in ``operators/syntax.h``.

Registration translation units
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``register_<family>_operators()`` is defined in
``src/hgraph/lib/std/operators/<family>_impl.cpp``. Every
``register_overload<Op, Impl>()`` instantiates the wiring plan for one
overload, and those instantiations — not the header — set what the compiler
needs for the file: an impl header on its own costs about 0.5 GB of peak
compiler memory and each registered overload adds 5–8 MB on top (GCC 14,
``-O3``, thin LTO, precompiled headers; ``-O0`` is no cheaper, because the
cost is front-end instantiation). A family with a hundred and fifty
overloads in one file therefore needs about 1.7 GB, which is what made the
largest std files fail side by side on the 3-core / 7 GB hosted macOS
runners.

A registration translation unit is budgeted at **1 GB peak compiler memory**
(the measurement recipe is in :doc:`build_system`). The budget applies to
every registration file that ships — the std families and the extension
registrations alike. A family whose registration would exceed it is split
by operator group:

- ``<family>_impl.cpp`` keeps ``register_<family>_operators()``, which only
  calls the groups in sequence — plus any family-level registration that is
  not an overload, such as the runtime value conversions in
  ``conversion_impl.cpp``.
- ``<family>_impl_<group>.cpp`` defines
  ``register_<family>_<group>_overloads()``, declared beside
  ``register_<family>_operators()`` in ``impl/<family>_impl.h``, and holds
  the ``register_overload`` calls for one coherent group of operators.
- A new overload goes in the group that owns its operators; a new group is
  added when the owning file would leave the budget. Every group file is
  listed in ``src/CMakeLists.txt``.

Registration order does not affect resolution — ``resolve`` selects by rank
and a tie is an ambiguity error — so the split may interleave the overloads
of one operator differently from the single-file layout; each group keeps
its original relative order. The order is visible in two places: the
candidate list in diagnostics, and the generated API files
(``tools/api_inventory.py`` lists an operator's overloads in registry
order in ``docs/source/reference/operator_catalogue.rst``, the
``_operator_typing.pyi`` stub and ``_operator_docs.py``). Moving a
registration between files therefore means regenerating those files,
which ``python/tests/test_api_inventory.py`` checks.

The current groups:

- ``arithmetic``: ``numeric`` (numeric ``add_`` / ``sub_`` / ``mul_``,
  string concatenation and repetition, the unary numeric operators),
  ``division`` (``div_`` / ``floordiv_`` / ``mod_`` / ``divmod_`` /
  ``pow_``), ``temporal`` (duration, instant, date, period, civil and zoned
  arithmetic), ``container`` (list / set / map operators and the running
  sums and means) and ``aggregate`` (``min_`` / ``max_`` / ``sum_`` /
  ``mean`` over ``TS[list | set | map]``).
- ``collection``: ``mapping`` (TSD and map key / value operators and
  ``combine_tsd`` / ``combine_map``), ``sequence`` (TSL, tuple and TSB
  operators), ``aggregate`` (``min_`` / ``max_`` / ``sum_`` / ``mean`` over
  TSS, TSD and TSL, and the item-wise ``sum_`` / ``mean`` maps) and ``set``
  (TSS / TSD logic, equality and set algebra).
- ``comparison``: ``equality`` (``eq_`` / ``ne_`` / ``cmp_``), ``ordering``
  (``lt_`` / ``le_`` / ``gt_`` / ``ge_`` including the enum orderings) and
  ``extremum`` (``min_`` / ``max_``).
- ``conversion``: ``scalar`` (sources, scalar ``convert`` / ``str_``, the
  up- and downcasts), ``collection`` (``convert`` between collections, TSS,
  TSD, TSB and TSL) and ``combine`` (``combine`` / ``collect`` / ``emit``).
- ``stream``: ``flow`` (sampling, lag, scheduling, gating, take / drop /
  step / slice, ``dedup``, ``batch``) and ``window`` (``to_window``, the
  ``TSW`` aggregates and ``window``).
- ``temporal``: ``components`` (date / time attributes,
  ``evaluation_time_in_range`` and the time-series properties) and
  ``instants`` (zone resolution, range algebra and quantisation).
- ``hgraph-analytics`` ``statistics`` (``extensions/analytics/src/``,
  declared in its ``operator_registration.h``): ``container`` (``std_`` /
  ``var_`` over ``TS[map | set | list]`` and the running moments),
  ``collection`` (TSB, TSS, TSD and TSL statistics and the item-wise maps)
  and ``window`` (``TSW`` deviation, ``rolling_mean`` and ``resample``).


Higher-order operators and the ``WiredFn`` scalar
-------------------------------------------------

The higher-order constructs (``reduce``, ``switch_`` and the current ``map_``
subset) are **ordinary operators**, mirroring the ``release/0.5`` Python
direction where ``map_`` is an ``@operator`` whose old wiring-time
implementation became the default registered overload (``map_default``), and
user specialisations register alongside it — selected by the standard
best-match machinery, including ``requires`` predicates over the *value* of
the callable argument.

The C++ analogue of ``func: Callable`` is the **``WiredFn`` scalar**
(``include/hgraph/types/wired_fn.h``): ``fn<X>()`` erases an operator marker,
static node, or sub-graph ``X`` into a small registered scalar value — a stable
identity (its ``type_info``) plus a wiring thunk that dispatches ``wire<X>``
over erased ports. Because it is just a scalar:

- a ``Scalar<"func", WiredFn>`` parameter matches by type in patterns;
- an overload's ``requires_`` gates on the function's **identity**
  (``context.scalar_as<WiredFn>("func")`` and ``*func == fn<Only>()``) — the
  C++ mirror of Python's ``requires=lambda m, func: func == only_even``;
- equal functions hash/compare equal, so nodes configured with the same
  function intern/dedup like any other scalar configuration.

``ValueCallable`` is the separate **evaluation-time** callable scalar
(``include/hgraph/types/value_callable.h``). ``WiredFn`` creates graph
structure during wiring; ``ValueCallable`` consumes value views when a node
evaluates. Native ``value_fn<F>()`` values retain a lifted kernel operation
table, while a bridge backend owns its callable context and supplies the same
``invoke`` operation. The ``apply`` and ``call`` graph overloads pack
positional and keyword ports structurally, then delegate to native runtime
nodes. Positional validity gates invocation; invalid keyword ports are omitted.
The packed ABI carries the positional count explicitly, so user keyword names
cannot be confused with internal structural field labels.

Named arguments, defaults and ``**kwargs``
------------------------------------------

Operator calls follow the **Python calling rules**, applied as a call
normalisation step inside ``OperatorRegistry::resolve`` (runtime-first: a
future Python frontend gets identical behaviour by passing named
``WiringArg``\ s; the typed forms are sugar):

1. positional arguments fill parameters in declared order; overflow goes to
   the variadic tail (``VarIn``);
2. **named arguments** (``arg<"name">(value)`` at the call site — Python's
   ``name=value``) follow all positional ones and target parameters **by
   name**: node-overload inputs are named by their ``In<"name", …>``
   declarations, scalars by ``Scalar<"name", …>``, and graph-overload ports
   by ``NamedPort<"name", S>`` (a drop-in ``Port<S>``; higher-order
   overloads use these names for arguments such as ``arg<"key">(k)`` and for
   mapping ``**kwargs`` onto a supplied ``WiredFn``).
   Duplicates ("got multiple values for argument"), unknown names
   ("unexpected keyword argument") and positional-after-named are rejected
   with Python-style messages;
3. omitted parameters take their **declared defaults** — the impl's
   ``static std::vector<std::pair<std::string_view, Value>> defaults()``
   hook, validated at registration. A default on a **time-series** parameter
   follows the Python conversions: a value becomes ``const(value)`` (the
   standard scalar-to-ts promotion at the resolved schema) and an empty
   ``Value`` is ``None`` — a null source, leaving the input unwired (pair it
   with ``InputValidity::Unchecked`` to observe it invalid). Each default a
   candidate falls back on costs one rank point, so an overload whose
   parameters were all supplied wins at equal specificity;
4. named arguments matching no parameter collect into the candidate's
   ``**kwargs`` — the trailing ``VarKwIn<"kwargs">`` selector (marker +
   last ``compose`` parameter, after ``VarIn`` if both), received as
   ``(name, port)`` pairs in call order. A **plain value** here follows
   the same rule as time-series defaults: it lifts to a ``const`` source
   (materialised for the WINNING candidate only — losing candidates never
   touch the graph). A kwargs collector costs one rank point, like a
   variadic tail.

The selected candidate's ``wire`` receives the **normalised** call:
arguments in declared parameter order with defaults materialised and the
variadic tail appended (``ResolvedOperatorCall``) — so node-overload scalar
assembly and input collection are oblivious to how the call was spelled.
Labels render defaults as ``=…`` and the collector as ``**kwargs``.


Variadic operator parameters
----------------------------

An operator overload may take **zero-or-more trailing time-series arguments**
(Python's ``*ts``) via the named ``VarIn<"name", Pattern>`` selector
(``operator_dispatch.h``), which serves both roles like ``Scalar``: in the
``Operator<…>`` **marker** it declares the variadic contract
(``VarIn<"ts", TsVar<"TS">>``), and as the last ``compose`` parameter of a
graph overload the implementation receives the tail as erased
``WiringPortRef``\ s. ``switch_(key, cases, *ts, **kwargs)`` and
``map_(func, *args, **kwargs)`` use it; ``map_`` has no fixed anchor
parameter, so its inputs are resolved onto the mapped function's parameter
order before the TSD/TSL kernel is selected.

Dispatch semantics (all in the **runtime** matcher — the capability is fully
available to a future Python frontend, which calls the same
``OperatorRegistry::resolve`` over erased args; the typed ``wire<Op>`` form is
sugar):

- a variadic candidate matches when ``args >= fixed-params``; each tail
  argument must be a time-series and is matched against the declared pattern
  **independently** (a throwaway binding scope per argument: bindings made by
  the fixed prefix constrain the match, but heterogeneous tail arguments
  never bind type variables — ``switch_`` branches see differently-typed
  ``ts`` args);
- the variadic tail contributes rank once per supplied tail argument, plus a
  small fixed penalty, so fixed-arity candidates are **preferred** over
  variadic ones at equal specificity; a user specialisation with an exact
  signature wins against the variadic default;
- candidate labels render the variadic parameter with a ``*`` prefix.

Static-node signatures remain fixed-arity today. Variadic operator overloads
should therefore be authored as graph overloads that receive ``VarIn``. A
non-empty ``VarIn`` tail can be passed as a normal operator argument where a
collection input is expected; the dispatcher erases it as a fixed structural
``TSL`` with one child per tail element. This is the variadic-tail counterpart of
scalar auto-const promotion, and lets graph overloads write calls such as
``wire<reduce_>(w, fn<binary>(), args, zero)`` directly. ``reduce`` also detects
that packed-tail marker: ``reduce(func, args)`` has no zero, while
``reduce(func, args, zero)`` supplies the optional associative zero. In both
cases only valid/live values participate; see *Nested Graphs > reduce* for the
empty/singleton/multi-value contract. Operators such as ``merge`` whose
combiner must observe positional invalidity use the shared static binary-layout
helper directly rather than ``reduce``. If a packed ``VarIn`` could
match both a true variadic overload and a fixed ``TSL`` overload, the dispatcher
expands it back into tail arguments for the variadic candidate and penalizes the
fixed-``TSL`` conversion, so the variadic overload wins. Pure variadic overloads
whose output type is the tail type must still provide ``resolve_default_types``
from the call context because variadic tail matches intentionally do not bind
type variables.

Graph compose bodies should return the port produced by their inner wiring call.
Do not wrap an existing port in a different ``Port<...>`` type to make the graph's
signature look more specific; output type resolution belongs in the overload
metadata, not in a graph-body port cast.

Scalars cannot appear in the tail — configuration belongs in the fixed
prefix (cf. ``SwitchCases``) or in **keyword-only parameters**: ``Scalar``
params declared AFTER the ``VarIn`` in ``compose`` are Python's
keyword-only-after-``*args`` — they fill by name (``arg<"name">(…)``) or by
default only, never positionally (positional overflow always goes to the
tail). ``map_``'s ``__key_arg__`` is the canonical example.


The markers live in ``operators/higher_order.h`` and the default overloads in
``impl/higher_order_impl.h`` — **their own family files in lib/std**; there is
nothing special about them now that sub-graph compilation is standardised
(``compile_subgraph`` / ``nested_``, see *Nested Graphs*). The ``reduce``
default overload is an ordinary **graph overload**
(``register_graph_overload``) whose ``compose`` takes a **generic port
parameter** — ``Port<TSL<TsVar<"V">>>`` — and lays the reduction out
statically; the dynamic ``TSD`` kernel arrives later as another overload of
the same name, selected by pattern rank.

Two framework rules make generic graph-overload ports work:

- a ``Port`` whose static schema is **non-concrete** (contains a type
  variable) keeps the ref's runtime-resolved schema instead of stamping an
  interned one (``schema_descriptor`` returns ``nullptr`` for var schemas);
- a graph-overload port argument is accepted by **pattern semantics**, not
  strict schema equivalence (``graph_port_accepts``): a declared size-0 TSL is
  a size wildcard, matching the ``TypePattern`` rule — the port never becomes
  an input endpoint of the declared schema, it is handed to ``compose``
  carrying the argument's own schema. Node overloads keep the strict check,
  since they build a real input endpoint of the declared schema.


Roadmap
-------

1. **Phase 1 — core dispatch (C++) — done.** The full ``TypePattern`` AST +
   ``match`` / ``rank`` / ``resolve`` / ``to_pattern``; ``Operator<>`` /
   ``WiringArg`` / ``OperatorImpl`` / ``OperatorRegistry`` / ``register_overload``;
   the ``wire<>`` operator arm; the reset-listener hook. Proven by ``add_``
   (``TS<Int>`` specific beats the generic; the generic wins for ``TS<Str>``) plus
   no-match and ambiguity errors.
2. **Phase 2 — scalars, predicates, and shape support — done.** ``WiringArg``
   scalar matching, scalar-to-time-series auto-const promotion, map-only and
   context-aware ``requires_`` / ``resolve_default_types`` hooks, explicit output
   schema filtering, sink return shaping, graph overload registration, constrained
   variables, repeated-variable ranking, and ``TSW`` / ``TSB`` pattern support.
3. **Phase 3 — ``lib/std`` operator family — done.** ``include/hgraph/lib/std/std_operators.h``
   registers scalar arithmetic, comparison, logical / bitwise, const / zero and simple IO
   sink overloads through an explicit ``register_standard_operators()`` call. Arithmetic
   includes homogeneous and mixed numeric cases, heterogeneous-temporal cases
   (``DateTime + TimeDelta -> DateTime``, ``Date + TimeDelta -> Date``), result-differs
   cases (``div_: Int / Int -> Float``, ``sub_: DateTime - DateTime -> TimeDelta``),
   Python-style floor semantics for integer ``floordiv_`` / ``mod_`` / ``divmod_``,
   binary scalar ``min_`` / ``max_``, string ``contains_`` / ``len_`` / ``is_empty`` /
   ``getitem_`` plus ``match_`` / regex ``replace`` / ``substr`` / ``split`` /
   ``join`` / ``format_``, TSD ``keys_`` and TSS ``union_`` / ``intersection_`` /
   ``difference_`` / ``symmetric_difference_``, stream ``sample`` / ``filter_`` /
   ``take`` / ``drop`` / ``step`` / ``slice_`` / scalar and structural
   ``dedup``, ``str_``, date components /
   ``explode(Date)``, time-series properties (``valid`` / ``modified`` /
   ``last_modified_*``), and optional
   wiring-time ``DivideByZero`` policy overloads (``Error`` / ``Nan`` / ``Inf`` /
   ``NoTick`` / ``Zero`` / ``One``, mirroring the earlier implementation where
   applicable).
   ``DivideByZero`` is a registered *enum scalar*. These optional scalar policies predate
   framework parameter defaults (see *Named arguments, defaults and kwargs* above) and
   remain modelled as two overloads selected by
   **arity** — e.g. ``div_(a, b)`` defaults to ``Error`` and ``div_(a, b, policy)`` uses
   the supplied policy. ``zero_`` mirrors Python's ``zero(tp, op)``: the value depends on
   the output type **and** the operation (a ``WiredFn`` argument — ``add_``/``sum_`` -> 0,
   ``mul_`` -> 1, ``min_``/``max_`` -> the saturating bound) for ``int`` / ``float`` /
   ``str``; an unmapped operation is a wiring-time error (Python's ``KeyError``).
   ``default_`` is the REF-forwarding substitute-until-valid implementation mirroring
   Python's ``_default`` (``valid=()``; keeps ``ts`` active while invalid, forwards the
   reference and goes passive once valid). The **per-overload catalogue is
   maintained once**, in *User Guide > Wiring Graphs in C++ > Supported standard
   operator overloads* — this page records the operator-framework design and
   deliberately does not duplicate that inventory.
4. **Phase 4 — Python implementation path.** Runtime-data ``OperatorImpl`` from a
   Python signature; ``NodeCallbacks`` hosting a Python callable; cross-boundary
   identity asserted. Behind ``HGRAPH_ENABLE_PYTHON_USER_NODES``.
