RFC 0033: Type Carriers Resolved by the Registry
================================================

:Status: Proposed
:Author: Howard Henson
:Created: 2026-09-05
:Target: ``include/hgraph/types/type_resolution.h``,
         ``include/hgraph/types/operator_dispatch.h``,
         ``src/hgraph/types/operator_dispatch.cpp``,
         ``src/hgraph/types/type_pattern.cpp``,
         ``python/py_carriers.h``, ``python/py_wiring.cpp``,
         ``python/py_type_system.cpp``, ``python/module.cpp``,
         ``python/hgraph/_wiring/{_operator,_node,_graph,_services,_resolution,_core}.py``,
         ``python/hgraph/_types.py``, ``python/hgraph/reflection.py``
:Related: RFC 0003 (Python scalar registration), RFC 0004 (python-owned
          structured scalars), developer guide ``operators.rst``
          ("Named arguments, defaults and ``**kwargs``"), ``python_bridge.rst``
          ("The erased operator contract"), ``testing.rst``
          ("Authoring-shape sweeps", "Architecture ratchets"),
          ``python/tests/test_type_carrier_sweep.py`` (the behaviour pins)

Summary
-------

A *type carrier* is a ``type[...]`` parameter: a wiring-time argument whose
value is a type rather than a value of that type. It is spelled many ways in
the Python DSL -- ``fn[X]``, ``fn[VAR: X]``, ``fn(..., to=X)``,
``= DEFAULT[X]``, ``= X``, ``= AUTO_RESOLVE``, a scalar argument carrying a
``TS[...]`` expression, a collection type, a ``Size[n]`` -- and every
spelling means the same thing: *bind (or check) the type variables that the
parameter's annotation mentions*.

Today the C++ resolver owns the matching of every time-series argument but
none of the carriers. A ``type[...]`` parameter is lowered to a throwaway
scalar variable that never binds anything, and each decorator kind
re-implements what the carrier means in Python: seven subscript rules, three
type-variable collectors, a two-pass materialisation that re-runs user
resolvers, an operator-name table for the positional carriers of ``const``,
``nothing`` and ``replay``, and two shadow dictionaries that map schemas back
to Python annotations because the registry cannot. The 2026-09-04
retrospective traced a run of wiring defects to exactly this fragmentation.

This RFC moves the carrier into the registry's dispatch model:

* a **core carrier value**, ``TypeCarrier``, that holds one resolution binding
  (a TS schema, a scalar schema, or a size) as a scalar ``Value``;
* a third **parameter kind**, ``ParamPattern::Kind::TypeArg``, whose pattern
  is matched against the *carried* type inside the candidate loop, before
  user resolvers run, and whose omitted default may be a *pattern resolved
  after* the resolvers;
* one normative **dispatch order** that fixes when a carrier binds, when a
  deferred carrier materialises, and what ``resolvers=`` and ``requires=``
  can see;
* a ``TypeArg<"name", Pattern, Default>`` descriptor so C++ operators declare
  carriers in their signatures (``const``, ``nothing``, ``replay`` lose their
  name-keyed table);
* one bridge-side **reverse binding** registry so ``python_type_for_value``
  rebuilds every Python annotation the DSL produced, which retires the shadow
  dictionaries; and
* one Python subscript rule and one type-variable collector, with the
  Python-side binding algebra deleted.

Behaviour is pinned cell by cell in ``python/tests/test_type_carrier_sweep.py``
(77 cells: PR #662, open in the hardening stack that the implementation PRs
build on; this RFC's own branch is documentation only and does not carry
the sweep). The cells this RFC deliberately changes are listed under
*Compatibility*.

Motivation
----------

The inventory below is what the retrospective found on ``main`` on
2026-09-04. Line numbers drift; the ratchets in
``python/tests/test_architecture_ratchets.py`` count the sites and only
accept a fall.

**Which argument is a carrier (spelling to placeholder)** -- seven
``__getitem__`` rules with three different bare-item semantics:

.. list-table::
   :header-rows: 1
   :widths: 28 52 20

   * - Site
     - Rule
     - Kind
   * - ``_OperatorFunction.__getitem__`` (``_core.py``)
     - ``op[OUT: X]``, ``op[SIZE: Size[n]]``, ``op[VAR: X]``; bare ``op[X]``
       is an output or input constraint per
       ``operator_output_is_selective``; special branches for
       ``with_columns`` and ``getattr_``
     - registry operator
   * - ``_Operator.__getitem__`` (``_operator.py``)
     - bare item binds the sole variable, else the ``DEFAULT[...]`` one;
       otherwise delegates to the registry rule above (so ``two[str]`` on a
       two-variable operator is *accepted*)
     - ``@operator``
   * - ``_PyNode.__getitem__`` (``_node.py``)
     - ``node[VAR: X]`` pins; bare items fill the remaining variables in
       first-appearance order; a single bare item needs a sole remaining
       variable or a ``DEFAULT`` (else ``WiringError``)
     - node
   * - ``_GraphFn.__getitem__`` (``_graph.py``)
     - slices pin; bare items fill ``DEFAULT`` carriers first, then
       ``AUTO_RESOLVE`` carriers in declaration order; rewrites the wrapper
       signature
     - graph
   * - ``_ServiceStub`` / ``_AdaptorStub`` / ``_ServiceAdaptorStub``
       ``__getitem__`` (``_services.py``)
     - bare item binds the single unresolved public variable; slices go
       through ``_specialization`` (``TimeSeriesSchema`` subclass becomes
       ``TSB[...]``)
     - services

**Binding a variable from a carrier, and materialising the carrier** -- the
Python "binding algebra" in ``_resolution.py`` (``_resolution_binding``,
``_python_value_for_binding``, ``_binding_for_type_value``,
``_bind_native_resolution``, ``_match_type_argument``) mirrors
``ResolutionMap`` and is driven from four places: ``apply_type_carriers``
(``@operator`` overloads; two passes, ``required=False`` then resolvers then
``required=True``, with an ``OUT`` special case that resolves the output
pattern), the wire-trampoline value pass, ``_graph_auto_resolve``, and the
``_PyNode.__call__`` resolution phase with its three projection helpers.
Services add ``_apply_service_defaults`` (``tp: type[X] = TS[K]`` seeded
after request matching). The C++ matcher sees a carrier only as
``scalar_pattern_var("__type_arg__<id>__<name>")``
(``_register_overload``), which binds a variable nobody reads.

**Type-variable collectors** -- ``_type_variables_of`` (``_types.py``),
``_service_type_variables`` (``_services.py``, a subset), and
``_annotation_type_vars`` + ``_PyNode._ordered_type_vars`` (``_node.py``,
read off the lowered C++ pattern with ``type[VAR]`` special-cased).

**Operator-name branches** -- ``_core.py`` decides by ``self.__name__``
that ``const``/``nothing``/``replay`` take a positional ``_TsExpr`` at index
``{"const": 1, "nothing": 0, "replay": 1}``, that ``apply`` and ``const``
derive ``output_type``, and that ``with_columns``/``getattr_`` subscripts
mean an output type. ``python_bridge.rst`` already records the standing
ruling that dispatch never keys on an operator name; these branches predate
it.

**Shadow dictionaries** -- ``_TS_SCALAR_TYPES`` and ``_VALUE_SCALAR_TYPES``
in ``_types.py`` (six writes, two reads, two more reads in
``reflection.py``) exist because ``python_type_for_value``
(``py_type_system.cpp``) rebuilds atomics, registered scalars, nominal
bundles and enums but **not parameterised generics** (``tuple[int, ...]``,
``Mapping[str, int]``, ``frozenset[int]``). The dictionaries are keyed by
native handles and are not cleared by ``reset_registries()``: a recycled
address can alias a new schema to an old annotation.

What this costs: the same rule written five times drifts (the sweep found
graphs preferring ``DEFAULT`` before ``AUTO_RESOLVE`` where nodes use
sole-or-``DEFAULT``; a graph ``type[TSD[K, TS[int]]]`` argument binds ``K``
without checking ``TS[int]``; a ``Size`` pinned by subscript reaches the body
as a plain ``int`` while the auto-resolved form is a ``Size`` object); user
resolvers run twice; C++ callers cannot pass a type argument at all; and
every new decorator kind needs its own copy.

Terms
-----

carrier
   A wiring-time argument whose value is a type: a TS schema, a scalar
   schema, or a fixed size. Exactly the three ``ResolutionKind`` forms.

carried pattern
   The pattern inside the ``type[...]`` annotation, e.g. ``TS[SCALAR]`` in
   ``to: type[TS[SCALAR]]``. Matching a carrier means matching the carried
   type against the carried pattern in the candidate's ``ResolutionMap``.

deferred carrier
   A carrier the call omitted and whose default is a *pattern*
   (``AUTO_RESOLVE``, ``DEFAULT[X]``, ``= X`` where ``X`` is a type variable,
   ``= TS[K]``). It binds nothing at match time and is materialised from the
   map after resolvers.

materialise
   Produce the carrier's value from the resolution map, as a ``TypeCarrier``
   for C++ and as the corresponding Python object (``TS[int]``, ``int``,
   ``tuple[int, ...]``, ``Size[3]``) at the bridge.

Ownership boundary
------------------

* **Core** (``hgraph_core``) owns the carrier value type, the ``TypeArg``
  parameter kind, its matching, ranking, deferred-default materialisation,
  the dispatch order, and the C++ declaration descriptor. It knows nothing
  about Python objects.
* **Bridge** (``python/*.cpp``) owns the conversion of Python objects to and
  from ``TypeCarrier`` (arrival and materialisation), the
  ``ResolutionScope.match_carrier`` / ``materialise`` entry points used by
  the Python paths that do not go through operator dispatch, and the reverse
  binding registry (schema to Python annotation). It follows the existing
  precedent of ``bundle_class_info_registry()`` / ``enum_class_registry()``:
  bridge-side maps keyed by interned metadata, cleared on
  ``reset_registries()``.
* **Python** (``python/hgraph``) owns the DSL spellings only: stripping
  ``DEFAULT[...]``, the ``AUTO_RESOLVE`` sentinel object, the subscript
  syntax, and user ``resolvers=`` callables. It does not match, bind, or
  project types.

The direction is the one in ``AGENTS.md``: the C++ runtime is the source of
truth and Python is a wiring bridge. Nothing in this RFC adds a per-tick
path; every change is wiring-time.

C++ contract
------------

``TypeCarrier`` -- one resolution binding as a value
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Declared in ``include/hgraph/types/type_resolution.h`` next to
``ResolutionMap``, because it is the value form of one map entry. A carrier
is exactly one of the three ``ResolutionKind`` forms at a time, so it is a
closed sum type -- the case ``AGENTS.md`` reserves ``std::variant`` for --
and costs one pointer-or-size plus the alternative index (16 bytes), not
three mostly-empty slots:

.. code-block:: cpp

   struct TypeCarrier
   {
       using Binding = std::variant<const TSValueTypeMetaData *,   // ResolutionKind::TimeSeries
                                    const ValueTypeMetaData *,     // ResolutionKind::Scalar
                                    std::size_t>;                  // ResolutionKind::Size
       Binding binding;   ///< never empty; the alternative *is* the form

       [[nodiscard]] ResolutionKind kind() const noexcept;                 // from binding.index()
       [[nodiscard]] const TSValueTypeMetaData *ts() const noexcept;       // nullptr unless TimeSeries
       [[nodiscard]] const ValueTypeMetaData   *scalar() const noexcept;   // nullptr unless Scalar
       [[nodiscard]] std::optional<std::size_t> size() const noexcept;     // empty unless Size
       friend bool operator==(const TypeCarrier &, const TypeCarrier &) noexcept = default;
   };
   HGRAPH_DECLARE_STANDARD_SCALAR_BINDING(TypeCarrier);   // as WiredFn

It is registered as a standard scalar exactly like ``WiredFn`` (the other
wiring-time artefact that travels as a scalar ``Value``; see ``operators.rst``
"Higher-order operators and the ``WiredFn`` scalar"). A carrier therefore
arrives at dispatch as an ordinary
``WiringArg{kind = Scalar, scalar_value = Value{TypeCarrier{...}},
scalar_meta = <TypeCarrier schema>}`` from C++ and Python alike.

The bridge's ``PyTsMetaRef`` (``python/py_carriers.h``) is the TS form of
this struct under a bridge-private name; it is removed and both of its uses
(a ``type[...]`` argument, and the resolved TS schema handed to a generic
Python node for its recordable-state output) become ``TypeCarrier``. One
carrier struct, in core.

``ParamPattern::Kind::TypeArg`` -- a parameter role, not a value shape
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``ParamPattern`` (``operator_dispatch.h``) gains a third kind and the fields
a carrier needs:

.. code-block:: cpp

   struct ParamPattern
   {
       enum class Kind { Input, Scalar, TypeArg };

       Kind          kind{Kind::Input};
       std::string   name{};
       TypePattern   ts{};       ///< Input; TypeArg carried TS pattern, or the size
                                 ///< pattern (``size_name`` / ``fixed_size`` / ``size_var``)
       ScalarPattern scalar{};   ///< Scalar; TypeArg carried scalar pattern
       std::optional<Value> default_value{};   ///< a concrete default (for TypeArg: a TypeCarrier)

       // TypeArg only
       ResolutionKind carrier{ResolutionKind::TimeSeries};  ///< which form the carrier takes
       /** A deferred default: a pattern resolved in the candidate's map after
           resolvers ran (AUTO_RESOLVE / DEFAULT[X] / = X / = TS[K]). */
       struct DeferredCarrier { TypePattern ts{}; ScalarPattern scalar{}; };
       std::optional<DeferredCarrier> default_pattern{};
   };

Why a parameter kind rather than a ``ScalarPattern::Kind::TypeCarrier`` (the
form the retrospective's blueprint sketched): a carrier is matched by its
*value*, whereas ``scalar_pattern_match`` matches a *schema*; the carried
pattern is a ``TypePattern`` for TS carriers, and ``ScalarPattern`` cannot
hold a ``TypePattern`` without a circular definition or an indirection; and
ranking, defaults and label rendering are already per-parameter. See
*Alternatives*.

Matching
~~~~~~~~

A new core function, declared in ``type_pattern.h`` beside the existing
matchers:

.. code-block:: cpp

   /** Match the carried type against a TypeArg parameter's carried pattern,
       binding variables in ``map``. Fails when the carrier's form differs
       from the parameter's (``carrier``), or when the carried type does not
       match the pattern. */
   [[nodiscard]] HGRAPH_EXPORT bool type_carrier_match(const ParamPattern &param,
                                                       const TypeCarrier &carrier,
                                                       ResolutionMap &map);

It dispatches on ``param.carrier``:

* ``TimeSeries`` -- ``input_ts_pattern_match(param.ts, carrier.ts(), map)``;
* ``Scalar`` -- ``scalar_pattern_match(param.scalar, carrier.scalar(), map)``;
* ``Size`` -- ``size_pattern_match(param.ts, *carrier.size(), map)``.

A form mismatch (a TS carried where the parameter expects a scalar, or the
reverse) is a candidate failure with the message *"parameter 'to' expects a
time-series type, got scalar type 'int'"*. This is precisely the "outer kind
must agree" rule of today's Python ``_match_type_argument``, once.

In the candidate loop of ``OperatorRegistry::resolve``
(``operator_dispatch.cpp``), the ``TypeArg`` branch requires the supplied
argument to be a scalar whose schema is the ``TypeCarrier`` schema and calls
``type_carrier_match``. Any other argument fails the candidate as a type
mismatch, exactly as a wrong scalar does today.

A ``Scalar`` parameter never receives a ``TypeCarrier``. Arrival mints a
carrier only for an argument that targets a ``TypeArg`` parameter (see
*Bridge contract*), so a class passed as data -- ``const(OpaqueBase)``,
``def f(cls: object)`` -- stays the opaque scalar value it is today and an
unconstrained scalar variable can never bind to the carrier schema.

Ranking
~~~~~~~

A ``TypeArg`` parameter contributes the rank of its carried pattern
(``ts_pattern_rank`` for TS and size carriers; the scalar pattern's rank for
scalar carriers), so ``type[TS[int]]`` is more specific than
``type[TS[SCALAR]]``. An omitted carrier that fell back on a default costs one
rank point like every other default (``operators.rst`` "Named arguments,
defaults and ``**kwargs``", rule 3).

Deferred defaults
~~~~~~~~~~~~~~~~~

A carrier the call omitted takes, in this order:

1. ``default_value`` when present -- a concrete carrier
   (``schema: type[SCALAR] = SweepRow``). It is synthesised into the call
   *before* matching, like any other default, and therefore **binds** the
   variables of the carried pattern (``SCALAR`` becomes ``SweepRow`` above).
2. ``default_pattern`` when present -- a deferred carrier. Nothing binds at
   match time. After the resolvers ran, the dispatcher resolves the default
   pattern in the map (``AUTO_RESOLVE`` lowers to the carried pattern itself;
   ``= OUT`` to the variable ``OUT``; ``= TS[K]`` to that pattern), builds
   the ``TypeCarrier``, and then matches it against the carried pattern with
   ``type_carrier_match`` so that variables the default mentions but the
   inputs did not bind (``SCALAR`` in ``to: type[TS[SCALAR]] = OUT``) are
   bound from it.
3. Neither -- the candidate fails with *"missing required argument 'to'"*.

Deferred carriers are materialised to a fixed point interleaved with output
resolution (see the order below), so a carrier defaulting to ``OUT`` sees the
resolved output and a carrier that supplies a variable the output needs is
materialised first. A deferred carrier still unresolved when a pass makes no
progress fails the candidate: *"type variable 'K' for parameter 'key_type'
could not be resolved"*. This replaces the Python two-pass in
``apply_type_carriers`` and the ``OUT`` special case.

Dispatch order (normative)
~~~~~~~~~~~~~~~~~~~~~~~~~~

For every candidate of ``OperatorRegistry::resolve``:

1. Call normalisation: positional and named arguments fill declared
   parameters; omitted parameters with a ``default_value`` are synthesised.
   A concrete carrier default is a supplied argument from here on.
2. Parameter matching in declared order: ``Input`` through
   ``input_ts_pattern_match``; ``Scalar`` through ``scalar_pattern_match``;
   a supplied ``TypeArg`` through ``type_carrier_match``; an omitted
   ``TypeArg`` with a ``default_pattern`` is recorded as deferred; an omitted
   ``TypeArg`` with neither default fails the candidate.
3. The ``**kwargs`` pack pattern, as today.
4. ``default_resolver`` (the user's ``resolvers=``). It sees every binding
   from step 2, including the ones supplied carriers made.
5. Output resolution and deferred-carrier materialisation, iterated together
   until neither makes progress.
6. ``requires_predicate`` (the user's ``requires=``). It sees the complete
   map, and at the bridge the materialised carrier values.
7. The winning candidate's ``wire`` receives the normalised call
   (``ResolvedOperatorCall``) with every carrier present as a ``TypeCarrier``
   scalar, supplied or materialised.

Steps 4 and 6 are where today's behaviour is pinned by the sweep's group C
(``test_resolvers_run_after_the_carrier_is_bound``,
``test_requires_sees_the_materialised_carrier_value``,
``test_requires_after_resolver_sees_the_resolved_carrier``); the order above
preserves each of them and makes the Python second pass unnecessary.

Carriers never enter a node's scalar layout from the C++ side: a C++
implementation receives its types as template parameters, so a ``TypeArg``
is consumed by dispatch. Graph overloads read it from the resolved call when
they need it. For Python implementations the bridge delivers the
materialised Python object where the trampoline delivers the projected
value today (see *Bridge contract*); how a Python node stores it afterwards
is unchanged.

Declaring carriers from C++
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Candidate parameters are not read from the ``Operator<...>`` marker, which
is documentary (``operators.rst``: "the operator signature is a suggestion,
not a rule"). ``register_overload<Op, Impl>()`` derives them from the
*implementation*: ``build_node_params<Impl>`` / ``build_graph_params<Impl>``
(``operator_dispatch.h``) walk ``signature_args_of<Impl>`` -- the parameter
list of ``Impl::eval`` (node overloads) or ``Impl::compose`` (graph
overloads), or an explicit ``Impl::signature_args`` tuple -- and lower each
``In<>``/``Scalar<>``/``VarIn<>``/``VarKwIn<>``/``Out<>`` to a
``ParamPattern``. A carrier is declared the same way, as a parameter type in
that list, at the position the call expects it:

.. code-block:: cpp

   /** A wiring-time type argument. ``Pattern`` is the carried pattern
       (``TsVar<"O">``, ``TS<ScalarVar<"T">>``, ``ScalarVar<"T">``,
       ``SizeVar<"N">``); ``Default`` is ``void`` (required), ``AutoResolve``
       (deferred from ``Pattern`` itself) or another pattern (deferred from
       that pattern). A concrete default comes from the ``defaults()`` hook
       as a ``TypeCarrier`` value, like any scalar default. */
   template <fixed_string Name, typename Pattern, typename Default = void>
   struct TypeArg;   // empty at evaluation: the dispatcher consumed it

   struct const_source
   {
       static constexpr auto name              = "const";
       static constexpr bool schedule_on_start = true;
       static void resolve_default_types(ResolutionMap &resolution) { const_resolve_output(resolution); }
       static void eval(Scalar<"value", ScalarVar<"T">> value,
                        TypeArg<"tp", TsVar<"S">, AutoResolve>,     // position 1, as in 0.5
                        Out<TsVar<"S">> out)
       {
           const_apply(value.value(), out);
       }
   };
   // const_delayed declares (value, tp, delay, out); nothing declares (tp, out).
   // The documentary markers list the carrier too, so labels and the Python
   // signature render it:
   struct const_ : Operator<"const", Scalar<"value", ScalarVar<"T">>,
                            TypeArg<"tp", TsVar<"S">, AutoResolve>,
                            Scalar<"delay", TimeDelta>, Out<TsVar<"S">>> {};

The lowering and the runtime treat it as follows:

* ``build_node_params`` / ``build_graph_params`` lower ``TypeArg`` to
  ``ParamPattern{Kind::TypeArg}`` with ``carrier`` derived from ``Pattern``
  (a TS pattern, a scalar pattern, or a ``SizeVar``/fixed size) and
  ``default_pattern`` from ``Default``; ``apply_param_defaults`` accepts a
  ``TypeCarrier`` from ``defaults()`` for it.
* ``StaticNodeSignature`` excludes ``TypeArg`` parameters from the node's
  input, scalar and state layout: a carrier is not a runtime field. The
  ``eval`` invocation passes an empty ``TypeArg`` placeholder in its slot,
  so an implementation's types keep coming from its template parameters.
  Graph overloads (``compose``) receive the ``TypeCarrier`` from the
  resolved call when they need it.
* The ``wire<>`` call arm accepts a ``TypeCarrier`` (or a
  ``TSValueTypeMetaData*`` / ``ValueTypeMetaData*`` / ``std::size_t``, which
  it wraps) wherever a scalar argument is accepted, so C++ callers can write
  ``wire<nothing>(w, ts_schema<TS<Int>>())``.
* A resolver that infers the same variable (``const_resolve_output`` binds
  ``S`` from ``T``) binds only when the variable is still free, so a supplied
  carrier wins and the resolver keeps serving the omitted case, after which
  the deferred carrier materialises from ``S``.
* **Family consistency.** A parameter name that is a ``TypeArg`` in one
  candidate of an operator must be a ``TypeArg`` in every candidate that
  declares that name; ``register_overload`` rejects a candidate that
  disagrees. This makes "is this argument a carrier?" a property of the
  operator, which arrival relies on (below), and is the registered form of
  the rule ``python_bridge.rst`` already states: which argument is a carrier
  is a property of the signature, never of the operator name.

Declaring the carrier on the concrete candidates is what retires the Python
name table: ``const``, ``nothing`` and ``replay`` get their ``tp`` parameter
at the index the table hard-codes, which is also the 0.5 reference's
signature (``const(value, tp=AUTO_RESOLVE, delay=MIN_TD)``).

Errors
~~~~~~

All carrier failures are candidate failures that surface through the
existing "no matching overload" report, with these messages added to the
per-candidate reason: form mismatch (above), carried type mismatch
(*"parameter 'tsd_type' expects type[TSD[K, TS[int]]], got TSD[str, TS[str]]"*),
unresolved deferred carrier (above), missing required carrier (above). The
Python bridge maps them to ``WiringError`` as it maps every other dispatch
failure.

Bridge contract
---------------

Arrival
~~~~~~~

``py_wiring.cpp`` converts an argument to a ``TypeCarrier`` when it can only
be a type, and otherwise only when it *targets a carrier parameter*:

* a ``PyTsType`` (``TS[int]``, ``TSD[str, TS[int]]``, ``REF[...]``) is
  unambiguous and always arrives as ``TypeCarrier{ts}`` (today:
  ``PyTsMetaRef``); a ``Size[n]`` object likewise as ``TypeCarrier{size}``;
* a Python ``type`` object is ambiguous -- ``const(OpaqueBase)`` passes a
  class as *data* and must keep producing ``TS[object]`` -- so it arrives as
  ``TypeCarrier{scalar = _value_type(cls)}`` (a ``TimeSeriesSchema``
  subclass as ``TypeCarrier{ts = TSB[cls]}``, the services'
  ``_specialization`` rule) only when the name or position it fills is a
  carrier parameter of the operator being called, as reported by
  ``OperatorRegistry::carrier_parameters(name)`` from the family-consistency
  rule above; anywhere else it stays an opaque scalar value, exactly as
  today;
* everything else arrives as today (ports, ``WiredFn``, scalar values, node
  handles).

The Python paths that do not go through ``OperatorRegistry::resolve``
(``@graph`` auto-resolution, ``_PyNode`` calls, service specialisation) know
their own signature's ``type[...]`` parameters and mint the carrier for those
explicitly through ``_hgraph.type_carrier(x)`` before calling
``match_carrier``. No path decides from the object alone.

``ResolutionScope``
~~~~~~~~~~~~~~~~~~~

``PyResolutionScope`` (``py_type_system.cpp``) gains two methods, both thin
wrappers over core:

.. code-block:: python

   scope.match_carrier(param_pattern, value) -> bool      # type_carrier_match on scope.map
   scope.materialise(param_pattern) -> TsType | type | Size | None   # deferred default, projected

``match_carrier`` is what the Python paths that do not go through
``OperatorRegistry::resolve`` (``@graph`` auto-resolution, ``_PyNode``
calls, service specialisation) use instead of their own binding algebra, so
all three use the one matcher. ``materialise`` projects with the reverse
binding below and lands with it.

Materialisation to Python
~~~~~~~~~~~~~~~~~~~~~~~~~

``operator_scalar_to_py`` already unwraps ``PyTsMetaRef`` to a ``PyTsType``
for the wire trampoline and the resolver/requires bridges. It unwraps a
``TypeCarrier`` by form: TS to ``PyTsType``; scalar to
``python_type_for_value(meta)``; size to ``Size[n]``. A ``Size`` carrier
therefore reaches a Python body as a ``Size`` object however it was spelled
(today a subscript-pinned size arrives as a plain ``int``; see
*Compatibility*).

Reverse binding: one registry, not two dictionaries
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The bridge gains ``python_type_registry()``: a bridge-side map from
``const ValueTypeMetaData *`` to a strongly held ``nb::object`` (the Python
annotation that produced the schema), exposed as
``_hgraph.bind_python_type(value_type, python_object)`` and written by
``_value_type`` in ``_types.py`` for every schema it produces, not only TS
payloads. ``python_type_for_value`` consults it first, after the
``any -> object`` rule and before the opaque/native/bundle/enum/builtin
chain. The first registration for a schema wins; a later registration with a
different object for the same schema is ignored, so aliases
(``Mapping[str, int]`` after ``dict[str, int]``) are deterministic and the
sweep's lattice uses canonical spellings.

``reset_registries()`` (``python/module.cpp``) clears it with the other
bridge registries, so the slot dies with the metadata. That is the defect
the dictionaries carry today: nothing clears them, and a freed handle reused
for a new schema aliases the old annotation.

``_TS_SCALAR_TYPES`` is keyed by TS handle and only ever yields the TS's
value type's annotation; it collapses into the same registry through
``python_type_for_value(ts_value_vt(handle))``. Both dictionaries, their six
writes, ``_resolution_value_type``, the ``_time_series_full_value_type``
read, and the two reads in ``reflection.py`` are deleted.

This is the blueprint's ``python_type`` slot, moved out of
``ValueTypeMetaData``: a ``void *`` on core metadata would put a Python
object into the type layer that family 6 of the retrospective is removing
Python from, and would change the metadata ABI. The bridge map follows the
existing ``bundle_class_info_registry()`` precedent instead.

Python contract
---------------

One subscript rule
~~~~~~~~~~~~~~~~~~

``_pin_type_arguments(signature, items, *, default_var, kind)`` in
``_resolution.py`` returns ``{variable_name: pinned_value}``:

1. slice items (``fn[VAR: X]``) pin the named variable; the name must be a
   variable of the signature;
2. bare items fill the ``DEFAULT[...]`` variable first, then the remaining
   variables in order of first appearance in the signature; a single bare
   item with no ``DEFAULT`` and more than one remaining variable, or more
   bare items than remaining variables, raises ``WiringError``;
3. each pinned value is classified: ``Size[n]``/``int`` is a size pin; a
   ``TS`` expression is a TS pin; a class is a scalar pin; a
   ``TimeSeriesSchema`` subclass is ``TSB[cls]``.

Callers: ``_Operator.__getitem__``, ``_PyNode.__getitem__``,
``_GraphFn.__getitem__``, the three service stubs.
``_OperatorFunction.__getitem__`` (registry operators without a Python
signature) keeps only its registry-driven bare-item rule
(``operator_output_is_selective``: output or input constraint) and derives
named pins from the same helper; its ``with_columns`` and ``getattr_``
branches become declarations on those overloads (a ``TS[Frame[SCALAR]]``
output pattern with a scalar carrier; a ``requires=``/output pattern on the
descriptor overload).

The pins are applied by ``scope.match_carrier`` against the parameters'
carried patterns, so a pin that contradicts an annotation
(``key_name[TSD[str, TS[str]]]`` on ``type[TSD[K, TS[int]]]``) fails at
subscript time with the C++ message.

One collector
~~~~~~~~~~~~~

``_type_variables_of`` (``_types.py``) is the collector, with the
``type[VAR]`` case folded in. ``_service_type_variables`` and
``_annotation_type_vars``/``_PyNode._ordered_type_vars`` become calls to
it. It already visits in annotation order, which rule 2 above relies on.

Deleted
~~~~~~~

After the matcher and the registry land: ``apply_type_carriers`` and its
two passes; the wire-trampoline value pass; ``_match_type_argument``,
``_bind_native_resolution``, ``_binding_for_type_value``;
``_PyNode._resolved_placeholder_value`` and the explicit-``type[...]``
branch of ``_PyNode.__call__``; the carrier half of ``_graph_auto_resolve``;
``_apply_service_defaults`` (a ``default_pattern`` in C++);
``_OperatorFunction._normalise_type_arguments`` and the
``{"const": 1, "nothing": 0, "replay": 1}`` table; the ``apply`` and
``const`` ``output_type`` branches (``apply`` gains a ``resolvers=`` on its
overload that reads the callable's return annotation -- a Python callable is
only introspectable from Python -- and ``const`` infers its output through
the declared carrier and the scalar's registered type); the ``__type_arg__``
lowering in ``_register_overload``, which now emits ``TypeArg`` patterns;
``_TS_SCALAR_TYPES``, ``_VALUE_SCALAR_TYPES``, ``_resolution_value_type``.

Kept in Python, and why
~~~~~~~~~~~~~~~~~~~~~~~

``default_type_var_of`` (``DEFAULT[...]`` is a Python-only spelling that is
stripped before lowering); the ``AUTO_RESOLVE`` and ``DEFAULT`` objects (the
API); ``_apply_resolvers`` (user resolver callables are Python, invoked from
the C++ ``default_resolver`` bridge as today); the service registration
ledger (``_materialize_*``, ``_pending_registrations`` -- adjacent, not a
carrier; out of scope); ``_requested_input_shape`` (union inputs, not a
carrier); the ``const`` opaque-class pre-registration for an *instance* of an
unregistered class (``const(SomeClass())``; RFC 0004 territory, pinned by
``test_const_of_an_unregistered_class_registers_it_first``); the
``getattr_``/``getitem_`` call fast paths and the record/replay durability
hooks in ``_core.py``, which are not carrier decisions and are tracked by
their own families.

Runtime representation and serialisation
----------------------------------------

None. A carrier is consumed at wiring. ``TypeCarrier`` is a standard scalar
so it can travel through ``Value`` and the resolved call, but it is never
placed in a node's scalar layout by core, never ticks, and is not part of
recorded or replayed state. Python nodes that keep the materialised Python
type object among their scalars keep it as the opaque object they keep
today. Serialised forms (record/replay, manifests, checkpoints) are
untouched.

Compatibility and migration
---------------------------

The sweep pins today's behaviour; each cell below changes deliberately in
the PR named, and the pin flips in that PR with the diff saying so. Every
other cell of the sweep stays green through every PR.

.. list-table::
   :header-rows: 1
   :widths: 44 30 26

   * - Behaviour
     - Today (pin)
     - After
   * - Bare ``op[X]`` on an ``@operator`` with two free variables and no
       ``DEFAULT``
     - accepted, meaning deferred to the registry
       (``test_operator_bare_item_with_two_variables_and_no_default_is_accepted``)
     - ``WiringError`` at subscript time, as a node already does (PR D)
   * - Bare item on a generic ``@adaptor`` stub, ``stub[int]``
     - refused with "requires TYPEVAR: concrete" while a reference service
       and a service adaptor bind the sole variable
       (``test_adaptor_bare_subscript_is_rejected_even_for_a_sole_variable``,
       ``test_service_adaptor_bare_subscript_binds_the_sole_variable``)
     - binds the sole variable on every stub (PR D)
   * - ``type[TSD[K, TS[int]]]`` graph argument given ``TSD[str, TS[str]]``
     - ``K`` bound, element unchecked
       (``test_graph_ts_type_argument_binds_variables_without_validating_the_rest``)
     - ``WiringError`` (PR D)
   * - ``Size`` pinned by subscript, ``g[Size[3]]``
     - body sees ``3``
       (``test_size_carrier_by_subscript_materialises_as_a_plain_int``)
     - body sees ``Size[3]``, as the ``AUTO_RESOLVE`` form and the 0.5
       reference do (PR D)
   * - Two bare items on a node whose ``DEFAULT`` variable is not first in
       appearance order
     - first-appearance order
     - ``DEFAULT`` first, then first-appearance (PR D; the graph rule, made
       common)
   * - Bare items on a graph filling variables that appear only in
       time-series inputs
     - not fillable (graph fills carriers only)
     - fillable, as on a node (PR D)
   * - ``const(value, <positional non-type>)``
     - reaches ``delay``
     - rejected; ``delay`` is keyword-only after ``tp`` as in the 0.5
       signature (PR B; the implementation PR sweeps the tree for positional
       ``delay``)
   * - Alias annotations for one schema
     - last ``_value_type`` call wins
     - first wins (PR C)
   * - ``reset_registries()`` then a new schema at a recycled address
     - stale annotation (latent)
     - cleared with the registries; regression test added (PR C)

No user-facing API is added or removed: every spelling in the sweep keeps its
meaning. The 0.5 parity harness (``tools/parity``) is the external oracle;
its operator and wiring shards run unchanged on each PR.

Performance and memory
----------------------

Wiring-time only. A ``TypeCarrier`` is 16 bytes and lives in the resolved
call for the duration of one dispatch. Per candidate, a supplied carrier costs one pattern match
instead of a throwaway variable bind plus a Python re-validation; a deferred
carrier costs one map lookup per fixed-point pass. The Python two-pass
(which ran user resolvers twice) and the trampoline value pass disappear, so
``construct_dispatch_cases`` and ``construct_overloads``
(``benchmarks/scenarios.py``) are expected neutral to better. PR D measures
them with the interleaved A/B recipe in ``benchmarks/README.md`` (two wheels,
alternated runs) rather than a single cross-run comparison, which carries
about ten per cent of machine noise.

The reverse-binding registry holds one ``nb::object`` per schema the DSL
produced -- the same set the two dictionaries hold today -- bounded by the
interned schema count and freed on reset. The per-tick path is untouched;
``test_registry_snapshot.py`` keeps asserting evaluation takes no
type-system lock.

Installed-extension and ABI consequences
----------------------------------------

* ``ParamPattern`` gains a kind value and three fields, and
  ``operator_dispatch.h`` gains ``TypeArg``/``AutoResolve``. Extensions that
  register operators against the SDK headers recompile; there is no source
  change for them unless they construct ``ParamPattern`` by hand. This is
  within the pre-1.0 native SDK policy (RFC 0005: no ABI promise before the
  freeze).
* ``TypeCarrier`` is a new exported standard scalar binding in
  ``type_resolution.h`` (``HGRAPH_DECLARE_STANDARD_SCALAR_BINDING``).
* ``PyTsMetaRef`` is removed from ``python/py_carriers.h``; it is
  bridge-internal and not part of the extension SDK.
* ``ValueTypeMetaData`` does not change.
* Python: ``_hgraph.bind_python_type``, ``ResolutionScope.match_carrier``
  and ``ResolutionScope.materialise`` are added to the private ``_hgraph``
  module; nothing in the public ``hgraph`` surface changes.

Alternatives considered
-----------------------

``ScalarPattern::Kind::TypeCarrier`` (the blueprint's first sketch)
   Rejected. ``scalar_pattern_match`` receives a schema, and a carrier's
   information is in its value; a TS child needs a ``TypePattern`` inside
   ``ScalarPattern``, which is defined first and would need a
   ``shared_ptr`` indirection; and a carrier is a role of the parameter, which
   is where rank and defaults already live.

A ``default_from_var`` string on ``ParamPattern``
   Rejected in favour of ``default_pattern``: a service default such as
   ``tp: type[X] = TS[K]`` is a pattern, not a variable, and the pattern form
   subsumes ``AUTO_RESOLVE``, ``DEFAULT[X]`` and ``= X``.

A ``void *python_type`` slot on ``ValueTypeMetaData``
   Rejected. It leaks a Python object into the type layer the project is
   removing Python from, changes the metadata ABI, and couples Python
   lifetime to interned metadata. A bridge-side registry keyed by the
   metadata pointer is the established pattern and resets with the rest.

Unify the carrier rules in Python only
   Rejected. The resolver ordering (carriers visible to ``resolvers=`` in one
   pass) cannot be fixed from outside the candidate loop; the name table
   stays because Python cannot declare a carrier in a C++ operator's
   signature; and C++ callers still cannot pass a type argument.

TS-only carriers
   Rejected. ``type[SCALAR]``, ``type[SIZE]`` and collection carriers are
   all in use (sweep groups A and B).

Arrival by object identity alone
   Rejected (review finding). Converting every Python class to a carrier
   before knowing the parameter it fills conflates a class used as data
   (``const(OpaqueBase)`` is ``TS[object]`` on purpose) with a type
   argument, and would bind ``const``'s value variable to the carrier
   schema. Arrival is role-directed: unambiguous type objects always, bare
   classes only into carrier parameters.

A three-slot carrier struct
   Rejected (review finding). A carrier is one of three forms at a time;
   two pointers and an optional size is 32 bytes of mostly-empty slots
   where a closed sum type is the contract and costs 16.

``TypeArg`` on the ``Operator<>`` marker only
   Rejected (review finding). The marker is documentary; candidates derive
   their parameters from each implementation's ``eval``/``compose``
   signature, so the carrier has to be declared there to change dispatch.

Unresolved questions
--------------------

1. Whether the family-consistency rule should also require carrier
   parameters to sit at the same *position* in every candidate that
   declares them. Names suffice for arrival by keyword; a positional bare
   class is classified by the position's role across the family, which
   only matters if candidates disagree. Registration can start by rejecting
   disagreement and relax later.
2. Which of the positional-``delay`` call sites for ``const``, if any, exist
   in the tree and downstream. PR B answers it with a sweep and either keeps
   the change or adds a deprecation path.
3. Whether ``apply`` should keep a resolver on the overload permanently or
   whether a declared ``TypeArg<"output_type", TsVar<"O">>`` with the return
   annotation projected at arrival is cleaner. PR D decides; both remove
   the name branch.

Acceptance criteria and test plan
---------------------------------

C++ (Catch2, ``tests/cpp``; PR B)
   ``type_carrier_match`` binds TS, scalar and size variables; a form
   mismatch and a carried-type mismatch reject the candidate with the
   messages above; a concrete carrier default binds before matching; a
   deferred carrier materialises after ``default_resolver`` and before
   ``requires_predicate``; a carrier defaulting to ``OUT`` materialises from
   the resolved output and a carrier supplying a variable the output needs is
   materialised first (the fixed point); ``type[TS[int]]`` outranks
   ``type[TS[SCALAR]]``; ``wire<nothing>`` and ``wire<const_>`` accept a
   ``TypeCarrier`` from C++; ``register_overload`` rejects a candidate whose
   ``tp`` is not a ``TypeArg`` when the family declares it as one, and
   ``carrier_parameters("const")`` reports ``tp``; ``StaticNodeSignature``
   leaves a ``TypeArg`` out of the scalar layout.

Python (PR B, C, D)
   ``python/tests/test_type_carrier_sweep.py`` stays green through PR B and
   PR C with no cell changed. PR C adds the reset-aliasing regression and the
   first-wins alias cell. PR D flips exactly the cells in the compatibility
   table and adds the fixed-point ordering cell. The ported upstream suites
   and the REF-consumer sweep stay green throughout.

Ratchets (``python/tests/test_architecture_ratchets.py``)
   ``types-shadow-schema-dicts`` 10 to 0 (PR C);
   ``wiring-type-carrier-sites`` 20 to 0 (PR D);
   ``wiring-operator-name-branches`` 8 to 4 (PR D: ``apply``, ``const``,
   ``with_columns`` and the ``getattr_`` subscript branch go; the two
   record/replay durability hooks and the ``getattr_``/``getitem_`` call
   fast paths remain and belong to other families). Baselines move in the
   same PR as the code, with ``testing.rst`` updated.

Parity
   The ``tools/parity`` nightly shards run on each PR's wheel; the campaign
   catalogue gains carrier templates for the fix shapes the retrospective
   catalogued (positional ``tp``, ``DEFAULT`` before ``AUTO_RESOLVE``, size
   pins, collection carriers).

Documentation, in the same change as the code (CLAUDE.md section 2)
   ``operators.rst`` gains a "Type arguments" section (the ``TypeArg``
   descriptor, ``TypeCarrier``, the dispatch order) and updates the
   ``const``/``nothing`` catalogue entries; ``python_bridge.rst`` "The erased
   operator contract" gains the carrier paragraph and the reverse-binding
   registry joins the bridge registries table; ``testing.rst`` records the
   new baselines and the flipped pins. This RFC moves to ``Accepted`` in
   PR D.

Implementation plan
-------------------

The stages are ordered so that no PR needs a Python behaviour change before
the machinery it relies on exists. Each PR is independently reviewable and
leaves the tree green.

PR A -- pins (merged as #662)
   ``test_type_carrier_sweep.py`` and the ``wiring-type-carrier-sites``
   ratchet.

PR B -- core matcher and C++ declaration
   ``TypeCarrier``; ``ParamPattern::Kind::TypeArg``, ``carrier``,
   ``default_pattern``; ``type_carrier_match``; the candidate-loop branch and
   the deferred fixed point; ranking; ``TypeArg``/``AutoResolve`` descriptors
   and their ``to_pattern`` lowering; ``const``, ``nothing``, ``replay``
   declare ``tp``; the bridge renames ``PyTsMetaRef`` to ``TypeCarrier`` at
   arrival and in ``operator_scalar_to_py`` and adds
   ``ResolutionScope.match_carrier``. Python wiring is untouched and keeps
   using its own algebra, which still works because the lowering it emits
   (``__type_arg__`` scalar variables) is unchanged until PR D.

PR C -- reverse binding
   ``python_type_registry()``, ``bind_python_type``, the
   ``python_type_for_value`` lookup, the reset clearing; ``_value_type``
   registers every schema it produces; the dictionaries and their readers
   are deleted; ``ResolutionScope.materialise``. Independent of PR B; it
   lands before PR D so that materialising a collection carrier from C++
   rebuilds ``tuple[int, ...]`` rather than a raw schema handle.

PR D -- Python cutover
   ``_register_overload`` emits ``TypeArg`` patterns; ``_pin_type_arguments``
   and the one collector; the deletion list; the ``apply`` resolver;
   ``with_columns``/``getattr_`` overload declarations; compatibility cells
   flipped; ratchets lowered; benchmarks re-run; RFC to ``Accepted``.

Risks, and the pin that gates each
   1. The bare-item order (graph vs node vs operator) -- sweep group E.
   2. ``const``/``nothing``/``replay`` positional carriers and ``const``
      inference -- sweep group F plus ``test_opaque_python_scalar.py``.
   3. ``Size`` carriers -- ``test_size_carrier_*`` in groups A and B.
   4. The ``OUT`` fallback order -- group C and the new fixed-point cell.
   5. ``operator_output_is_selective`` for registry operators -- the
      ``TestOperatorCarriers`` cells and the ported operator suites.
   6. Reverse-binding staleness across reset -- the new PR C regression.
   7. The service ledger and the "requested output" ``DEFAULT`` check are
      adjacent, not carriers, and stay out of these PRs.

Implementation status
---------------------

Proposed. PR A (#662) is open in the hardening stack (#658 to #663) and
carries the sweep and the ``wiring-type-carrier-sites`` ratchet; this RFC's
branch is based on ``main`` and is documentation only, so the sweep is not
in its history. PR B branches from the stack once #662 has landed. The C++
survey that produced this design is recorded in the retrospective notes and
in the sweep's module docstring. No implementation has started.

References
----------

* ``docs/source/developer_guide/operators.rst`` -- "``TypePattern`` -- the
  one matcher", "``WiringArg`` -- erased arguments", "Named arguments,
  defaults and ``**kwargs``", "Higher-order operators and the ``WiredFn``
  scalar".
* ``docs/source/developer_guide/python_bridge.rst`` -- "The erased operator
  contract".
* ``docs/source/developer_guide/testing.rst`` -- "Architecture ratchets",
  "Authoring-shape sweeps".
* ``python/tests/test_type_carrier_sweep.py`` -- the 72 behaviour pins.
* RFC 0003 (Python scalar registration), RFC 0004 (python-owned structured
  scalars), RFC 0005 (1.0 API and release process).
* hgraph 0.5 (``release/0.5``): ``hgraph/_wiring/_wiring_node_signature.py``
  and ``_operator.py`` -- the reference semantics for ``AUTO_RESOLVE``,
  ``DEFAULT`` and ``Size``.
