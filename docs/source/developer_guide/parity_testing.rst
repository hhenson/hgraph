Differential Parity Testing
===========================

The parity harness continuously compares the public Python authoring surface of
the maintained Python-first hgraph 0.5 runtime with C++-first hgraph in this
repository.  It complements the fixed compatibility suite: generated
recipes explore multi-tick combinations which have not yet become ordinary
regression tests.

The Python-first ``hgraph==0.5.41`` release is the behavioral oracle. The
harness pins that version at campaign setup, records its exact distribution
environment, and uses it for the complete run. The candidate ``hgraph`` wheel
is built and installed into a separate environment. Every recipe then runs in a
fresh isolated process, so imports, registry state, crashes, and timeouts cannot
leak between implementations or stop the rest of a campaign.

Quick Start
-----------

Install the optional controller dependency and validate the committed corpus:

.. code-block:: bash

   uv pip install --python .venv/bin/python "hypothesis>=6"
   .venv/bin/python -m tools.parity validate

Prepare the two isolated runtime environments:

.. code-block:: bash

   .venv/bin/python -m tools.parity setup

Run the bounded pull-request profile:

.. code-block:: bash

   .venv/bin/python -m tools.parity campaign --profile pr

Results are written below ``.parity/results``.  The reference environments,
candidate wheels, and reference traces are content-addressed below
``.parity``.  A change confined to recipes, tests, tooling, or documentation
does not alter the candidate wheel fingerprint and therefore does not rebuild
the native artifact.

Useful focused commands are:

.. code-block:: bash

   python -m tools.parity replay tools/parity/corpus/issue-40-no-key-rebind.json
   python -m tools.parity reduce path/to/failing-recipe.json
   python -m tools.parity coverage --inventory
   python -m tools.parity catalogue

Upstream conformance suite
--------------------------

Generated recipes cannot prove that existing applications will continue to
wire: the generator exercises only its trusted catalogue.  The upstream
conformance tier therefore checks out the tag matching the installed reference
distribution and stages its exact ``hgraph_unit_tests`` and ``examples`` trees
without the upstream ``hgraph`` package.  The same unmodified test sources then
run in separate reference and candidate environments:

.. code-block:: bash

   python -m tools.parity conformance --profile operators
   python -m tools.parity conformance --profile core --exit-zero
   python -m tools.parity conformance --profile all --with-extras --exit-zero

``operators`` is the focused public-operator tier.  ``core`` adds runtime,
types, wiring, nodes, test helpers, and direct time-series tests.  ``all`` also
collects the adaptor, Arrow, NumPy, debug, and example suites; use
``--with-extras`` so optional-import results are meaningful.  An existing clean
checkout of the exact release tag may be supplied with ``--upstream-source``.
The default candidate environment contains only the core wheel.  For a public
module owned by a separately installed extension, create a candidate
environment containing the matching core and extension wheels and pass its
interpreter with ``--candidate-python``.  A core-only collection error records
the approved package-ownership boundary; it is not evidence for the
extension's behavior.
The report records the reference version, tag, commit, in-tree declared
version, and digest of the staged test and example trees.  The declared version
is evidence rather than the source selector: hgraph release automation may tag
the release commit before its follow-up ``pyproject.toml`` version bump, so the
exact release tag remains authoritative and any discrepancy stays visible.
The controller installs one explicit conformance dependency set into both
environments, verifies every version is identical, and records that package
inventory in the report before it runs either suite.

An upstream test failure is not automatically a C++-first hgraph defect.  The
runner first requires the released reference assertion to execute successfully
(``PASS`` or ``XPASS``).  A candidate difference is then classified using
``tools/parity/upstream_conformance.json``:

* an unmatched difference is **review required**, not a confirmed problem;
* ``expected-change`` records an approved public semantic difference;
* ``converted`` records an upstream internal test whose contract is exercised
  through named public Python and native C++ replacement evidence;
* ``confirmed-gap`` is used only after review has established a compatibility
  defect.

Accepted rules are deliberately narrow.  They name the upstream node-id shape,
allowed candidate outcome, expected diagnostic, reason, decision record, and
review date.  A converted rule additionally names existing replacement tests.
A changed failure diagnostic falls out of the rule and returns to review.
There is no blanket private-internal exclusion: a test coupled to a non-ported
concept such as ``HgTypeMetaData`` must be converted to the public reflection
and type-resolution surface with equivalent evidence, or the required concept
must be ported.  A manifest exclusion is reserved for a whole upstream module
whose subject has been explicitly ruled experimental and outside the supported
user contract.  Each exclusion names one exact test file, its reason, and its
review date; the file is not collected and does not contribute to audit totals.

A test skipped by the released reference can only be converted when a rule
names ``skipped`` explicitly and matches the reference's exact skip diagnostic
as well as the candidate diagnostic.  This is reserved for a source test whose
supported contract is established independently and whose public Python and
native C++ replacement tests are named as evidence; other skips remain
reference-unverified.  Executing a skipped body may establish that it is not a
released oracle, but does not by itself supply reference behavior.

Reference collection failures and nondeterministic/platform-disabled tests are
reported as unverified evidence, never candidate noncompliance.  Reports and
raw pytest output are written below ``.parity/results``.  Use ``--exit-zero``
for discovery while rules are being reviewed; the normal command fails while
review-required, confirmed-gap, ambiguous-rule, or reference-unverified
entries remain.

An upstream test explicitly marked XFAIL because it is polluted by suite
context may opt into isolated reference replay through a converted rule.  The
controller reruns only that exact node id in a fresh reference process, accepts
only PASS or XPASS, and retains both the suite and isolated results in the
report.  A failed isolated replay remains reference-unverified.  This mechanism
does not retry ordinary failures or skips and does not run candidate code in
the reference process.

``--reference-python`` and ``--candidate-python`` select already-provisioned
interpreters.  ``--candidate-wheel`` installs a pre-built stable-ABI wheel,
which is how CI reuses the wheel already built by the distribution workflow.

Recipe Contract
---------------

Recipes are versioned JSON data, not executable Python.  A recipe selects a
checked-in graph template, supplies per-cycle inputs and bounded scalar
parameters, and records semantic coverage tags:

.. code-block:: json

   {
     "schema_version": 1,
     "id": "feedback-accumulate-sparse",
     "template": "feedback_accumulate",
     "inputs": {"value": [1, 2, null, 3, -1, null, 5, 0]},
     "parameters": {"initial": 7},
     "features": [
       "shape:TS",
       "topology:feedback",
       "lifecycle:multi-cycle"
     ]
   }

``null`` means no tick.  Tagged objects represent identity-sensitive deltas,
for example ``{"$remove": true}`` for a TSD removal and
``{"$set_delta": {"added": [...], "removed": [...]}}`` for a TSS delta.
``{"$map": [[key, value], ...]}`` represents a mapping whose keys cannot be
expressed as JSON object strings, including integer-keyed TSD input.
Input names, tick counts, scalar types, expression operations, and template
parameters are validated before either runtime imports hgraph.

``tools/parity/catalog.py`` is the trusted language boundary.  Each template
declares its inputs, feature tags, operator coverage, comparison tolerance, and
public hgraph construction.  The same source executes in both environments.
Do not add runtime semantics or compatibility shims to the harness.

Generation and Coverage
-----------------------

Hypothesis recursively composes validated scalar expressions and produces
stateful multi-cycle sequences for feedback, switching, keyed TSD lifecycle,
reference, subscription, and request/reply services, automatic and
multi-client adaptors, context capture across switching branches, grouped
operator pipelines, and mesh lifecycle driven by a TSD key set.  TSD key-set
recipes include value-only updates,
same-cycle replacement, removal, repopulation, and empty transitions.

Most generated framework families pass their inputs through a fixed structural
``TSL`` projection before use.  This intentionally combines a
non-peered container with a ``REF``-producing child projection, so ordinary
operators and framework boundaries must consume REF-transparent sources.
The scalar-expression and scalar-operator-argument families retain direct
inputs to preserve an independent baseline and isolate public overload
selection from reference projection behavior.

The pull-request profile generates 96 cases of 8--32 ticks in addition to the
curated corpus.  The nightly profile generates 5,000 cases of 8--64 ticks and
may shrink a failure below that range.  Nightly-only parameter variants retain
high-value stress points, such as direct formatting of a selected REF, without
turning the merge smoke profile into a permanent failure.  Ruled parameter
spaces which carry no differential signal, such as undeduplicated key-set
cardinality, remain corpus-only.

Generated discovery targets the agreed compatibility contract, not accepted
deviations.  The fixed corpus permanently retains the latter, but unrestricted
generation works around them: every reduction supplies its explicit identity
zero; scalar operator recipes constrain zero divisors, powers, and
``DivideByZero`` policies to combinations which complete on released hgraph;
collection generation excludes operator/shape pairs absent from the released
public overload set; collection and nested scalar outputs use ``dedup`` to
normalize equal re-ticks; subscription keys include replacements and
re-subscriptions while using a non-zero multiplier; nested service-backed
invalid windows remain fixed-corpus cases while standalone service and
service-adaptor generators cover their agreed behavior; reference-unsupported
temporal method-call spellings and cyclic adaptor/outer-switch composition
remain ordinary compatibility or fixed coverage.  This keeps overload, churn,
branch lifecycle, service, and reduction coverage while reserving random
examples for previously unruled behavior.

Coverage is semantic rather than only line-based.  Reports count templates,
time-series shapes, scalar types, operator families, topology, lifecycle
events, tick lengths, and feature pairs.  They also compare the template
catalogue with the runtime's public operator inventory, so unsupported
generation surface remains visible.

The catalogue is deliberately targeted at compatibility-issue classes found
through production triage, starting with the 2026-07 batch (issues #74-#92)
and extending as new regressions are identified:

- ``scalar_expression`` const arguments exercise scalar auto-const overload
  selection (the #74/#78 exact-matching class);
- ``scalar_operator_arguments`` calls numeric arithmetic and comparison
  operators with a raw scalar on either side.  Division, floor division,
  modulo, and power additionally vary the wiring-time ``DivideByZero`` enum,
  including reference-valid zero-handling paths;
- ``compound_scalar_downcast`` preserves the released
  ``downcast_(Derived, ts)`` positional-target spelling across a realistic
  ``CompoundScalar`` event hierarchy while the same native checked narrowing
  also backs the output-selected ``downcast_[TS[Derived]](ts)`` spelling;
- ``enum_literal_selection`` selects between raw ``IntEnum`` and ``StrEnum``
  members to ensure Python auto-const conversion retains their nominal enum
  schema instead of treating them as their ``int`` or ``str`` base class;
- ``legacy_compound_scalar_json`` round-trips the release/0.5
  ``__serialise_base__`` and ``__serialise_discriminator_field__`` forms,
  including child-defined discriminator values and discriminators stored as
  ordinary CompoundScalar fields;
- ``temporal_expression`` drives date/datetime arithmetic into the upstream
  ``getattr_`` accessor tables — properties and method-call spellings, the
  ``(date - date).days`` shape included (the #82 class);
- ``collection_size`` covers the valid ``len_``/``is_empty``/``contains_``
  pairs over every upstream-supported sized shape (the #81 class); its
  no-change re-tick elision is the ruled deviation, bounded by the
  ``no-change-elision``
  relation in ``known_divergences.json``;
- ``lifecycle_state`` generates start/stop lifecycle functions across the
  accepted signature spellings — strict, bare-injectable, and unannotated
  name-match (the #79 class) — and alternates attribute and dictionary-view
  access over naked ``STATE``;
- ``realtime_default_start`` runs a bounded real-time graph and reduces its
  nondeterministic timestamp to whether the executor started after the
  simulation-only ``MIN_ST`` sentinel, pinning the release/0.5 default when
  no explicit start time is supplied;
- ``data_frame_recording`` records through the DATA_FRAME model and emits
  the frame a ``DataFrameStorage`` hands back to user code — column names,
  **timezone presentation**, and row values — so a tz-aware engine column or
  a changed frame surface is a trace difference (the PR #92 class). It varies
  between the default bitemporal names and names configured through
  ``set_table_schema_date_key``/``set_table_schema_as_of_key`` (the #417
  class);
- ``postponed_annotations`` is a generation MODE on the source-built
  templates: the generated module opts into PEP 563, so string annotations
  exercise signature resolution on both distributions (the #83 class);
- ``nested_higher_order`` targets the composition breeding ground directly:
  a key set GROWING AND SHRINKING under ``map_``/``mesh_``, a per-key
  ``switch_`` FLIPPING branches (nested graphs start/stop), request-reply
  and subscription services and adaptors living inside that structure, and
  optionally the whole pipeline under an outer ``switch_`` that tears it
  down and rebuilds it. Weighted double in the generator. Composition is
  restricted to upstream-supported space (a per-key adaptor client cycles
  released hgraph's toposort, so the adaptor consumes the reduced pipeline
  output; subscriptions subscribe per key outside the flipping switch), and
  the subscription variant avoids key re-adds inside this already-complex
  composition; standalone subscription generation covers that lifecycle.
  Its first differential replays surfaced issues #94 and #95.
- ``polymorphic_event_flow`` changes concrete Python ``CompoundScalar`` leaves
  behind one public base while exercising compute nodes, ``emit``, feedback,
  tuple/set/mapping conversion, ``collect``/``values_``, ``batch``, ``window``,
  JSON round trips, and in-memory component recording.  The base is consumed
  before extension leaves are declared, matching normal package import order.
- ``polymorphic_event_map`` combines the same changing event hierarchy with
  keyed child creation/removal, mapped ``emit`` and feedback, and an outer
  keyed ``emit``.  This permanently covers the production failure sequence
  where each individually correct operator lost the concrete leaf only when
  composed under ``map_``.
- ``structural_map_projection`` maps a typed child graph which performs keyed
  lookup into ``TSD[str, TSB[...]]``.  It varies forwarding the physical
  ``REF[TSB]`` terminal, materializing an owned bundle with ``combine``, and
  passing that bundle through dispatch.  It also maps a generic
  ``TSB[TS_SCHEMA]`` child through ``dereference`` and combines reference
  fields selected from captured dictionaries.  Keyed churn, explicit key
  sets, and ``pass_through`` keep peered/non-peered structural binding in the
  generated path.
- ``arrow_typed_projection`` crosses enum and polymorphic ``CompoundScalar``
  fields through pair/first/second projections, direct and configured
  ``debug_``, and both ordinary graph evaluation and Arrow's standalone
  ``eval_`` runner.  Its first seed found that ``eval_`` closed the graph before
  client-defined concrete leaves were registered.

When a new compatibility issue is fixed, extend this list: either an
existing template's generated space must provably contain the issue's
minimized shape, or a new template/mode is added alongside a
``coverage-*`` corpus recipe pinning it.

Add a template when a behavior cannot be represented by the existing bounded
language:

#. Implement the public graph construction in ``catalog.py`` without a module
   level hgraph import.
#. Declare its required inputs, operators, features, and any narrowly justified
   floating-point tolerance.
#. Add validation which rejects malformed or unbounded parameters.
#. Add at least one multi-tick corpus recipe and controller tests.
#. Confirm the recipe executes identically in the isolated reference and
   candidate environments.

Do not execute model-authored Python.  Model-assisted coverage work may propose
recipe JSON or catalogue changes, but deterministic validation, replay, and
review remain mandatory.

Mismatch Lifecycle
------------------

A potential mismatch passes these gates:

#. Replay both implementations three times in fresh processes.
#. Quarantine a failed or nondeterministic reference; it is not evidence of a
   C++-first hgraph defect.
#. Use Hypothesis prefix shrinking followed by aligned tick removal, event
   clearing, value simplification, and expression reduction.
#. Replay the minimized case three more times.
#. Compute a normalized fingerprint and suppress an already tracked
   divergence.
#. Publish one issue containing the minimized recipe, canonical traces,
   versions, seed, reduction history, and acceptance criteria.

Known differences live in ``tools/parity/known_divergences.json`` with their
issue, rationale, and review date.  They remain in the corpus: once a fix lands,
the same recipe changes from a known mismatch into a passing regression.

Two mechanisms keep an accepted deviation from re-filing as noise.  Exact
``divergences`` entries suppress a specific fingerprint.  ``families`` entries
suppress the deviation's whole parameter space — a template plus a
``parameters_not_equal`` map matching every recipe whose named parameters
differ from the stated identity (an omitted parameter runs at the template
default — the identity — and is outside the family; an **empty** map matches
every recipe of the template).  Parameter membership is necessary but not
sufficient.  Each family also names a bounded ``relation`` which proves that
the complete reference and candidate traces differ in exactly the documented
way:

* ``trace-value`` accepts only a trace value difference, used by the
  non-identity reduce-zero deviation;
* ``key-set-size-no-retick`` removes only ``size`` fields and then requires
  every remaining field to compare within the template's float tolerance;
* ``switch-flip-map-removal`` requires the candidate to remove exactly every
  currently-live mapped output, opposite the reference's canonical empty-map
  delta, on the exact ``beta``-to-request/reply-``alpha`` switch flip. It may
  additionally remove the one silent response-feedback cycle made obsolete by
  RFC 0014; the earlier response payload and every other tick must still match;
* ``request-reply-one-cycle-earlier`` requires a self-coupled request/reply
  candidate trace to equal the complete released-hgraph trace after removing
  exactly its leading silent response-feedback cycle;
* ``nested-request-reply-one-cycle-earlier`` permits an unchanged structural
  map prefix before that cycle so a reducer may remove unrelated later input
  events without minting a new divergence. The request/reply-backed ``alpha``
  branch must be active at the removed cycle, the advanced tick must carry a
  mapped response value, and every payload and remaining silence must agree.
* ``polymorphic-json-preserves-leaf`` covers the intentional improvement over
  0.5's lossy JSON reconstruction of a value declared as ``TS[Event]``.  It
  accepts the difference only when projecting each candidate concrete event
  back to the base (and dropping only the leaf fields) makes the complete
  traces identical.

An unknown relation, payload corruption, unrelated missing field, candidate
crash, or status difference does not match and therefore continues through
normal verification and publishing.  A mismatch satisfying both the parameter
and trace relation is classified as a known failure on first detection, before
any verification replays or reduction budget is spent, because each new
minimized variant would otherwise mint a fresh fingerprint.  The publisher
re-checks the same records (``tools/parity/known.py``) before creating or
reopening an issue, so a stale or concurrently produced report cannot re-file
a documented deviation.

For the same reason the generator does not explore ruled parameter spaces which
carry no independent signal (for example non-identity reduce zeros or
undeduplicated key-set size); the corpus keeps explicit deviation recipes as
permanently known mismatches.  The publisher additionally files at most one
issue per fingerprint per run.

A fix for Python-visible behavior must promote the minimized case to ordinary
public Python wiring coverage and add equivalent native C++ ``eval_node``
coverage.  The differential harness does not replace the C++-first testing
contract.

A divergence may instead resolve as an **accepted deviation** when the
reference behavior is an implementation artifact rather than a contract —
for example, a value that depends on the reference's internal capacity
history and that upstream's own suite never pins.  The resolution then
records the deviation in ``parity_matrix.rst``, keeps the fingerprint in
``known_divergences.json`` (pointing at that record), and still adds the
same two tiers of coverage — asserting the C++-first behavior, so the
intended semantics are pinned even though the traces intentionally differ.
The corpus recipe remains a permanently known mismatch.

CI and Security
---------------

The normal distribution workflow runs the curated corpus and a deterministic
generated matrix against its already-built Linux wheel under Python 3.14.
This pull-request job has read-only permissions, makes no model calls, and
never creates issues.

The nightly workflow builds one candidate wheel, runs eight bounded shards
over a deterministic 5,000-example matrix, and uploads complete reports.  Each
shard has a one-hour campaign budget and a 90-minute job ceiling; the example
limit remains the normal stopping condition, while the time budget prevents a
slow or pathological family from monopolizing a runner.  Campaign jobs have
read-only permissions.  A
separate default-branch publisher receives only the validated report artifacts
and has ``issues: write`` permission.  It creates or reopens deduplicated
``bug``/``parity`` issues and then exits; individual crashes and mismatches do
not stop later recipes.

Model-assisted catalogue evolution is deliberately outside GitHub Actions.
Invoke the ``hgraph-parity`` Codex skill during weekly maintenance.  It uses a
cost-efficient model for constrained coverage proposals and reserves frontier
analysis for a stable mismatch which deterministic reduction cannot simplify.
No model credential is stored in this repository.
