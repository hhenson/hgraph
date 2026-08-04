Differential Parity Testing
===========================

The parity harness continuously compares the public Python authoring surface of
the released Python hgraph runtime with the C++ runtime and compatibility bridge
in this repository.  It complements the fixed compatibility suite: generated
recipes explore multi-tick combinations which have not yet become ordinary
regression tests.

The released Python runtime is the behavioral oracle.  The harness resolves the
latest hgraph release once at campaign setup, records its exact version and
distribution environment, and uses it for the complete run.  ``hg_cpp`` is
built and installed into a separate environment.  Every recipe then runs in a
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

Twelve of the thirteen generated families pass their inputs through a fixed
structural ``TSL`` projection before use.  This intentionally combines a
non-peered container with a ``REF``-producing child projection, so ordinary
operators and framework boundaries must consume REF-transparent sources.
The remaining scalar-expression family retains direct inputs to preserve an
independent baseline.

The pull-request profile generates 48 cases of 8--32 ticks in addition to the
curated corpus.  The nightly profile generates 5,000 cases of 8--64 ticks and
may shrink a failure below that range.  Nightly-only parameter variants retain
high-value stress points, such as direct formatting of a selected REF, without
turning the merge smoke profile into a permanent failure.  Ruled parameter
spaces which carry no differential signal, such as undeduplicated key-set
cardinality, remain corpus-only.

Generated discovery targets the agreed compatibility contract, not accepted
deviations.  The fixed corpus permanently retains the latter, but unrestricted
generation works around them: every reduction supplies its explicit identity
zero; collection and nested scalar outputs use ``dedup`` to normalize equal
re-ticks; subscription keys include replacements and re-subscriptions while
using a non-zero multiplier; nested service-backed invalid windows remain
fixed-corpus cases while standalone service and service-adaptor generators
cover their agreed behavior; reference-unsupported temporal method-call
spellings and cyclic adaptor/outer-switch composition remain ordinary
compatibility or fixed coverage.  This keeps churn, branch lifecycle, service,
and reduction coverage while reserving random examples for previously unruled
behavior.

Coverage is semantic rather than only line-based.  Reports count templates,
time-series shapes, scalar types, operator families, topology, lifecycle
events, tick lengths, and feature pairs.  They also compare the template
catalogue with the runtime's public operator inventory, so unsupported
generation surface remains visible.

The catalogue is deliberately targeted at the compatibility-issue classes
production triage has actually produced (the 2026-07 batch, issues #74-#92):

- ``scalar_expression`` const arguments exercise scalar auto-const overload
  selection (the #74/#78 exact-matching class);
- ``temporal_expression`` drives date/datetime arithmetic into the upstream
  ``getattr_`` accessor tables — properties and method-call spellings, the
  ``(date - date).days`` shape included (the #82 class);
- ``collection_size`` covers ``len_``/``is_empty``/``contains_`` over every
  upstream-supported sized shape (the #81 class); its no-change re-tick
  elision is the ruled deviation, bounded by the ``no-change-elision``
  relation in ``known_divergences.json``;
- ``lifecycle_state`` generates start/stop lifecycle functions across the
  accepted signature spellings — strict, bare-injectable, and unannotated
  name-match (the #79 class);
- ``data_frame_recording`` records through the DATA_FRAME model and emits
  the frame a ``DataFrameStorage`` hands back to user code — column names,
  **timezone presentation**, and row values — so a tz-aware engine column or
  a changed frame surface is a trace difference (the PR #92 class);
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
#. Quarantine a failed or nondeterministic reference; it is not evidence of an
   ``hg_cpp`` defect.
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
  exactly the silent response-feedback cycle immediately before the first
  divergent response. An unchanged structural prefix is permitted so a
  reducer may remove unrelated later input events without minting a new
  divergence; every payload and remaining silent cycle must still agree.

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
same two tiers of coverage — asserting the ``hg_cpp`` behavior, so the
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
