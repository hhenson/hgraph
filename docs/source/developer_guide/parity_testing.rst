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
Input names, tick counts, scalar types, expression operations, and template
parameters are validated before either runtime imports hgraph.

``tools/parity/catalog.py`` is the trusted language boundary.  Each template
declares its inputs, feature tags, operator coverage, comparison tolerance, and
public hgraph construction.  The same source executes in both environments.
Do not add runtime semantics or compatibility shims to the harness.

Generation and Coverage
-----------------------

Hypothesis recursively composes validated scalar expressions and produces
stateful multi-cycle sequences for feedback, switching, and keyed TSD
lifecycle.  Generated exploration uses 8--32 ticks by default; a reducer may
shrink a failure below that range.

Coverage is semantic rather than only line-based.  Reports count templates,
time-series shapes, scalar types, operator families, topology, lifecycle
events, tick lengths, and feature pairs.  They also compare the template
catalogue with the runtime's public operator inventory, so unsupported
generation surface remains visible.

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
default — the identity — and is outside the family).  Family suppression
covers only the deviation's own shape: both implementations completed and
disagreed on a trace value.  A candidate crash or status difference inside
the same parameter space still verifies and publishes normally.  A mismatch
matching a family is classified as a known failure on first detection,
before any verification replays or reduction budget is spent, because each
new minimized variant would otherwise mint a fresh fingerprint.  For the same reason the generator does not explore
documented-unspecified space at all (e.g. ``tsd_map_reduce`` recipes are
generated with the identity zero only); the corpus keeps explicit deviation
recipes as permanently known mismatches.  The publisher additionally files at
most one issue per fingerprint per run.

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

The nightly workflow builds one candidate wheel, runs four bounded shards, and
uploads complete reports.  Campaign jobs have read-only permissions.  A
separate default-branch publisher receives only the validated report artifacts
and has ``issues: write`` permission.  It creates or reopens deduplicated
``bug``/``parity`` issues and then exits; individual crashes and mismatches do
not stop later recipes.

Model-assisted catalogue evolution is deliberately outside GitHub Actions.
Invoke the ``hgraph-parity`` Codex skill during weekly maintenance.  It uses a
cost-efficient model for constrained coverage proposals and reserves frontier
analysis for a stable mismatch which deterministic reduction cannot simplify.
No model credential is stored in this repository.
