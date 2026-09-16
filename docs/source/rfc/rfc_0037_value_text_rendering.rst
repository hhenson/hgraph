RFC 0037: A Value Has One Text
==============================

:Status: Accepted
:Author: Howard Henson
:Created: 2026-09-13
:Target: ``include/hgraph/types/value/value_ops.h``,
         ``include/hgraph/types/value/{compact,mutable}_container_ops.h``,
         ``src/hgraph/types/metadata/value_plan_factory.cpp``,
         ``src/hgraph/types/metadata/ts_data_{fixed_structured,slot,dynamic_list,window}_ops.cpp``,
         ``src/hgraph/types/value/any_ops.cpp``,
         ``include/hgraph/types/time_series/ts_data/ops.h``,
         ``include/hgraph/lib/std/operators/impl/conversion_impl.h`` (``str_``)
:Related: issues #819 (``str_``/``print_`` spellings), #831 (float precision);
          PRs #907, #919; developer guide ``operators.rst``,
          ``testing.rst`` ("Architecture ratchets")

Summary
-------

Rendering a value as text is implemented **46 times** across ten files.
Each site hand-writes its own brackets, separators and element recursion, and
which one runs for a given value is decided by runtime routing that is not
visible from the call site. This RFC replaces them with a single renderer that
takes a schema, a memory pointer and a *spelling*, and makes every ops table
delegate to it.

Motivation
----------

This is the **parallel abstractions** guardrail in ``CLAUDE.md`` — "one runtime
model, no generic fallback … two ways to do one thing is the smell to kill, not
add to" — at a count of 46.

The cost is not theoretical. Correcting one spelling (a TSB should render as a
dictionary: ``{'a': 1, 'b': 'x'}``) during #919 produced the following, all
measured:

* the spelling was installed at ``TSDataOps::format_string_impl`` — the hook
  added precisely for this purpose — and the output **did not change**;
* it was then installed at the ``value_indexed_ops`` fallback that hook
  delegates to, and the output **still did not change**;
* a third path serves it, and locating the first two required building
  markers into the binary and reading them back out of ``strings``;
* two earlier "fixes" in the same series were silent no-ops for the same
  reason, each caught only by a marker build.

The failure is silent in both directions. ``test_a_named_tsb_keeps_its_\
structural_mapping_rendering`` passes on the *default* spelling rather than on
the hook it appears to exercise: deleting the hook leaves the test green. A
test can therefore pin a behaviour it does not reach.

A human reviewer has no marker build. If tracing one spelling takes six
attempts with a compiler in the loop, the design is not reviewable.

Current state
-------------

.. list-table::
   :header-rows: 1
   :widths: 60 12

   * - File
     - Renderers
   * - ``include/hgraph/types/value/compact_container_ops.h``
     - 10
   * - ``include/hgraph/types/value/mutable_container_ops.h``
     - 8
   * - ``src/hgraph/types/metadata/ts_data_slot_ops.cpp``
     - 7
   * - ``src/hgraph/types/metadata/ts_data_fixed_structured_ops.cpp``
     - 6
   * - ``src/hgraph/types/metadata/value_plan_factory.cpp``
     - 5
   * - ``src/hgraph/types/metadata/ts_data_dynamic_list_ops.cpp``
     - 5
   * - ``src/hgraph/types/value/any_ops.cpp``
     - 2
   * - ``ts_data_window_ops.cpp``, ``type_registry.cpp``, ``type_realization.cpp``
     - 1 each

**46** in total. The count is reproducible, not quoted from memory --
``grep -cE "std::string [a-z_]*_(to|format|repr)_string\\("`` over those ten
files -- because the acceptance criteria pin it with a ratchet. 118
hand-written bracket and separator literals sit inside them.

Three spellings are already implied by the code and are the ones users see:

``diagnostic``
   What ``to_string`` produces today: ``{a: 1, b: x}``, bare and unquoted.
   Logs, error text and roughly 180 C++ assertions depend on it.

``str``
   Python's ``str()`` at the top level. A bare string is unquoted; a bool is
   ``True``; a float keeps its point.

``repr``
   Python's ``repr()`` — what a value looks like **inside** a container. The
   only scalar whose spelling differs from ``str`` is a string, which is
   quoted.

Proposal
--------

**There is one ``to_string``.**

The first draft proposed three spellings -- ``diagnostic``, ``str`` and
``repr`` -- chosen by a parameter. That was wrong, and wrong in the same way
the code was: it answered a duplication problem by keeping the duplication and
naming it. ``format_string`` and ``repr_string`` were added beside
``to_string`` during #907 and #919 and should never have existed.

``ValueOps`` carries ``to_string`` and nothing else. It is the value's own
representation -- a string is quoted, a bool is ``True``, a float keeps its
point, a container shows its real brackets. A diagnostic and the text a user
sees are the SAME text, because a diagnostic that disagrees with what the user
sees is a worse diagnostic.

Python's ``str`` differs in exactly one place: a string **at the top level** is
its characters rather than a quoted literal. That single exception is
``stdlib::python_str``, shared by the operators that mean Python's ``str``
(``str_``, ``format_``) -- a property of those operators, not a second spelling
replicated across the value layer.

The traversal contract
~~~~~~~~~~~~~~~~~~~~~~

The consolidated renderer does **not** take ``(schema, memory, spelling)``.
``ValueTypeMetaData`` is deliberately layout-free -- that separation is the
point of Schema versus Plan -- while ``memory`` is representation-specific: a
compact and a mutable container share a schema and do not share a layout. A
renderer dispatching on schema kind could not enumerate their elements, and
would leak strategy-specific knowledge into a semantic owner, which
``AGENTS.md`` forbids.

Rendering therefore stays **inside the type-erased contract**. Element access
continues through the ops table that already owns it (``IndexedValueOps``,
``SetValueOps``, ``MapValueOps``); what consolidates is the *spelling* --
brackets, separators, quoting, and the recursion between them -- not the
traversal. The ops tables keep their element accessors and lose their
``*_to_string`` functions, which become one shared walk parameterised by the
accessors the table already provides.

Where the representation differs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A named TSB and its ``CompoundScalar`` twin deliberately **share a value
schema**: ``TypeRegistry::tsb`` builds the matching named value bundle so the
nominal identity is the same. The rendering choice therefore cannot be data on
``ValueTypeMetaData`` -- one schema, two answers.

It belongs to the **endpoint**, as ``TSDataOps::to_string_impl``. A TSB
installs the dictionary spelling; a ``TS[CompoundScalar]`` falls through to the
value's own constructor-style text. The strategy is selected once from plan
metadata and dispatched inside the erased contract, which is the pattern
``AGENTS.md`` prescribes.

This is not hypothetical. Routing ``str_`` at the value rather than the
endpoint made a TSB render ``RenderPair(a=1, b='x')``, because at the value
layer the two are indistinguishable. The endpoint is the only place that knows.

Compatibility
-------------

``diagnostic`` output is unchanged by construction: the consolidation is a
refactor, and the existing assertions are the test that it is. ``str`` and
``repr`` already reached their intended spellings in #907 and #919 for every
path that is actually routed there; the paths that are *not* routed (the TSB
dictionary case) change to the documented spelling, which is the point.

Serialization is unaffected: JSON writes its own literals and never goes
through these tables, and the delta publisher owns its key text since #919.

Performance
-----------

Rendering is not on the per-tick path. ``render`` dispatches on the schema's
kind once per call rather than per element, so the consolidated form does
strictly less indirection than a chain of ops-table hops.

Alternatives considered
-----------------------

*Install the spelling at each site as it is found.* This is what the last six
attempts did. It cannot converge: there is no enumeration of the sites, and a
site that is never routed absorbs the change silently.

*A flag on the value metadata to distinguish representations.* Proposed and
rejected during #919 — a named TSB and its CompoundScalar genuinely share a
value schema, so the distinction does not belong in the value metadata. The
representation belongs to the schema that owns it.

Acceptance criteria and test plan
---------------------------------

#. ``render`` is the only definition containing a bracket or separator literal
   for a container, bundle or tuple; a ratchet in
   ``python/tests/test_architecture_ratchets.py`` pins the renderer count at
   one and may only fall.
#. Every spelling currently asserted against released hgraph 0.5.41 keeps its
   value, and a TSB renders ``{'a': 1, 'b': 'x'}``.
#. Each spelling is exercised through ``eval_node<stdlib::str_>`` and its
   Python equivalent, not by calling the formatter directly — a test must fail
   when the implementation regresses, which the current TSB test does not.
#. ``VALUE_OPS_ABI_VERSION`` is bumped with its ``static_assert`` if the ops
   table changes shape, and the whole workspace is rebuilt.

Unresolved questions
--------------------

* Whether ``diagnostic`` should survive as a distinct spelling or become
  ``repr`` once the two agree everywhere except the ~180 pinned assertions.
* Whether the window and slot delta spellings have users outside diagnostics.

Implementation status
---------------------

The collapse is implemented (#929). ``format_string_impl`` and
``repr_string_impl`` are gone from ``ValueOps``; every ``*_format_string`` twin
added in #907/#919 is folded back into its ``*_to_string``;
``TSDataOps::format_string_impl`` is renamed ``to_string_impl``;
``VALUE_OPS_ABI_VERSION`` is 9 with ``sizeof(ValueOps)`` pinned beside it.

Deleting the second spelling turned "which of the 46 formatters serves this
value?" into a list of compiler errors -- the enumeration that could not be
obtained by searching, and which caught ``format_`` (``"{}".format(x)``)
needing ``str`` semantics, a site inspection had missed.

The cost was three assertions out of 1856 C++ and 3518 Python: a tuple's string
element gains quotes, a named bundle scalar renders
``SpecializedViewBundle(count=3, name='items')``, and a TSB renders
``{'a': 1, 'b': 'x'}``. All three are the same rule in different places, and
all three agree with released hgraph 0.5.41.

The consolidation itself remains: 46 renderers still hand-write their own
brackets. That is now a refactor with one spelling to preserve rather than
three to reconcile.
