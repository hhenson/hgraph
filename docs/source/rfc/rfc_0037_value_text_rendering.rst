RFC 0037: One Authority For Rendering A Value As Text
=====================================================

:Status: Draft
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

Rendering a value as text is implemented **forty times** across ten files.
Each site hand-writes its own brackets, separators and element recursion, and
which one runs for a given value is decided by runtime routing that is not
visible from the call site. This RFC replaces them with a single renderer that
takes a schema, a memory pointer and a *spelling*, and makes every ops table
delegate to it.

Motivation
----------

This is the **parallel abstractions** guardrail in ``CLAUDE.md`` — "one runtime
model, no generic fallback … two ways to do one thing is the smell to kill, not
add to" — at a count of forty.

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

118 hand-written bracket and separator literals sit inside them.

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

One function::

    std::string render(const ValueTypeMetaData &schema,
                       const void *memory,
                       Spelling spelling);

walks the structure once and is the only place brackets, separators, quoting
and element recursion are written. ``ValueOps`` keeps ``to_string``,
``format_string`` and ``repr_string`` as the public surface, but each becomes
a thin call into ``render`` with the corresponding spelling. Container and
TSData ops tables stop carrying renderers at all.

Where a representation genuinely differs — ``TS[CompoundScalar]`` renders
constructor-style, a ``TSB`` renders as a dictionary — that is **data on the
schema**, not a function pointer installed in one of forty tables. The choice
is made once, by the schema, and is visible to a reader of the schema.

Ownership boundary
------------------

The value layer owns text rendering. The time-series layer supplies the value
and its schema and nothing else; ``TSDataOps::format_string_impl`` is removed.
The std-operator layer (``str_``, ``print_``, ``debug_print``) selects a
spelling and never formats.

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

Draft. No implementation yet.
