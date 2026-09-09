API Surface Triage
==================

The ``surface`` audit (:doc:`parity_testing`) compares the public Python API of
this repository's ``hgraph`` against the maintained Python-first ``hgraph``
0.5.41 reference: exported names, callable signatures, class methods, and module
importability.  Every difference is either recorded in
``tools/parity/surface_known.json`` with a reason, or it is *actionable*.

The actionable list had accumulated without ever being triaged.  It is a
productive reservoir — PR #795 came out of it (``use_wall_clock`` dropped from
the stream operators), and the ``SCHEDULER.schedule`` signature had been flagged
there before the runtime callback refresh restored it.  This page records the
first full pass over that list, and keeps the remaining backlog.

Triage of 2026-09-09
--------------------

Commit triaged: ``081e03431`` (main).  83 actionable findings, classified into
three buckets:

.. list-table::
   :header-rows: 1
   :widths: 10 8 82

   * - Bucket
     - Count
     - Meaning
   * - A — accepted deviation
     - 66
     - A deliberate difference.  Recorded in ``surface_known.json`` with a
       reason naming the design or ruling it follows.
   * - B — real gap
     - 11
     - The candidate is missing something a user could reasonably call, or a
       signature diverges in a way that breaks ported code.  Listed below; not
       fixed by the triage.
   * - C — audit artefact
     - 6
     - Not a real difference: the probe could not see the candidate's
       parameters.  Fixed in the probe, so these findings no longer exist.

Bucket A splits into six families.

*Generic type variables* (9).  ``OUT``, ``V``, ``TS_SCHEMA``, ``SCALAR_1``,
``SCALAR_2``, ``KEYABLE_SCALAR``, ``ENUM``, ``WINDOW_SIZE`` and
``WINDOW_SIZE_MIN`` are ``_TypeVarSentinel`` values rather than
``typing.TypeVar``, joining ``NUMBER``/``NUMBER_2``/``TABLE`` which were already
accepted.  The sentinel carries the scalar / time-series split upstream spells
with ``HgScalarTypeVar``/``HgTimeSeriesTypeVar``; both spellings annotate and
resolve identically, verified by wiring and evaluating a ``TS[SCALAR_1]`` node.

*Markers and type constructors* (6 kind mismatches, 3 constructors).
``AUTO_RESOLVE``, ``RECORDABLE_STATE``, ``SIGNAL``, ``TS_OUT``, ``Array`` and
``Series`` are subscripted, not constructed, so their runtime class differs from
upstream's; ``TSW``, ``DEFAULT`` and the nanobind ``JSON`` report a constructor
that no user calls.

*Annotation-class runtime surface* (17 + 3).  ``REF`` and ``TSW`` join the
family whose upstream runtime ABC surface is deliberately not replicated
(ruling 2026-08-01) — instance behaviour lives on the runtime views.
``KeyValue`` joins ``TryExceptResult`` for the three ``TimeSeriesSchema``
scalar-schema conversion helpers.

*The LOGGER facade* (13 + its constructor).  The handler, filter and
``LogRecord`` surface of ``logging.Logger`` is absent because the injected
``LOGGER`` is a facade over the executor-owned run logger; that logger is
CONFIGURED through ``GraphConfiguration``, not from node code, which is why
``setLevel`` in particular stays absent -- a node reconfiguring the run it is
part of is not something to enable.  Reading the level is a different matter,
and ``isEnabledFor`` / ``getEffectiveLevel`` were added on issue #810 item 3.4:
guarding an expensive message is ordinary node code, not configuration.

*An injected GlobalState* (10).  ``write_frame`` on the four data-frame storage
classes in two modules, and ``get_table_schema_date_key`` /
``get_table_schema_as_of_key``, take an optional trailing ``global_state``.  The
upstream call shape is unchanged; this matches the already-accepted
``*DataFrameStorage.instance`` rule.

Both of those data-frame rules pin the exact ``reference`` and ``candidate``
signatures rather than matching on module, name and kind alone.  A rule that
constrains only the name accepts *any* future difference on that method: were
``as_of`` to be dropped upstream, or ``global_state`` to become required here,
the finding would still be classified as known and the audit would go quiet on
a real regression.  Pin the payload whenever a rule waives a signature.

*Operator markers* (4).  ``collect``, ``convert`` and ``emit`` are
subscriptable marker objects, so introspection shows the marker's ``__call__``
rather than upstream's declared operator signature; each accepts the upstream
call unchanged.  ``dispatch_`` adds the documented keyword-only
``__output_type``.

Bucket B
--------

This is the actionable backlog, in the order a user is most likely to hit it.

.. list-table::
   :header-rows: 1
   :widths: 22 26 26 26

   * - Symbol
     - Upstream offers
     - Candidate offers
     - How a user hits it
   * - ``RecordReplayContext.__init__``
     - ``(mode=RecordReplayEnum.RECORD, recordable_id=None)``
     - ``(mode=None, recordable_id='')``, and ``mode=None`` becomes
       ``MODE_NONE``
     - ``with RecordReplayContext():`` records upstream and does nothing here.
       Silent: no error, just no recording.  ``recordable_id=''`` also differs
       from upstream's ``None``, which means "inherit the parent recordable
       id".
   * - ``RecordReplayContext.instance``
     - ``instance()`` static returning the active context, plus ``mode`` and
       ``recordable_id`` properties
     - nothing; the class carries ``_mode``/``_id`` privately
     - Graph code that branches on ``RecordReplayContext.instance().mode``
       raises ``AttributeError`` at wiring time.
   * - ``DebugContext.instance``
     - ``instance()`` static returning the active context or ``None``
     - nothing; the stack is the private ``DebugContext._stack``
     - The upstream guard ``if DebugContext.instance() is not None:`` around
       expensive debug wiring raises ``AttributeError``.
   * - ``DebugContext.print``
     - ``(label, ts, print_delta=True, sample=-1)``
     - ``(label, ts, **kwargs)`` forwarding to ``debug_print``
     - Keyword calls work; ``DebugContext.print(label, ts, False)`` raises
       ``TypeError``.  The parameters are also invisible to help() and IDEs.
   * - ``with_columns`` (``hgraph.adaptors.data_frame`` and
       ``...._data_frame_operators``) -- **fixed, issue #817**
     - ``(ts, **columns)``
     - was ``(ts, _tp_out=DEFAULT[ROW_1], **columns)``; now
       ``(ts, **columns) -> DEFAULT[ROW_1]``
     - An internal resolver parameter sat in the public signature between
       ``ts`` and the columns, where it showed in ``help()`` and every
       generated signature, and a second positional argument bound to it
       instead of raising.  The DEFAULT variable now rides the return
       annotation, as the released signature spells it and as the identical
       ``to_json``/``from_json`` leak was fixed.  The overload's carrier is
       keyword-only, so the public signature and the overload agree: removing
       the parameter from the signature alone left the positional binding
       intact, because the signature is not what binds the call.

       One consequence in the original finding is **not** fixed and is not
       caused by the signature: a column legitimately named ``_tp_out`` is
       still rejected, because the only overload accepting ``**columns`` is
       the Python adapter and that adapter claims the name.  Mirroring
       upstream needs a second overload with a mutually exclusive ``requires``
       predicate; a prototype without one silently returned the unprojected
       frame through a port declared for the projected schema, so it is
       tracked separately rather than rushed.
   * - ``LOGGER.isEnabledFor`` -- **added, issue #810 item 3.4**
     - ``isEnabledFor(level)``
     - ``isEnabledFor(level)``
     - The standard guard ``if logger.isEnabledFor(logging.DEBUG):`` around an
       expensive message raised ``AttributeError`` inside a node.  Answered
       against the run logger's own threshold.
   * - ``LOGGER.getEffectiveLevel`` -- **added, issue #810 item 3.4**
     - ``getEffectiveLevel()``
     - ``getEffectiveLevel()``
     - Reports on the standard Python scale, so the result is comparable with
       ``logging.DEBUG`` and friends.  spdlog's ``off`` reports 60, above every
       standard level, which is what "nothing is enabled" means here.
   * - ``LOGGER.warn`` -- **accepted, issue #810 item 3.4**
     - ``warn(msg, *args, **kwargs)`` (deprecated alias of ``warning``)
     - nothing
     - Deprecated in Python's own logging.  ``warning`` is the spelling to
       carry forward, so the alias is not reproduced.
   * - ``LOGGER.fatal`` -- **accepted, issue #810 item 3.4**
     - ``fatal(msg, *args, **kwargs)`` (alias of ``critical``)
     - nothing
     - As ``warn``: ``critical`` is the spelling to carry forward.
   * - ``LOGGER.exception``
     - ``(self, msg, *args, exc_info=True, **kwargs)``
     - ``(self, msg, *args, **kwargs)``; the native emitter always attaches the
       active exception
     - ``logger.exception(msg, exc_info=False)`` is not rejected — the keyword
       falls into the interpolation kwargs and the exception is attached
       anyway.

The five ``LOGGER`` rows are one change: the emission-only facade in
``python/py_state_services.cpp`` is missing the two emission aliases, the level
query pair, and ``exception``'s ``exc_info``.  Everything else on
``logging.Logger`` — handlers, filters, ``setLevel``, ``LogRecord`` plumbing —
is bucket A, because the injected logger is the executor's, configured through
``GraphConfiguration``.

Probe change
------------

The bucket-C findings were all the same artefact: ``inspect.signature`` cannot
introspect a nanobind method, so the probe recorded "no signature" for the
candidate and every reference signature became an unresolvable mismatch.
nanobind and pybind11 both write ``name(params) -> return`` on the first line
of ``__doc__``; ``_documented_signature`` in ``tools/parity/surface.py`` now
parses that when ``inspect.signature`` fails.  An overloaded native declares
several such lines and stays unavailable, because one parameter list cannot
represent them.

The effect is a strict improvement: 16 findings disappeared and none appeared.
Six were actionable (``LOGGER.debug``/``info``/``warning``/``error``/
``critical``/``log``, all exactly upstream's ``(self, msg, *args, **kwargs)``).
Ten were previously accepted on the strength of a behavioural test rather than
a compared signature — ``SCHEDULER.has_tag``/``pop_tag``/``reset``/
``un_schedule``, ``Traits.get_trait``/``get_trait_or``,
``Node.notify_next_cycle``, ``NODE.notify_next_cycle``,
``TimeSeries.is_reference``, ``EvaluationEngineApi.request_engine_stop`` — and
are now proven identical.  Their rules are left in ``surface_known.json``: a
native declaration is rendered by the binding generator, so a platform whose
rendering differs should stay accepted rather than turn red.

Working the backlog
-------------------

When a bucket B item is fixed, remove nothing from ``surface_known.json`` — the
finding simply stops being produced.  When a new actionable finding appears,
it belongs in one of these three buckets before it is left alone: a rule with a
reason, a row in the table above, or a probe fix.  Re-run with:

.. code-block:: bash

   .venv/bin/python -m tools.parity surface --output-dir <dir> --exit-zero
