Open parity questions
=====================

Questions raised while triaging parity issues that neither the runtime
specification nor an earlier ruling decides. Each names the issue, the
observations, the options and a recommendation; the owner's answer moves it
into :doc:`parity_matrix` (accepted) or into a fix.

A native ``table_schema``: what it takes (issue #821, PR #1638)
---------------------------------------------------------------

Owner direction (2026-09-24): ``table_schema`` should be C++; find the
issues first, to choose how. A spike on ``main`` @ ``a629ad27b`` (branch
``spike/native-table-schema``) built it. Native ctest passed 2252/2252 with
every extension. The Python suite passed 3964; its one failure is the missing
HGL catalogue disposition for the new operator.

**What the spike needed, and what it proved**

1. *The operator.* A native ``hgraph::TableSchema`` bundle (``tp: type``,
   ``types: tuple[type, ...]``, the rest as released) and a const-evaluable
   compose ``table_schema(tp) -> TS[TableSchema]`` over
   ``ts_table_layout``. It is about 70 lines, and C++ graphs wire it like any
   operator.
2. *A type as a runtime value.* ``TypeCarrier`` (RFC 0033) is already a
   standard scalar with equality, hashing and text; it lacked a Python
   conversion. A hook pair (the ``WiredFn`` mechanism) sends it to Python as
   what a type argument crosses as (a ``TsType``, the scalar's Python class,
   a size) and accepts whatever a type-argument slot accepts. With it,
   ``getattr_`` of ``keys``, ``types`` and ``tp`` and whole-value reads all
   work. ``types`` holds real Python classes, closer to released hgraph than
   today's ``object`` for every non-leaf column.
3. *Binding the Python class to the native schema.* Python's ``type`` and
   ``type[T]`` deliberately map to the Python-object scalar, so a Python
   ``TableSchema`` cannot describe ``TypeCarrier`` fields through its
   annotations. The spike lets the class name its native schema
   (``__native_schema__ = "hgraph::TableSchema"``). The class keeps released
   hgraph's annotations, and the native schema decides the storage. Graph
   outputs arrive as ``TableSchema`` instances equal to ``.value``, and
   user-built values convert, including an unregistered class in ``types``.
   All 239 table and data-frame tests pass.

**Owner review (2026-09-24)**

- *Per-tick cost: withdrawn.* Measured without the test harness's output
  recording, a Python node producing a new ``TableSchema`` every tick, and a
  Python reader converting its ``types`` and ``tp``, take **no** type-system
  locks during evaluation (10 per tick, identical to a plain tuple node; all
  harness). The "+12 per tick" first reported came from ``eval_node``
  converting its recorded outputs after the run. A type is resolved once, to
  an interned schema, and only referenced from then on.
- *Text: ruled.* A type's text is its name: ``str_`` of ``int`` is ``int``,
  of a time-series type ``TS[int]``. (Today ``TypeCarrier`` renders
  ``type[int]``.)
- *Serialising a type: to and from its name.* The canonical name already
  exists; C++ has no parser back (the manifest descriptor of RFC 0022 is
  one-way: identity bytes, no decode). A core type-name parser, cold path
  and cached by name, gives every type value a wire form and a JSON form. A
  named type (a registered bundle or enum) must be registered in the
  decoding process first, as the binary codec already requires.
- *Table schema: Arrow.* A schema's target is an Arrow frame, and its
  columns come from a closed set (``HGRAPH_TABLE_ATOMIC_LEAVES`` in
  ``table_codec.cpp``: 17 leaves and lists of them), each with a fixed Arrow
  type. So the column types are Arrow types, not general type values. Arrow
  brings their serialisation (IPC) and their Python face (``pyarrow``). The
  one ambiguity is ``Str`` and ``ZoneId``, both ``utf8``. A field-metadata
  tag resolves it, and decoding a table does not need it, because the layout
  comes from ``tp``. Only ``tp`` remains a type value.

**Revised proposal**

1. ``TableSchema`` carries its target frame's **Arrow schema**: column
   names, Arrow types and field metadata. It is a native scalar, like
   ``Frame``, with Arrow IPC as its wire form and ``pyarrow.Schema`` in
   Python. ``keys`` stays a field (``getattr_`` on it is #821). ``types``
   derives from the Arrow schema; Python maps it to classes through the
   closed leaf table, so ``table_shape`` and released-style reads keep
   working. The smaller alternative is ``types`` as Arrow type names
   (strings), with no new value kind.
2. ``tp`` is a type value. Its text is its name, and its wire and JSON form
   is its name, parsed back through a core type-name parser.
3. The rest of phase 1 as before: the operator, the ``TypeCarrier`` Python
   conversion, the native-schema binding of the Python class, the eager
   fallback outside a graph, and the catalogue entry.

Still for the owner: an Arrow-schema value (1) or Arrow type names; and
binding the Python class by name or a public "hgraph type" annotation.
