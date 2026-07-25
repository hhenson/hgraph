RFC 0004: Python-Owned Structured Scalars
=========================================

:Status: Accepted
:Author: Howard Henson
:Created: 2026-07-23
:Target: Next ``hg_cpp`` minor release

Summary
-------

Add a Python-backed storage policy for ``CompoundScalar`` semantics. A Python
dataclass used as a scalar type remains an ordinary Python object throughout
graph execution while having a nominal hgraph Bundle schema. It supports the
same storage-independent reflection, generic resolution, casting, dispatch,
equality, hashing, and scalar graph operations as a native
``CompoundScalar``. Its runtime binding retains the original Python object and
the active concrete schema; it does not materialise the object into
field-by-field C++ storage. It exposes its declared fields through the same
type-erased ``BundleView`` used by native Bundle storage.

``CompoundScalar`` remains the native-layout structured scalar. This RFC adds
a Python-backed Bundle representation for application models whose
constructors, descriptors, inheritance, equality, or deliberately permissive
field values rely on Python behaviour.

Motivation
----------

The original Python hgraph treated a ``CompoundScalar`` value as a Python
object. Existing applications therefore rely on behaviours that a native
field-by-field representation cannot preserve without either rejecting the
value or silently changing it. Examples include:

* a dataclass annotation being descriptive rather than a runtime validator;
* custom ``__init__``, ``__post_init__``, property, descriptor, ``__eq__``,
  and ``__hash__`` implementations;
* Python subclasses and additional non-schema attributes;
* fields containing values accepted by the application even when their exact
  runtime class is narrower or wider than the annotation; and
* object identity being retained while Python code holds or forwards a value.

The current arbitrary-object fallback preserves a Python object but maps every
class to the same native ``object`` schema. It cannot provide reliable nominal
dispatch, field reflection, or wiring-time output resolution for
``getattr_``. The current canonical ``Bundle`` binding provides those features
but eagerly converts every declared field into native storage, which is the
source of the compatibility problem.

The required model therefore separates logical schema and view from physical
layout. Both representations are Bundles. The native binding projects fields
through offsets in structured storage; the Python binding projects fields
through Python attribute access. ``BundleView`` already exists to erase that
representation difference, so a second structured value kind would duplicate
the concept and weaken generic Bundle operators.

Goals
-----

* Preserve the exact Python instance, including its concrete runtime class and
  Python-defined behaviour.
* Make a Python-backed dataclass satisfy the storage-independent
  ``CompoundScalar`` behavioural contract, including parameterised classes,
  TypeVar resolution, Bundle conversion, nominal dispatch, equality, and
  hashing.
* Extract an ordered, typed schema suitable for hgraph wiring, reflection, and
  operator resolution.
* Present the value as a normal read-only ``BundleView`` to generic C++ code.
* Provide the structured scalar conveniences users expect, especially
  ``getattr_`` and construction from field time series.
* Avoid eager, recursive field validation or conversion when the whole object
  enters a time series.
* Keep native ``CompoundScalar`` semantics, storage, and C++ access unchanged.
* Make the ownership and performance boundary visible: C++ can access fields
  through type erasure but cannot assume native field addresses or layouts.

Non-goals
---------

This RFC does not:

* make a Python-owned object usable in a Python-free process;
* expose ``PyObject`` or bridge-internal storage in the public C++ SDK;
* expose Python-backed fields as direct native memory addresses;
* provide a mutable ``BundleView`` over the held Python object;
* infer an Arrow layout merely from the Python storage strategy;
* observe in-place mutation as an hgraph tick;
* perform general runtime validation from Python type annotations; or
* change the representation of an existing ``CompoundScalar``.

Terminology and ownership
-------------------------

**Python-owned structured scalar**
   The Python-facing feature proposed by this RFC. Its declared class and
   fields participate in hgraph's type system, while its value remains one
   Python object.

**Python CompoundScalar**
   A concise name for a Python-owned structured scalar when discussing the
   common behavioural contract. It is a normal nominal Bundle with
   ``CompoundScalar`` semantics and Python-backed storage; it is not an
   instance of the native-layout ``CompoundScalar`` base class.

**Python-backed Bundle binding**
   A ``ValueTypeKind::Bundle`` binding whose storage owns a Python object plus
   its active concrete schema and whose ``IndexedValueOps`` project declared
   fields by Python attribute access. It implements the same read-only
   ``BundleView`` contract as a field-expanded native binding without sharing
   its layout.

Bundle schema metadata, ``BundleView``, and the representation-neutral indexed
operations belong to the hgraph core. The class registry, ``PyObject``
lifetime, schema extraction, Python projection operations, and construction
policy belong to the optional Python bridge. Pure C++ builds contain no Python
class registrations and retain no dependency on the Python runtime.

Python contract
---------------

Dataclasses
~~~~~~~~~~~

A concrete standard-library dataclass that does not derive from
``CompoundScalar`` is recognised lazily when used in an hgraph scalar
annotation:

.. code-block:: python

   from dataclasses import dataclass
   from hgraph import TS, graph

   @dataclass(frozen=True)
   class Quote:
       instrument: str
       bid: float
       ask: float

       @property
       def mid(self) -> float:
           return (self.bid + self.ask) / 2.0

   @graph
   def spread(quote: TS[Quote]) -> TS[float]:
       return quote.ask - quote.bid

``TS[Quote]`` is a distinct nominal hgraph type. Values read by Python nodes
are the original ``Quote`` instances; hgraph does not replace them with a
dictionary, namespace, reconstructed dataclass, or proxy.

``CompoundScalar`` detection takes precedence over dataclass detection. A
dataclass deriving from ``CompoundScalar`` therefore keeps the existing native
``Bundle`` representation.

Explicit registration
~~~~~~~~~~~~~~~~~~~~~

Non-dataclass application classes are opt-in:

.. code-block:: python

   from hgraph import register_python_object_type

   class LegacyQuote:
       instrument: str
       bid: float
       ask: float

   register_python_object_type(LegacyQuote)

``register_python_object_type`` accepts a class and, optionally, an ordered
``fields`` mapping for classes whose public attributes are not completely
described by resolved annotations. It returns the class, so it may also be
used as a decorator. Registration is process-wide and idempotent for the same
class and field schema. Registering the same class or qualified schema identity
with a conflicting field schema fails deterministically.

An arbitrary, unregistered, non-dataclass class retains the existing
``TS[object]``-style fallback. Merely adding ``__annotations__`` to a class
does not opt it into structured semantics.

Schema extraction
~~~~~~~~~~~~~~~~~

For a dataclass, field order and membership come from ``dataclasses.fields``.
This includes inherited and ``init=False`` data fields and excludes
``ClassVar`` and ``InitVar`` pseudo-fields. Resolved type hints provide each
field's logical hgraph scalar schema. Dataclass defaults, default factories,
keyword-only status, and constructor participation are retained as Python
construction metadata but do not change the field's scalar type.

The class is the nominal identity. Two classes with identical fields remain
different hgraph types. The diagnostic name is based on ``module`` and
``qualname``; the registry uses the retained class identity rather than the
rendered name as the Python-side key.

Direct and mutually recursive references are permitted because they are
descriptive metadata and do not imply recursive native storage. Registration
uses the same forward-declaration and resolution mechanism as
``CompoundScalar``. Automatic recognition of PEP 681 dataclass-like frameworks
is deferred; such classes may use explicit registration once an unambiguous
resolved field mapping is available.

Parameterised classes and TypeVar resolution
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Parameterised Python classes use the existing ``CompoundScalar`` generic model:

.. code-block:: python

   from dataclasses import dataclass
   from typing import Generic, TypeVar

   T = TypeVar("T")

   @dataclass(frozen=True)
   class Box(Generic[T]):
       value: T

   @dataclass(frozen=True)
   class IntegerBox(Box[int]):
       pass

``Box[int]`` and ``Box[str]`` resolve to distinct, invariant nominal Bundle
schemas. Resolution substitutes TypeVars through fields, inherited bases,
nested structured types, and container arguments. The resolved schema records
the origin class and concrete generic arguments, and presents the same
qualified diagnostic name and reflection surface as the equivalent
``CompoundScalar`` specialisation. ``IntegerBox`` records ``Box[int]`` as its
resolved parent.

An unresolved argument creates the same Bundle generic pattern used for a
native ``CompoundScalar`` and participates in the normal wiring resolution
scope. Consequently a signature such as ``TS[Box[T]] -> TS[T]`` binds ``T``
from ``TS[Box[int]]`` without a Python-specific resolver. Generic arguments are
invariant unless the hgraph type system explicitly introduces variance.

Python normally erases generic arguments from ``type(value)``. The Python-backed
binding therefore retains the resolved specialisation as the value's active
schema; it must not attempt to recover ``Box[int]`` versus ``Box[str]`` from
the runtime class during dispatch. At a target-typed boundary the target
specialisation supplies that identity. At a schema-free boundary inference
uses, in order:

* an exact registered concrete subclass such as ``IntegerBox``;
* ``__orig_class__`` when Python has retained an applicable parameterised
  alias; and
* the existing ``CompoundScalar`` field-schema matching algorithm across
  registered specialisations when the alias is absent, as it commonly is for
  frozen dataclasses.

The last step is inference, not eager validation: it is used only when the
target schema is unknown. A tie is an explicit ambiguity error rather than a
registry-order choice. Once selected, the active schema is retained with the
object through copies, up-casts, switches, and time-series storage.

Reflection provides:

.. code-block:: python

   scalar_type(TS[Quote]) is Quote
   fields(Quote) == {
       "instrument": str,
       "bid": float,
       "ask": float,
   }
   fields(TS[Quote]) == fields(Quote)

The Python class is retained strongly for as long as its schema registration is
live. Test-only registry reset clears the association alongside the schema and
storage-plan registries.

Runtime type checking
~~~~~~~~~~~~~~~~~~~~~

A value entering ``TS[Quote]`` must be an instance of ``Quote``; a Python
subclass is accepted and remains that subclass. The bridge does not walk the
object's fields, compare their runtime types with annotations, copy them, or
normalise them when the whole object is assigned.

The field annotation is a wiring contract, not an eager object validator.
Conversion to a native field type happens only when an operator actually
extracts that attribute into its typed output. An incompatible extracted value
then fails at that boundary with the field name, declared type, actual type,
and owning class in the diagnostic.

``None`` retains hgraph's existing scalar meaning: it does not form a valid
value tick. For attribute access, a missing attribute or ``None`` produces no
tick unless the default-valued ``getattr_`` form is used.

CompoundScalar compatibility contract
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A Python CompoundScalar must pass the existing storage-independent
``CompoundScalar`` behaviour suite. This includes nominal and generic schema
identity, TypeVar resolution, inheritance and recursive schemas, reflection,
auto-constant conversion, attribute projection, Bundle composition and
conversion, overload dispatch, comparison, and hashability. The suite should
be parameterised over native and Python-backed storage wherever the behaviour
does not require direct native field addresses.

The intentional differences are limited to representation-dependent
capabilities: a Python CompoundScalar preserves the exact Python object,
projects attributes under the GIL, has no native field offsets or mutable
Bundle view, and delegates whole-value equality and hashing to Python. These
differences must not leak into generic schema matching or operator selection.

C++ schema contract
-------------------

The Python class registers as an ordinary nominal
``ValueTypeKind::Bundle``. Its ordered ``ValueFieldMetaData`` is the logical
field contract, and its qualified class name provides nominal identity. No new
value kind or opaque-record capability is introduced.

Schema and representation remain separate. A schema may be paired with a
binding whose storage plan is not the factory-derived named-tuple layout,
provided that binding supplies the erased operations required by the schema's
views. The core registration facility is conceptually:

.. code-block:: cpp

   register_bundle_binding(
       nominal_bundle_schema,
       python_object_storage_plan,
       python_indexed_value_ops);

The general core change is the ability to register a non-layout-derived
canonical binding for a Bundle schema. It is not Python-specific. Registration
verifies that the binding exposes a complete read-only ``IndexedValueOps``
surface matching the schema field count and field schemas. The Python bridge's
storage plan and projection operations remain private.

``BundleView`` is the semantic interface, not a promise of native named-tuple
storage. A Python-backed binding therefore supports:

.. code-block:: cpp

   BundleView bundle = value.as_bundle();
   ValueView bid = bundle.at("bid");

The indexed operations project each field through a field-specific erased
binding. That projection interprets the parent Python-object storage, performs
ordinary Python attribute lookup under the GIL, and materialises or converts
the field only when an operation consumes it. The field view remains governed
by its declared field schema. The erased assignment/materialisation protocol
must support copying such a projected view into canonical native storage
without requiring a direct pointer to an ``Int``, ``Float``, or other child
inside the parent object.

This may require extending the indexed/value ops contract beyond its current
direct-child-pointer fast path. Native composite bindings retain that fast
path. The Python binding must not create a parallel Python-only Bundle view or
teach generic operators to branch on storage policy.

A C++ consumer can inspect, iterate, copy, compare, serialise, and project
fields through ``BundleView`` and the normal erased operations. It cannot call
``checked_as<PythonClass>()``, obtain native field offsets, or assume that
``bundle.at(i).data()`` points at canonical child storage. A
``MutableBundleView`` is not provided for the held Python object.

This facility is separate from :doc:`rfc_0003_extension_scalar_registration`.
RFC 0003 associates a Python facade with extension-owned native scalar storage.
This RFC does the inverse: the Python class owns the value semantics and the
core sees a normal nominal Bundle schema through a Python-backed erased
binding.

Runtime representation
----------------------

The canonical value plan is a bridge-owned Python object handle plus an active
concrete Bundle schema tag. Assignment retains the exact object with a strong
reference; copy and destruction preserve the tag and adjust Python reference
counts under the GIL; conversion back to Python returns the same object.
Nested attributes remain part of that object and create no independent hgraph
storage until extracted. The schema tag is type-system state, not a
field-expanded copy or external application discriminator.

When a target schema is known, ``isinstance`` against its registered origin
class is authoritative and the resolved target specialisation becomes the
active schema. No fields are read for validation. At a schema-free boundary,
the bridge selects the most specific registered class using Python
``isinstance`` and MRO semantics, then applies the generic inference rules
above when that class has more than one registered specialisation. The realised
binding exposes the active schema through the existing erased concrete-type
operation so native Bundle hierarchy, casting, and dispatch machinery can use
it without inspecting ``PyObject``.

Equality and hashing
~~~~~~~~~~~~~~~~~~~~

Whole-value equality follows the held object's Python ``==`` and propagates
Python exceptions. It does not substitute structural field equality: a class
may deliberately include hidden state, use subclass-sensitive equality, or
define identity semantics even though its declared fields remain projectable
through ``BundleView``. ``eq_``, ``ne_``, and deduplication must therefore use
the Python-backed binding's erased value operations.

Hash support is advertised only when the registered class has an effective
``__hash__`` implementation. Hashing follows Python ``hash`` and must not
silently fall back to a pointer or object-identity hash when Python declares
the class unhashable. An unhashable Python CompoundScalar is rejected where
``TSS`` or ``TSD`` key semantics require hashing. Ordering is not advertised
merely because Python could attempt ``<``. These rules preserve Python's
equality/hash contract, including equal distinct frozen dataclass instances and
custom ``__eq__``/``__hash__`` implementations.

Operator semantics
------------------

Attribute access
~~~~~~~~~~~~~~~~

``getattr_(ts, attr)`` and ``ts.attr`` use the existing Bundle overload.
``attr`` remains a wiring-time string, the output is resolved from the Bundle
field schema, and evaluation copies the projected field view into the output.
The binding's erased indexed operations determine whether that projection uses
a native field offset or Python attribute access. The operator contains no
storage-policy branch.

Properties and other descriptors use Python lookup. A descriptor not listed in
the Bundle schema requires an explicit output specialisation, for example:

.. code-block:: python

   getattr_[SCALAR: float](quote, "mid")

An unknown attribute without an explicit output type is a wiring error.
Attribute lookup exceptions are translated with the class and attribute name.
The default-valued form uses the default when lookup reports absence or
returns ``None``; it does not swallow other descriptor exceptions.

Construction
~~~~~~~~~~~~

``combine[TS[Quote]](...)`` uses the ordinary Bundle composition path. Wiring
checks field names and input time-series types. The target Bundle binding
receives the composed field views through the erased assignment protocol and
calls the registered Python construction policy with keyword arguments. It
publishes the resulting object unchanged.

For dataclasses, omitted ``init=True`` fields must have a default or default
factory; required constructor fields must be wired. ``init=False`` fields are
not constructor arguments. Python's constructor, default factory, and
``__post_init__`` behaviour remain authoritative. Supplied field inputs must
be valid before the first construction; their last values are reused when
another supplied field ticks.

The initial implementation does not define a generic field-update or
``setattr_`` operation. A future operator may use ``dataclasses.replace`` or a
registered class-specific factory, but it must return a new emitted object and
must not mutate the held value in place.

Bundle conversion and casting
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The existing ``CompoundScalar`` conversion surface applies unchanged:

* ``TS[PythonClass]`` converts to ``TSB[PythonClass]`` by projecting its
  declared fields through ``BundleView``;
* a ``TSB[PythonClass]`` or compatible structural Bundle converts to
  ``TS[PythonClass]`` through the registered Python construction policy;
* strict and non-strict ``combine`` behaviour, omitted defaults, ``None``
  handling, ``as_scalar_ts``, and reference conversion follow the existing
  Bundle conversion rules; and
* the ``COMPOUND_SCALAR``/Bundle generic patterns match both native and
  Python-backed representations.

The target specialisation determines the Bundle shape. For example,
``TS[Box[int]]`` casts to a Bundle whose ``value`` field is ``TS[int]``, while
``TS[Box[str]]`` produces ``TS[str]``. Round-tripping reconstructs the
registered Python class with the same resolved specialisation tag. Converting
an already-held Python object to a target-typed ``TS[Box[int]]`` preserves that
object and does not reconstruct it; only conversion from field values invokes
the construction policy.

Storage policy is not part of structural compatibility. Generic conversion
operators work through ``BundleView`` and the erased construction/materialise
operations, so they do not branch on native versus Python-backed storage.
Nominal assignment and down-casting still follow the declared Bundle hierarchy
and active concrete schema.

Overload dispatch
~~~~~~~~~~~~~~~~~

Python CompoundScalars use the same native Bundle dispatch path as
``CompoundScalar``. They must not fall back to the generic Python-object
``type_``/switch implementation. The binding reports its active concrete
schema, and ``dispatch_cases`` uses the graph's closed Bundle hierarchy snapshot
to select the most specific applicable overload.

Class matching follows Python ``isinstance`` semantics represented by the
registered Bundle ancestry. A child value dispatches to a child overload in
preference to a base overload; a base fallback remains applicable; explicit
``dispatch_on``, output specialisation, union overloads, switching, down-cast,
and reference down-cast behave as for native ``CompoundScalar``. Ambiguous
multiple inheritance produces the same diagnostic as the existing
``CompoundScalar`` dispatcher.

Generic dispatch uses the retained specialisation, not ``type(value)``.
Consequently overloads for ``Box[int]`` and ``Box[str]`` remain distinguishable
even though both values have runtime class ``Box``. Up-casting to a base port
must preserve the active child/specialisation tag, so later dispatch can still
recover the most-specific valid overload. ``type_`` may still return the
object's actual Python runtime class; it is not the dispatch discriminator for
this feature.

Other scalar operations
~~~~~~~~~~~~~~~~~~~~~~~

Pass-through, merge, switch, sampling, ``type_``, equality, string conversion,
and explicit deduplication use the ordinary scalar and Bundle framework.
Generic Bundle operators consume ``BundleView`` and therefore apply to both
storage strategies. An operator that requires a concrete native child layout
must state that narrower requirement explicitly; testing
``ValueTypeKind::Bundle`` alone never establishes a layout.

Mutation and time-series semantics
----------------------------------

Frozen dataclasses are strongly recommended but not required. Hgraph stores a
reference, not a defensive copy. Mutating an object after emission:

* does not schedule a graph evaluation or create a tick;
* may change what later Python reads observe through the retained reference;
* may invalidate equality, deduplication, or container-key assumptions; and
* may make previously recorded values non-reproducible.

An application requiring value semantics must emit a new object. The
documentation and diagnostics should call this **snapshot-by-emission**
semantics. Hgraph does not monitor ``__setattr__`` or deep-copy arbitrary
object graphs.

Inheritance and assignability
-----------------------------

The declared Bundle schema is nominal. A runtime subclass instance is valid
for a base-class target and remains a subclass object. A port declared as
``TS[Child]`` may bind to ``TS[Base]`` through the normal Bundle up-cast while
retaining the same Python object handle. Down-casts require the existing
explicit dispatch/cast mechanisms and an ``isinstance`` check.

Python-backed Bundle ancestry participates in the same wiring-time hierarchy
snapshot as native Bundles, so later class definitions do not change an
existing graph's overload set. Its realised binding remains Python-backed:
storage size is unaffected by descendants, and the binding retains the active
concrete schema selected at the typed or inference boundary. The explicit tag
is necessary because Python runtime classes do not in general retain generic
arguments. It is internal erased-binding state rather than an application
field or a separately emitted discriminator.

Serialization and persistence
-----------------------------

Schema-directed Bundle codecs operate through ``BundleView`` and therefore may
serialise the declared fields of either storage representation. Decoding or
replay targets the Python-backed binding, which reconstructs the Python object
through its registered construction policy. The same field schemas,
discriminator rules, and codec compatibility checks apply.

Only declared Bundle fields participate. Hidden attributes and other Python
object state are not part of the representation. A class whose constructor
cannot recreate the declared schema must register an explicit construction
factory or is rejected by reconstruction-dependent codecs. Field-wise Arrow or
table support still requires an operator-defined layout; being Python-backed
does not itself imply one. Pickle is not an implicit interchange format.

Compatibility and migration
---------------------------

Existing native ``CompoundScalar`` classes are unchanged. Code that requires
direct field addresses, GIL-free field access, native construction, or
Python-free execution should continue to use ``CompoundScalar``. Generic C++
code written against ``BundleView`` supports both representations.

A legacy model that requires Python object semantics migrates by removing the
``CompoundScalar`` base while retaining its dataclass definition:

.. code-block:: python

   # Native, field-addressable C++ value
   @dataclass(frozen=True)
   class NativeQuote(CompoundScalar):
       bid: float
       ask: float

   # Python-owned value with a reflected logical schema
   @dataclass
   class LegacyQuote:
       bid: float
       ask: float

Both support typed Python wiring and attribute access, but their storage and
C++ field-view surface is intentionally the same. Their layout, mutation,
construction, GIL, and Python-free capabilities are different.

Dataclasses currently falling through to the shared ``object`` schema gain a
distinct nominal schema. Python source behaviour is compatible, but code that
compares raw native schema handles or intentionally relies on all application
classes sharing ``TS[object]`` must use an explicit ``object`` annotation.

The proposal deliberately does not put a mutable ``cpp_native`` policy on one
class. Representation is part of a time-series type's cross-language contract;
using separate Python-owned and ``CompoundScalar`` types keeps schema identity,
operator selection, persistence capability, and installed-extension behaviour
unambiguous.

Performance and memory
----------------------

The time-series slot contains one owning handle and one active-schema tag rather
than storage for every declared field. Whole-object flow avoids native
decomposition and later dataclass reconstruction. It still incurs Python
reference counting, and copy/destruction, equality, hashing, attribute access,
construction, and conversion require the GIL.

Each extracted attribute incurs Python lookup plus conversion to its output
schema. A graph that repeatedly performs field-level work in C++ should use a
native ``CompoundScalar`` instead. Benchmarks in the implementation PR must
compare:

* whole-object pass-through for Python-owned and native structured scalars;
* one and several ``getattr_`` projections per tick;
* construction from field time series; and
* equality/deduplication for representative frozen dataclasses.

No performance claim should compare the two forms without identifying whether
the workload is whole-object Python processing or field-level native
processing.

Alternatives considered
-----------------------

Use the shared ``object``/``Any`` schema
   This already preserves the object, but loses nominal schema identity and
   field types at the C++ wiring boundary. It cannot reliably resolve typed
   ``getattr_`` or native overloads.

Represent the value as a ``Bundle`` with Python-object storage
   Chosen. A Bundle promises a named structured view, not one physical layout.
   ``BundleView`` and ``IndexedValueOps`` are the type-erasure boundary that
   allows native offsets and Python attribute projections to satisfy the same
   contract.

Add an atomic ``OpaqueRecord`` schema
   Rejected because it duplicates the Bundle field schema, prevents generic
   Bundle operators from applying, and encodes a storage decision as a
   semantic value category.

Restore opaque storage for every ``CompoundScalar``
   This would weaken the established C++-first contract and make existing C++
   consumers representation-dependent. A distinct type category makes the
   trade-off explicit.

Add a new ``ValueTypeKind``
   A dedicated kind is more visibly distinct but requires changes to every
   exhaustive value-kind switch while still needing the same named-field view.
   The existing Bundle kind already describes the semantics; the binding
   supplies the representation.

Validate all annotated fields on assignment
   Rejected because it recreates the compatibility failure this RFC addresses
   and contradicts standard dataclass semantics, where annotations are not
   runtime validators. Validation belongs in the application's constructor,
   ``__post_init__``, descriptor, or an explicit validation node.

Deep-copy on emission
   Rejected as a default because arbitrary objects may be non-copyable, may own
   external resources, and may define identity-sensitive behaviour. It also
   hides unbounded cost in a scalar assignment.

Acceptance criteria and test plan
---------------------------------

Core metadata and storage
~~~~~~~~~~~~~~~~~~~~~~~~~

* A Python-owned class has normal nominal Bundle metadata with ordered fields.
* Its canonical binding owns the Python object and exposes complete read-only
  ``IndexedValueOps``; ``ValueView::as_bundle()`` succeeds.
* The binding retains and reports the active concrete/specialised schema
  independently of the erased Python runtime class.
* ``BundleView`` field access works by index and name without assuming native
  child offsets.
* Projected field views retain the declared field schema and can be copied or
  materialised into canonical native field storage through erased operations.
* Native offset-backed and Python-projection-backed Bundles pass the same
  generic ``BundleView`` conformance suite.
* Equal registration is idempotent; name, field, class, or binding conflicts
  fail deterministically.
* A Python-free build and the installed C++ SDK remain independent of Python.

Python typing and conversion
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* A plain dataclass resolves as ``TS[Class]`` without deriving from an hgraph
  base and reflects its ordered fields.
* A non-dataclass can be explicitly registered; an unregistered class keeps
  the generic object fallback.
* A native ``CompoundScalar`` remains an offset-backed Bundle; a plain
  dataclass uses the Python-backed Bundle binding.
* Typed assignment retains object identity and accepts subclasses without
  eagerly reading or validating fields.
* Parameterised dataclasses resolve TypeVars through fields, bases, recursion,
  nested structures, and containers with the same invariant semantics and
  reflection as parameterised ``CompoundScalar`` classes.
* Target-typed conversion records the target specialisation. Schema-free
  inference is deterministic across exact classes, ``__orig_class__``, MRO
  bases, and field-schema matching, with ambiguity reported explicitly.
* Registry reset and repeated import do not leave stale class or schema
  pointers.

CompoundScalar conformance
~~~~~~~~~~~~~~~~~~~~~~~~~~

* Existing storage-independent ``CompoundScalar`` hierarchy, generic
  resolution, reflection, recursive registration, auto-constant, and TypeVar
  tests are parameterised to run against Python-backed dataclasses.
* ``TS[PythonClass]`` to and from ``TSB[PythonClass]``, compatible structural
  Bundles, ``as_scalar_ts``, references, strict/non-strict composition,
  defaults, and ``None`` pass the corresponding ``CompoundScalar`` conversion
  tests.
* Resolved generic classes cast to Bundles with their substituted field shapes
  and retain the active specialisation on round-trip.
* Conversion and generic operators select by Bundle capability and schema, not
  storage policy.

Operators
~~~~~~~~~

* Declared ``getattr_`` fields infer the correct output and use normal Python
  descriptor semantics.
* ``None``, missing attributes, defaults, incompatible extracted values, and
  descriptor exceptions have focused tests and diagnostics.
* Explicitly typed computed properties work without being declared storage
  fields.
* ``combine[TS[Class]]`` honours dataclass required fields, defaults, default
  factories, keyword-only fields, ``init=False``, and ``__post_init__``.
* Runtime dispatch covers exact matches, base fallback, union overloads,
  explicit ``dispatch_on``, output specialisation, switch branches, down-cast,
  reference down-cast, and multiple-inheritance ambiguity using the existing
  Bundle dispatcher.
* Dispatch distinguishes generic specialisations such as ``Box[int]`` and
  ``Box[str]`` after storage and after an up-cast, despite their shared Python
  runtime class.
* Equality, inequality, and deduplication follow Python for equal distinct
  instances, subclasses, and custom equality implementations. Hashing follows
  Python for custom and generated hashes. Unhashable classes are rejected as
  ``TSS``/``TSD`` keys rather than receiving an identity-hash fallback.
* Existing generic Bundle attribute, comparison, conversion, and composition
  operators work through type erasure for both storage strategies.
* Schema-directed Bundle codec round trips reconstruct the Python class when
  its registered construction policy permits it.

Semantics and robustness
~~~~~~~~~~~~~~~~~~~~~~~~

* Tests document that in-place mutation creates no tick and that emitting a new
  object does.
* Recursive and inherited dataclass fields do not imply native recursive
  storage.
* Python exceptions cross the bridge without losing the owning class,
  attribute, or operator context.
* Full C++ and Python suites, limited-API builds, and an installed-package
  Python consumer pass.
* Benchmarks cover the workloads listed in `Performance and memory`_.

Implementation status
---------------------

The RFC was accepted on 2026-07-23. Its implementation is included in the
corresponding implementation PR.

The implementation keeps the accepted representation boundary: a Python-owned
structured scalar is a normal named ``Bundle`` with a non-composite owning
binding. Its complete read-only ``IndexedValueOps`` surface projects declared
attributes lazily, so ``BundleView`` and generic Bundle operators do not need
to know which storage policy owns the value. ``TSB`` continues to hold the
anonymous structural field representation and constructs the Python object
only when converted to the scalar form. Realised ``TSB`` output storage keeps
that structural field layout even when the owning binding is a non-composite
closed polymorphic union. The projection is recursive, so named Python-owned
bundles nested inside anonymous structural bundles keep the same layout
contract. This applies equally to standalone bundles and bundles held in
``TSD`` slots.

One representation-neutral capability was added to erased value operations:
an owning binding may decide whether a partially valid indexed source contains
enough fields to construct it. Python-owned bindings use this to distinguish
required constructor fields from defaults and ``init=False`` fields; ordinary
composite Bundles retain the existing all-fields-valid rule.

The public Python surface consists of lazy recognition of standard-library
dataclasses and ``register_python_object_type`` for explicit classes. The
implementation includes nominal and generic registration, inheritance and
closed-union dispatch, recursive fields, constructor/default handling,
``TS``/``TSB`` conversion, reflection, Python equality and hashing, key
capability validation, focused native and Python conformance tests, and paired
diagnostic benchmark scenarios for the workloads in `Performance and memory`_.

References
----------

* :doc:`rfc_0003_extension_scalar_registration`
* :doc:`../developer_guide/data_structures/schemas/scalar`
* :doc:`../developer_guide/python_bridge`
* `PEP 557 -- Data Classes <https://peps.python.org/pep-0557/>`_
* `Python dataclasses documentation
  <https://docs.python.org/3/library/dataclasses.html>`_
* `Python data model: customising attribute access
  <https://docs.python.org/3/reference/datamodel.html#customizing-attribute-access>`_
* `PEP 681 -- Data Class Transforms <https://peps.python.org/pep-0681/>`_
