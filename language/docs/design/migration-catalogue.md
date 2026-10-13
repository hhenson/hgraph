# HGL implementation catalogue

Status: accepted implementation workflow

The current milestone builds the HGL standard library alongside the existing
core. A compiled and tested parallel HGL implementation is completed authoring
work; replacing a production registration is a separate, deferred decision.
Core identity publication does not block a parallel implementation.

The catalogue joins the current C++ operator declarations, registration sites,
expanded registry signatures, and accepted HGL sources. Registration templates
are retained as templates; a source site is not described as an instantiated
overload. Internal operators and non-operator library nodes remain visible.
Optional extensions are listed separately from the core library.

Each operator records its HGL declarations and implementations, concrete or
retained materializations, source locations, implementation form, test evidence,
and outstanding domains. The implementation forms distinguish HGL runtime
bodies, HGL graph composition, and bindings to exposed native value functions.
Delegation to a production temporal operator is pending authoring work. Existing
compiled HGL bodies and native-function bindings receive credit without
claiming their entire native overload family or production cutover is complete.

Work is selected at the candidate-domain level. A blocked row names the missing
type, phase, generic, delta, lifecycle, resource, or testing contract. An
unreviewed candidate stays unreviewed, rather than acquiring an invented
blocker. New declarations and registration sites invalidate the checked
catalogue so additions cannot silently disappear from the migration plan.

The review status must agree with the sources. An implemented or
implemented-slice row needs an authored HGL body; a blocked or unreviewed row
must not have one, so a body the library already authors is reviewed as a
slice, or kept `native-provider` with the reason it does not count (a test form
with a different contract, for example). `implemented` claims the whole
candidate family, so a row that still names a blocker is a slice. The check
rejects each disagreement.

Authored scalar domains are also compared against the native operators they
re-implement, over generated tick sequences, by the standard library
differential in `language/tests/codegen/stdlib_differential_tests.cpp`; its
ratchet requires every scalar `instantiate` entry and concrete `impl fn` in the
hgraph_std implementation sources to be compared or excluded with a reason.

Overload-specific parallel names (for example `sum_reset` and `dedup_float`)
may preserve an admitted algorithm when HGL cannot yet combine its signature
with the main family. Record the native identity, parameter/default differences,
and the remaining publication work explicitly.

For each unblocked domain, preserve parameter names and defaults where admitted,
activation, validity, repeated ticks, and error behavior. Compile and install its HGL source
and readable generated C++; exercise it through public native wiring and
scripted HGL tests where the test harness supports the input shape. Existing
core implementations remain in place during this authoring milestone.

The [migration requirements ledger](https://github.com/hhenson/hgraph_spec/blob/main/language/docs/design/migration-requirements.md) defines the
`HGL-MIG-001`–`015` and `HGL-LIB-001`–`004` identifiers that reviews cite and
records each open decision. It was extracted from PR #801, which is closed.
Missing language semantics remain separate design work; the catalogue does
not authorize inventing them.
