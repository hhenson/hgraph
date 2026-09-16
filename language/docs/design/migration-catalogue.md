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

Overload-specific parallel names (for example `sum_reset` and `dedup_float`)
may preserve an admitted algorithm when HGL cannot yet combine its signature
with the main family. Record the native identity, parameter/default differences,
and the remaining publication work explicitly.

For each unblocked domain, preserve parameter names and defaults where admitted,
activation, validity, repeated ticks, and error behavior. Compile and install its HGL source
and readable generated C++; exercise it through public native wiring and
scripted HGL tests where the test harness supports the input shape. Existing
core implementations remain in place during this authoring milestone.

The recovered PR #801 provides the `HGL-MIG-001`–`015` and
`HGL-LIB-001`–`004` references. Its branch and proposed sources are guidance,
never an integration dependency. Missing language semantics remain separate
design work; the catalogue does not authorize inventing them.
