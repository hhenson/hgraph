HGL negative tests
==================

Test bodies admit ``assert raises("code") { ... }`` as a lexical block. The
frontend validates the literal against the execution-error catalogue before
constant folding. AST, HIR and execution IR retain the assertion block; all
ownership, use and phase walks visit it. Its bindings keep ordinary lexical
scope and its effects are preserved. The direct test backend executes once,
catches only a structured HGL execution error and compares the complete code.
Assertion failures and compiler diagnostics remain separate failure channels.
The contextual ``raises`` form is recognized only after ``assert``; native
function exception clauses continue to accept only ``throws``. In particular,
``raises`` cannot alter a native function's generated ``noexcept`` contract.

The error class gives its RTTI default visibility across scripted native
images even in static runtime builds, so Darwin's dynamic cast and exception
matching preserve the same identity. Native fixture functions retain a normal
return path for MSVC's unreachable-code analysis.

Generated generator nodes throw ``hgl::ExecutionError`` at the negative duration
and non-increasing target checks, after evaluating both operands. Root graph
annotation preserves the original exception with ``std::nested_exception``;
the outer ``std::runtime_error`` category and human-readable text are unchanged.
The test eval boundary retains the structured code through annotation. It uses
the existing executor phase runner to observe stop failures, including failures
suppressed by normal unwind policy. Cleanup-only and combined execution/cleanup
failures fail the test; combined failures report both messages. Executor policy,
publication, state, scheduling, and side effects remain unchanged.

The normative contract is ``language/docs/design/execution-error-assertions.md``
and ``error-catalogue.md`` in the shared HGL specification.

Source-rejection tests
----------------------

``hgl test file.hgl`` runs rejection cases and executable tests together. The lexer owns
comment recognition, so annotation-shaped text in strings and block comments
has no meaning. Before compilation, the driver validates whole-line
``# expect-error(category, "code")`` metadata against the source-error
catalogue. ``driver/rejection_source`` maps annotations to enclosing named
tests or declarations using the lossless source tree. Unnamed test contexts
retain their wrappers while their inner declarations own cases. Delimiters
must establish reliable boundaries; recovery at a new declaration keyword
alone cannot justify excluding source. A sole malformed expression-bodied
declaration may extend to EOF without hiding a neighbouring declaration.

All rejection owners are blanked without moving bytes or line breaks. The
driver checks this surviving module before admitting any case, then checks
each selected owner independently restored in the same module scope. Other
owners remain absent. Original source origins preserve file/line locations
through module-part assembly. Normal visibility, imports and resolution still
apply, including dependencies on excluded declarations. The frontend emits structured ``Diagnostic.code``
values at their originating checks. Expected errors match the primary source
file, next physical line, category and complete code one to one; missing,
additional and uncoded errors fail. Related notes are not primary errors.
Malformed metadata, ambiguous boundaries and invalid surviving source prevent
execution. A rejection mismatch fails only its case; remaining rejection
checks and executable tests continue. Named-test selectors also select named
rejection tests; declaration-owned cases always run. Unselected owners remain
excluded and still require valid metadata and boundaries. Results distinguish
executed tests from rejection cases and identify declaration cases by original
file and start line. Pure rejection runs never load or build native code.
There is no rejection flag or alias. Other commands continue to check the
ordinary source, treating annotations as comments.

The shared ``compile-rejection-fixtures.md`` defines the command and matching
contract. Initial coded origins cover required grammar tokens, rolling size
kind and bounds, yield operand type, raises literal code validation, and
statements forbidden in tests. Other diagnostics retain their ordinary
uncoded behavior and cannot satisfy an expectation.
