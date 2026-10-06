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
