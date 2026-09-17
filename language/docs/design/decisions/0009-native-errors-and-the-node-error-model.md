# ADR 0009: native functions may raise, under hgraph's node error model

Status: accepted. Implemented for source-defined and imported evaluation-time
native functions (`throws`, descriptor policy `translated`), and used by the
first checked kernels in `hgraph.native` (power, shifts).

## Context

The first native interface ([ADR 0005](0005-inline-cpp-native-functions.md))
fixed evaluation-time native functions as non-blocking and `noexcept`. That
envelope excluded every kernel whose contract includes failure: checked
integer arithmetic, shift counts, parsing, and the string and regex families.
The migration catalogue records seventeen core operators blocked on it
(blocker B1).

Meanwhile the language already had an error path it did not name. Generated
node bodies raise: strict `at` throws on a missing key, and a runtime body
that delegates to a native operator inherits that operator's exceptions. What
happened next was defined by hgraph, not by HGL: the language model listed
"error model for runtime nodes" as an open decision.

hgraph's runtime contract ([error handling](../../../../docs/source/developer_guide/error_handling.rst))
is settled: a node evaluation may throw; a node whose error output is
captured (`exception_time_series`, or `try_except` around a sub-graph) writes
a `NodeError` to that output; otherwise the exception propagates to the
enclosing `try_except` boundary or out of the graph. Output writes made before
the throw are already published.

## Decision

1. **HGL adopts hgraph's node error model as the language rule.** A runtime
   evaluation that raises ends at the raise. HGL adds no exception surface of
   its own: no `try`, no catch, no error value. Whether the raise becomes a
   `NodeError` on an error output or propagates is decided by the graph that
   wires the node, exactly as for a hand-written C++ node. Output and state
   writes made earlier in the same evaluation stand. A body that must not
   publish a partial result calls its fallible natives before writing.

2. **A native function declares that it may raise with `throws`.** The word
   follows the signature:

   ```hgl
   native fn power(lhs: i64, rhs: i64) -> i64 throws {
       cpp(const hgraph::Int &lhs, const hgraph::Int &rhs) {
           return hgraph::stdlib::scalar_pow<hgraph::Int>::apply(lhs, rhs);
       }
   }
   ```

   Without `throws` the function keeps the ADR 0005 contract and is emitted
   `noexcept`; a body that raises anyway terminates the process, as any
   `noexcept` C++ function does. With `throws` the generated function has no
   exception specification and the call site is unchanged.

3. **The descriptor records it as the `translated` exception policy**, and the
   reader admits `translated` in the evaluation phase. An imported native
   declared `translated` is callable from a runtime body on the same terms as
   a source `throws` function. `translated` means: the C++ symbol may throw an
   exception derived from `std::exception`, whose message hgraph carries into
   the `NodeError`. Blocking remains outside the evaluation envelope.

4. **Owned scalar results are in the value envelope.** A native result of a
   canonical scalar type, including `str`, is returned by value. Allocation
   for that result is not an effect the declaration must name. Results that
   are collections, borrowed views, or opaque state remain separate decisions.

## Consequences

- Blocker B1's error half is closed: checked kernels bind directly
  (`power`, `shift_left`, `shift_right` in `hgraph.native`; `pow_`,
  `lshift_`, `rshift_`, `substr` as parallel identities). What B1 still
  holds is the owned-result half beyond scalars: sequences (`split`), regex
  state (`match_`, `replace`), packs with formatting (`format_`), and the
  coupled result of `divmod_`.
- Parity with the native operator includes the exception and its message:
  the generated catalogue tests assert both. HGL-level `test` blocks cannot
  yet assert a raise; that harness gap is unchanged.
- The language model's open "error model for runtime nodes" decision is
  closed by this record. Integer division, overflow, NaN and string operator
  semantics remain open where they were.
- The `throws` word is a contextual keyword like `native`; it is no longer
  usable as a name.
