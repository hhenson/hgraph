<!-- Keep what applies; delete the rest. CLAUDE.md sections 2 and 3 are the contract. -->

## What and why

## Design record
- [ ] The developer-guide page this implements or changes is updated in this PR (`docs/source/developer_guide/...`), or this is explicitly a no-design-change fix.

## Guardrails
- [ ] `python/tests/test_architecture_ratchets.py` passes; any baseline change is a lowering, or a raise with its design record cited here.
- [ ] No per-tick registry lock, `thread_local`, or `TSTypeKind::REF` probe was added to a runtime path.

## If this fixes a behaviour found in the field
- [ ] The minimised shape is covered by an authoring-shape sweep axis (`python/tests/test_*_sweep.py`) or a parity recipe/template (`tools/parity`), per `parity_testing.rst`.
