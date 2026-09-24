# Architecture decision records

Numbered records in this directory capture decisions that constrain multiple
compiler passes or public artifacts. A record may accept an architectural
direction while leaving a specific library, serialization, ABI, or source
syntax unresolved and named as such.

- [0001: Declarative parser and source-accurate syntax](0001-declarative-parser.md)
- [0002: Typed HIR and hgraph IR are mandatory backend boundaries](0002-shared-ir-boundaries.md)
- [0003: External native code is exposed by descriptors](0003-native-descriptor-boundary.md)
- [0004: Module descriptors use canonical versioned JSON](0004-json-module-descriptors.md)
- [0005: Module-local exact native functions may contain C++](0005-inline-cpp-native-functions.md)
- [0006: Explicit source parts form one logical module](0006-multi-file-module-parts.md)
- [0007: Explicit parameter-pack shapes](0007-parameter-packs.md)
- [0008: Temporal programming, value functions, and target mappings](0008-temporal-contracts-and-target-mappings.md)
- [0009: Native functions may raise, under hgraph's node error model](0009-native-errors-and-the-node-error-model.md)
- [0010: Clock and scheduler capabilities, scheduled handlers, and input activity](0010-lifecycle-capabilities.md)
- [0011: `cache` declarations](0011-cache-declarations.md)
- [0012: Recursive struct fields](0012-recursive-struct-fields.md)
- [0013: Struct imports](0013-struct-imports.md)
- [0014: Native implementation interfaces](0014-native-implementation-interfaces.md)
