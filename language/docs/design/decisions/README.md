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
