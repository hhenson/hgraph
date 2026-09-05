# Architecture decision records

Numbered records in this directory capture decisions that constrain multiple
compiler passes or public artifacts. A record may accept an architectural
direction while leaving a specific library, serialization, ABI, or source
syntax unresolved and named as such.

- [0001: Declarative parser and source-accurate syntax](0001-declarative-parser.md)
- [0002: Typed HIR and hgraph IR are mandatory backend boundaries](0002-shared-ir-boundaries.md)
- [0003: Native code is exposed by descriptors, not inline source](0003-native-descriptor-boundary.md)
