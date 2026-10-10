HGL bytes values
================

The BYTE-1–6 contract in
``external/hgraph_spec/language/docs/design/bytes-values.md`` defines ``bytes``
as one atomic scalar backed by the existing native ``Bytes`` value strategy.
The frontend, canonical types, native package descriptors and both execution
backends preserve that identity. The native-package scalar enum appends its
new entry to preserve the existing package ABI ordinals. Scalar atomic and delta normalization,
publication plans, collection key admission and rolling arrivals admit bytes
recursively alongside the existing scalar leaves.

``bytes()`` constructs a present empty value. ``bytes(octets)`` consumes one
ordinary fixed or unbounded i64 list, once, in the call's ordinary phase.
``hgl/ordinary_values.h`` supplies the shared construction boundary: it checks
all octets before returning independently owned storage and reports
``value.byte_range`` for values outside 0 through 255. It does not wrap a
failed construction as an eval input-profile error. The checker supplies i64
list context to an empty literal and retains existing list-literal admission.
Graph composition cannot use this constructor to read a temporal list.

``len`` returns the ordinary bytes value's octet count. Equality, hash and
unsigned lexicographic order use the existing native scalar operations. Bytes
is admitted as a set member and map key without adding indexing, mutation or
text conversions. Equal and empty publications remain ticks; silence remains
absent. Existing owning Value capture, replay and record retain byte contents.

Required byte defaults use the existing scalar ``Construct`` constant recipe
and are checked before lowering; this does not add a parallel byte storage
representation.

Construction remains an execution recipe rather than an optionally folded
constant: calls in tests and runtime bodies retain their execution error even
when their arguments are constant. Required constant evaluation validates the
recipe and rejects a failed construction.
