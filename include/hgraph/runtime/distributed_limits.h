#ifndef HGRAPH_RUNTIME_DISTRIBUTED_LIMITS_H
#define HGRAPH_RUNTIME_DISTRIBUTED_LIMITS_H

// The size a distributed channel refuses to exceed (RFC 0037).
//
// Its own header because BOTH layers below ``distributed_map.h`` need the
// number and neither owns it: the byte transport enforces it per frame, the
// protocol writes and reads the prefix it bounds. The transport would
// otherwise have to include the value layer to learn one constant.
//
// It is a DEFAULT, not a protocol constant. 64 MiB is a starting bound chosen
// so that a malformed length prefix cannot make a reader allocate without
// limit -- not a statement about how large a legitimate cycle may be. A
// boundary whose delta is genuinely larger raises it through
// ``TransportLimits`` (``distributed_protocol.h``), which both sides of a
// channel must agree on, since the sender's cap bounds what it will write and
// the receiver's bounds what it will accept.

#include <cstddef>

namespace hgraph::distributed
{
    inline constexpr std::size_t DEFAULT_MAX_FRAME_SIZE = 64 * 1024 * 1024;
}  // namespace hgraph::distributed

#endif  // HGRAPH_RUNTIME_DISTRIBUTED_LIMITS_H
