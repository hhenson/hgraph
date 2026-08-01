#ifndef HGRAPH_TYPES_UTILS_STABLE_SLOT_STORE_FWD_H
#define HGRAPH_TYPES_UTILS_STABLE_SLOT_STORE_FWD_H

#include <cstdint>

namespace hgraph
{
    /** Durable lifecycle information required by a stable-slot owner. */
    enum class StableSlotStateModel : std::uint8_t
    {
        ConstructedOnly,
        ConstructedAndLive,
    };

    /** Physical representation selected when a stable-slot layout is bound. */
    enum class StableSlotRepresentation : std::uint8_t
    {
        Unbound,
        TaggedPointer,
        Bitmap,
    };

    struct StableSlotStoreOps;

    template <StableSlotStateModel Model>
    class StableSlotStore;
}  // namespace hgraph

#endif  // HGRAPH_TYPES_UTILS_STABLE_SLOT_STORE_FWD_H
