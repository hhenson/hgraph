#ifndef HGRAPH_TS_INPUT_ACTIVITY_H
#define HGRAPH_TS_INPUT_ACTIVITY_H

#include <cstddef>
#include <cstdint>
#include <vector>

namespace hgraph
{
    enum class TSInputActivityMode : std::uint8_t
    {
        Value = 1,
        Structural = 2,
    };

    /** One active observation, reached through static owned input prefixes.
     * An absent path is passive. Paths terminate at an owned projection or a
     * peered input root; activity inside a peered target is unsupported.
     */
    struct TSInputActivityEntry
    {
        std::vector<std::size_t> path{};
        TSInputActivityMode mode{TSInputActivityMode::Value};

        bool operator==(const TSInputActivityEntry &) const = default;
    };
}

#endif
