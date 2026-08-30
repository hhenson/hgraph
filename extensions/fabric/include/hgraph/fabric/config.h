#ifndef HGRAPH_FABRIC_CONFIG_H
#define HGRAPH_FABRIC_CONFIG_H

#include <hgraph/fabric/export.h>
#include <hgraph/fabric/notifier.h>
#include <hgraph/fabric/types.h>

#include <hgraph/persistence/frame_store.h>
#include <hgraph/persistence/value_store.h>
#include <hgraph/persistence/object_store.h>
#include <hgraph/runtime/global_state.h>

#include <cstddef>
#include <optional>
#include <string_view>

namespace hgraph::fabric
{
    inline constexpr std::size_t DEFAULT_NOTIFICATION_REQUEST_LIMIT{1024U};

    /** Run-scoped fabric resources. Handles are owning and copyable so normal
        GlobalState copy-in/copy-out preserves one configured fabric. */
    struct FabricConfig
    {
        Str                              prefix{};
        persistence::store::ObjectStore objects{};
        persistence::store::FrameStore  frames{};
        /** Declared metadata schemas. Structured data goes here as ordinary
            json documents; tabular data goes to `frames` as Arrow. Fabric owns
            no serialization code of its own (RFC 0030). */
        persistence::store::ValueStore  values{};
        Notifier                         notifications{};
        std::size_t                     notification_request_limit{
            DEFAULT_NOTIFICATION_REQUEST_LIMIT};
    };

    /** Construct an isolated in-process fabric for tests and local execution. */
    [[nodiscard]] HGRAPH_FABRIC_EXPORT FabricConfig
    make_memory_fabric_config(
        Str prefix,
        std::size_t notification_request_limit = DEFAULT_NOTIFICATION_REQUEST_LIMIT);

    HGRAPH_FABRIC_EXPORT void require_valid_config(const FabricConfig &config);
    HGRAPH_FABRIC_EXPORT void set_fabric_config(GlobalStateView state,
                                                std::string_view path,
                                                FabricConfig config);
    HGRAPH_FABRIC_EXPORT void set_fabric_config(GlobalStateView state,
                                                FabricConfig config);
    HGRAPH_FABRIC_EXPORT void clear_fabric_config(GlobalStateView state,
                                                  std::string_view path);
    HGRAPH_FABRIC_EXPORT void clear_fabric_config(GlobalStateView state);
    [[nodiscard]] HGRAPH_FABRIC_EXPORT std::optional<FabricConfig>
    fabric_config(GlobalStateView state, std::string_view path);
    [[nodiscard]] HGRAPH_FABRIC_EXPORT std::optional<FabricConfig>
    fabric_config(GlobalStateView state);
}  // namespace hgraph::fabric

#endif  // HGRAPH_FABRIC_CONFIG_H
