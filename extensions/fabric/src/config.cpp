#include <hgraph/fabric/config.h>

#include <hgraph/persistence/store_location.h>
#include <hgraph/types/metadata/type_registry.h>

#include <stdexcept>
#include <utility>

namespace hgraph::fabric
{
    namespace
    {
        inline constexpr std::string_view CONFIG_KEY{"__hgraph.fabric.config__"};

        struct FabricConfigHolder
        {
            FabricConfig config{};
        };

        void ensure_holder_type()
        {
            (void)TypeRegistry::instance().register_scalar<FabricConfigHolder>(
                "hgraph.fabric::ConfigHolder");
        }
    }  // namespace

    FabricConfig make_memory_fabric_config(Str prefix)
    {
        FabricConfig config{
            .prefix = std::move(prefix),
            .objects = persistence::store::make_object_store(
                persistence::store::ObjectStoreConfig{}),
            .frames = persistence::store::make_frame_store(
                persistence::store::FrameStoreConfig{}),
            .notifications = make_memory_notifier(),
        };
        require_valid_config(config);
        return config;
    }

    void require_valid_config(const FabricConfig &config)
    {
        persistence::store::require_valid_key(config.prefix);
        if (!config.objects)
        {
            throw std::invalid_argument("fabric configuration requires an object store");
        }
        if (!config.frames)
        {
            throw std::invalid_argument("fabric configuration requires a frame store");
        }
        if (!config.notifications)
        {
            throw std::invalid_argument("fabric configuration requires a notifier");
        }
    }

    void set_fabric_config(GlobalStateView state, FabricConfig config)
    {
        if (!state.valid())
        {
            throw std::logic_error("fabric configuration requires GlobalState");
        }
        require_valid_config(config);
        ensure_holder_type();
        state.set(CONFIG_KEY, Value{FabricConfigHolder{std::move(config)}});
    }

    void clear_fabric_config(GlobalStateView state)
    {
        if (!state.valid())
        {
            throw std::logic_error("clearing fabric configuration requires GlobalState");
        }
        static_cast<void>(state.erase(CONFIG_KEY));
    }

    std::optional<FabricConfig> fabric_config(GlobalStateView state)
    {
        if (!state.valid()) { return std::nullopt; }
        const ValueView value = state.get(CONFIG_KEY);
        if (!value.valid()) { return std::nullopt; }
        return value.checked_as<FabricConfigHolder>().config;
    }
}  // namespace hgraph::fabric
