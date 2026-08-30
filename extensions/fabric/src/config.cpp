#include <hgraph/fabric/config.h>

#include <hgraph/persistence/store_location.h>
#include <hgraph/types/metadata/type_registry.h>

#include <stdexcept>
#include <utility>

namespace hgraph::fabric
{
    namespace
    {
        inline constexpr std::string_view CONFIG_KEY_PREFIX{"__hgraph.fabric.config__/"};
        inline constexpr std::string_view DEFAULT_CONFIG_PATH{"fabric"};

        struct FabricConfigHolder
        {
            FabricConfig config{};
        };

        void ensure_holder_type()
        {
            (void)TypeRegistry::instance().register_scalar<FabricConfigHolder>(
                "hgraph.fabric::ConfigHolder");
        }

        [[nodiscard]] Str config_key(std::string_view path)
        {
            if (path.empty())
            {
                throw std::invalid_argument("fabric configuration path must not be empty");
            }
            Str key{CONFIG_KEY_PREFIX};
            key.append(path);
            return key;
        }
    }  // namespace

    FabricConfig make_memory_fabric_config(Str prefix,
                                           std::size_t notification_request_limit)
    {
        FabricConfig config{
            .prefix = std::move(prefix),
            .objects = persistence::store::make_object_store(
                persistence::store::ObjectStoreConfig{}),
            .frames = persistence::store::make_frame_store(
                persistence::store::FrameStoreConfig{}),
            .notifications = make_memory_notifier(),
            .notification_request_limit = notification_request_limit,
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
        if (config.notification_request_limit == 0U)
        {
            throw std::invalid_argument(
                "fabric notification request limit must be positive");
        }
    }

    void set_fabric_config(GlobalStateView state, std::string_view path,
                           FabricConfig config)
    {
        if (!state.valid())
        {
            throw std::logic_error("fabric configuration requires GlobalState");
        }
        require_valid_config(config);
        ensure_holder_type();
        state.set(config_key(path), Value{FabricConfigHolder{std::move(config)}});
    }

    void set_fabric_config(GlobalStateView state, FabricConfig config)
    {
        set_fabric_config(state, DEFAULT_CONFIG_PATH, std::move(config));
    }

    void clear_fabric_config(GlobalStateView state, std::string_view path)
    {
        if (!state.valid())
        {
            throw std::logic_error("clearing fabric configuration requires GlobalState");
        }
        static_cast<void>(state.erase(config_key(path)));
    }

    void clear_fabric_config(GlobalStateView state)
    {
        clear_fabric_config(state, DEFAULT_CONFIG_PATH);
    }

    std::optional<FabricConfig> fabric_config(GlobalStateView state,
                                              std::string_view path)
    {
        if (!state.valid()) { return std::nullopt; }
        const ValueView value = state.get(config_key(path));
        if (!value.valid()) { return std::nullopt; }
        return value.checked_as<FabricConfigHolder>().config;
    }

    std::optional<FabricConfig> fabric_config(GlobalStateView state)
    {
        return fabric_config(state, DEFAULT_CONFIG_PATH);
    }
}  // namespace hgraph::fabric
