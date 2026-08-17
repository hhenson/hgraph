#include <hgraph/types/table_config.h>

#include <hgraph/runtime/global_state.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/value/value.h>

#include <stdexcept>
#include <string_view>
#include <utility>

namespace hgraph::table
{
    namespace
    {
        inline constexpr std::string_view CONFIG_KEY{"__hgraph.table.config__"};

        void ensure_config_type()
        {
            (void)TypeRegistry::instance().register_scalar<TableConfig>("TableConfig");
        }
    }  // namespace

    void set_config(GlobalStateView state, TableConfig config)
    {
        if (!state.valid())
        {
            throw std::logic_error("table configuration requires GlobalState");
        }
        if (config.date_key.empty() || config.as_of_key.empty())
        {
            throw std::invalid_argument("table configuration requires non-empty column keys");
        }
        ensure_config_type();
        state.set(CONFIG_KEY, Value{std::move(config)});
    }

    TableConfig config(GlobalStateView state)
    {
        if (!state.valid())
        {
            return TableConfig{};
        }
        const ValueView value = state.get(CONFIG_KEY);
        return value.valid() ? value.checked_as<TableConfig>() : TableConfig{};
    }
}  // namespace hgraph::table
