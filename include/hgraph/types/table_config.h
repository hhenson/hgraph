#ifndef HGRAPH_TYPES_TABLE_CONFIG_H
#define HGRAPH_TYPES_TABLE_CONFIG_H

#include <hgraph/hgraph_export.h>
#include <hgraph/util/date_time.h>

#include <optional>
#include <string>

namespace hgraph
{
    class GlobalStateView;
}

/**
 * Generic table-conversion configuration (RFC 0025, checkpoint 2).
 *
 * ``to_table`` / ``from_table`` / ``from_table_const`` are generic operators:
 * they convert between time series and bitemporal table rows for ANY
 * consumer, and they must not read record/replay configuration.  This is
 * their own explicit configuration: the bitemporal column names and the
 * optional fixed as-of override (unset = the evaluation clock).
 *
 * A recording implementation that persists tables carries its OWN copy of
 * these options (wiring-time arguments and its own configuration); sharing
 * the defaults here is a vocabulary convenience, not a coupling.
 */
namespace hgraph::table
{
    struct TableConfig
    {
        std::string             date_key{"__date_time__"};
        std::string             as_of_key{"__as_of__"};
        std::optional<DateTime> as_of{};
    };

    /** Set the configuration in ``state`` before wiring. */
    HGRAPH_EXPORT void set_config(GlobalStateView state, TableConfig config);

    /** The configuration in ``state`` (defaults when no entry is present). */
    [[nodiscard]] HGRAPH_EXPORT TableConfig config(GlobalStateView state);
}  // namespace hgraph::table

#endif  // HGRAPH_TYPES_TABLE_CONFIG_H
