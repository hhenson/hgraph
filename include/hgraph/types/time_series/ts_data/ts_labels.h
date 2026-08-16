#ifndef HGRAPH_TYPES_TIME_SERIES_TS_DATA_TS_LABELS_H
#define HGRAPH_TYPES_TIME_SERIES_TS_DATA_TS_LABELS_H

#include <hgraph/types/metadata/type_record.h>
#include <cstdint>
#include <string_view>

namespace hgraph::ts_labels
{
    /**
     * The single source of the interned ``ts.*`` implementation-label
     * grammar: ``ts.<family>.<role>.<position>``, where a root Input is
     * spelled ``owned`` and target-link positions collapse the TSW
     * tick/duration split. Labels participate in TypeRecord identity, so
     * every factory must draw them from here — a hand-built duplicate that
     * drifts mints a distinct record (audit finding, 2026-08-16).
     *
     * Labels outside the grammar that appear at a single site (atomic
     * storage variants, proxy/alternative specials) stay at that site.
     */
    enum class Family : std::uint8_t { TSS, TSD, REF, Fixed, TSLDynamic, TSWTick, TSWDuration };
    enum class Position : std::uint8_t { Root, Embedded };

    namespace detail
    {
        [[nodiscard]] constexpr int role_index(TypeRole role) noexcept
        {
            switch (role)
            {
            case TypeRole::Data: return 0;
            case TypeRole::Input: return 1;
            case TypeRole::Output: return 2;
            default: return -1;
            }
        }
    }  // namespace detail

    /** Grammar label for one interned TSData record, or empty for a role
        outside Data/Input/Output. */
    [[nodiscard]] constexpr std::string_view record_label(Family family, TypeRole role,
                                                          Position position) noexcept
    {
        constexpr std::string_view table[7][2][3] = {
            /* TSS */
            {{"ts.tss.data.root", "ts.tss.input.owned", "ts.tss.output.root"},
             {"ts.tss.data.embedded", "ts.tss.input.embedded", "ts.tss.output.embedded"}},
            /* TSD */
            {{"ts.tsd.data.root", "ts.tsd.input.owned", "ts.tsd.output.root"},
             {"ts.tsd.data.embedded", "ts.tsd.input.embedded", "ts.tsd.output.embedded"}},
            /* REF */
            {{"ts.ref.data.root", "ts.ref.input.owned", "ts.ref.output.root"},
             {"ts.ref.data.embedded", "ts.ref.input.embedded", "ts.ref.output.embedded"}},
            /* Fixed */
            {{"ts.fixed.data.root", "ts.fixed.input.owned", "ts.fixed.output.root"},
             {"ts.fixed.data.embedded", "ts.fixed.input.embedded", "ts.fixed.output.embedded"}},
            /* TSLDynamic */
            {{"ts.tsl.dynamic.data.root", "ts.tsl.dynamic.input.owned", "ts.tsl.dynamic.output.root"},
             {"ts.tsl.dynamic.data.embedded", "ts.tsl.dynamic.input.embedded",
              "ts.tsl.dynamic.output.embedded"}},
            /* TSWTick */
            {{"ts.tsw.tick.data.root", "ts.tsw.tick.input.owned", "ts.tsw.tick.output.root"},
             {"ts.tsw.tick.data.embedded", "ts.tsw.tick.input.embedded", "ts.tsw.tick.output.embedded"}},
            /* TSWDuration */
            {{"ts.tsw.duration.data.root", "ts.tsw.duration.input.owned", "ts.tsw.duration.output.root"},
             {"ts.tsw.duration.data.embedded", "ts.tsw.duration.input.embedded",
              "ts.tsw.duration.output.embedded"}},
        };
        const int role_at = detail::role_index(role);
        if (role_at < 0) { return {}; }
        return table[static_cast<int>(family)][position == Position::Root ? 0 : 1][role_at];
    }

    /** Peered target-link record label (always Input role); the TSW
        tick/duration split collapses at target positions. */
    [[nodiscard]] constexpr std::string_view target_label(Family family) noexcept
    {
        switch (family)
        {
        case Family::TSS: return "ts.tss.input.target";
        case Family::TSD: return "ts.tsd.input.target";
        case Family::REF: return "ts.ref.input.target";
        case Family::Fixed: return "ts.fixed.input.target";
        case Family::TSLDynamic: return "ts.tsl.dynamic.input.target";
        case Family::TSWTick:
        case Family::TSWDuration: return "ts.tsw.input.target";
        }
        return {};
    }

    /** Composite (bundle-of-links) input record for a keyed root. */
    inline constexpr std::string_view tsd_input_composite = "ts.tsd.input.composite";

    /** Composite (bundle-of-links) input record for a fixed-structure root. */
    inline constexpr std::string_view fixed_input_composite = "ts.fixed.input.composite";

    /** Read-only key-set projection carved out of a TSD record. */
    [[nodiscard]] constexpr std::string_view tsd_key_set_label(TypeRole role) noexcept
    {
        switch (role)
        {
        case TypeRole::Data: return "ts.tsd.key-set.data";
        case TypeRole::Input: return "ts.tsd.key-set.input";
        case TypeRole::Output: return "ts.tsd.key-set.output";
        default: return {};
        }
    }

    /** Dynamic-TSL composite storage records, or empty for a role outside
        Data/Input/Output (callers preserve their throwing contract). */
    [[nodiscard]] constexpr std::string_view tsl_dynamic_composite_label(TypeRole role,
                                                                         bool root_record) noexcept
    {
        constexpr std::string_view table[2][3] = {
            {"ts.tsl.dynamic.data.composite.embedded", "ts.tsl.dynamic.input.composite.embedded",
             "ts.tsl.dynamic.output.composite.embedded"},
            {"ts.tsl.dynamic.data.composite", "ts.tsl.dynamic.input.composite",
             "ts.tsl.dynamic.output.composite"},
        };
        const int role_at = detail::role_index(role);
        if (role_at < 0) { return {}; }
        return table[root_record ? 1 : 0][role_at];
    }
}  // namespace hgraph::ts_labels

#endif  // HGRAPH_TYPES_TIME_SERIES_TS_DATA_TS_LABELS_H
