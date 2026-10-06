#ifndef HGL_TEMPORAL_LITERALS_H
#define HGL_TEMPORAL_LITERALS_H

#include <hgraph/runtime/global_state.h>
#include <hgraph/types/temporal.h>
#include <stdexcept>
#include <string>

namespace hgl::temporal
{
    // Literal construction is a cold value operation. The direct harness passes
    // its run provider explicitly. Cold generated composition can use an active
    // native wiring context, or the default provider when there is none.
    inline const hgraph::TimeZoneProvider &provider() {
        if (auto *state = hgraph::GlobalContext::active_state();
            state && state->view().contains(hgraph::TIME_ZONE_PROVIDER_STATE_KEY)) {
            return hgraph::time_zone_provider(state->view());
        }
        static const auto fallback = hgraph::make_time_zone_provider();
        return *fallback;
    }

    inline hgraph::ZoneId zone(std::string_view name, const hgraph::TimeZoneProvider &provider) {
        const hgraph::ZoneId result{name};
        if (!provider.contains(result)) {
            throw std::invalid_argument("unknown time-zone '" + std::string{name} + "'");
        }
        return result;
    }

    inline hgraph::ZoneId zone(std::string_view name) { return zone(name, provider()); }

    inline hgraph::ZonedTime zoned_time(std::int64_t micros, std::string_view name,
                                         const hgraph::TimeZoneProvider &provider) {
        return hgraph::ZonedTime{hgraph::CivilTime{micros}, zone(name, provider)};
    }

    inline hgraph::ZonedTime zoned_time(std::int64_t micros, std::string_view name) {
        return zoned_time(micros, name, provider());
    }

    inline hgraph::ZonedDateTime zoned(std::int64_t micros, std::string_view name, std::int32_t offset,
                                      const hgraph::TimeZoneProvider &provider) {
        const auto named = zone(name, provider);
        const hgraph::Instant instant{hgraph::Duration{micros}};
        if (provider.at(instant, named).offset_seconds != offset) {
            throw std::invalid_argument("zoned literal offset does not match time-zone '" + std::string{name} + "'");
        }
        return hgraph::ZonedDateTime::from_resolved(instant, named, offset);
    }

    inline hgraph::ZonedDateTime zoned(std::int64_t micros, std::string_view name, std::int32_t offset) {
        return zoned(micros, name, offset, provider());
    }
}
#endif
