#include <hgraph/types/v2/tss_event.h>
#include <sstream>

namespace hgraph
{

    std::string to_string(const TsSetDeltaAny &d) {
        std::ostringstream oss;
        oss << "TsSetDeltaAny{added=[";
        bool first = true;
        for (const auto &item : d.added) {
            if (!first) oss << ", ";
            oss << to_string(item);
            first = false;
        }
        oss << "], removed=[";
        first = true;
        for (const auto &item : d.removed) {
            if (!first) oss << ", ";
            oss << to_string(item);
            first = false;
        }
        oss << "]}";
        return oss.str();
    }

    std::string to_string(const TsSetEventAny &e) {
        std::ostringstream oss;
        oss << "TsSetEventAny{time=" << e.time.time_since_epoch().count() << ", kind=";
        switch (e.kind) {
            case TsEventKind::None: oss << "None";
                break;
            case TsEventKind::Recover: oss << "Recover";
                break;
            case TsEventKind::Invalidate: oss << "Invalidate";
                break;
            case TsEventKind::Modify: oss << "Modify";
                break;
            default: oss << "Unknown";
                break;
        }
        if (e.kind == TsEventKind::Modify || e.kind == TsEventKind::Recover) {
            oss << ", delta=" << to_string(e.delta);
        }
        oss << "}";
        return oss.str();
    }

}  // namespace hgraph
