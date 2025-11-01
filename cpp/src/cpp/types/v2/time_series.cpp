#include <hgraph/types/v2/time_series.h>
#include <sstream>
#include <chrono>
#include <iomanip>
#include <nanobind/stl/string.h>
#include <Python.h>

namespace hgraph {

bool operator==(TypeId a, TypeId b) { return a.info == b.info; }

TsEventAny TsEventAny::none(engine_time_t t) { return {t, TsEventKind::None, {}}; }
TsEventAny TsEventAny::invalidate(engine_time_t t) { return {t, TsEventKind::Invalidate, {}}; }

TsValueAny TsValueAny::none() { return {}; }

static const char* kind_to_cstr(TsEventKind k) {
    switch (k) {
        case TsEventKind::None: return "None";
        case TsEventKind::Invalidate: return "Invalidate";
        case TsEventKind::Modify: return "Modify";
        default: return "?";
    }
}

std::string to_string(const AnyValue<>& v) {
    if (!v.has_value()) return std::string("<empty>");

    if (auto p = v.get_if<bool>()) return *p ? "true" : "false";
    if (auto p = v.get_if<int64_t>()) return std::to_string(*p);
    if (auto p = v.get_if<double>()) return std::to_string(*p);
    if (auto p = v.get_if<std::string>()) return *p;

    // engine_date_t (YYYY-MM-DD)
    if (auto p = v.get_if<engine_date_t>()) {
        std::ostringstream oss;
        oss << std::setw(4) << std::setfill('0') << static_cast<int>(p->year())
            << '-' << std::setw(2) << std::setfill('0') << static_cast<unsigned>(p->month())
            << '-' << std::setw(2) << std::setfill('0') << static_cast<unsigned>(p->day());
        return oss.str();
    }

    // engine_time_t (microseconds since epoch)
    if (auto p = v.get_if<engine_time_t>()) {
        using namespace std::chrono;
        auto us = duration_cast<microseconds>(p->time_since_epoch()).count();
        return std::to_string(us) + "us_since_epoch";
        }

    // engine_time_delta_t (microseconds)
    if (auto p = v.get_if<engine_time_delta_t>()) {
        using namespace std::chrono;
        auto us = duration_cast<microseconds>(*p).count();
        return std::to_string(us) + "us";
    }

    // nb::object: render via Python str with fallback to repr
    if (auto p = v.get_if<nb::object>()) {
        // If the interpreter isn't initialized, avoid calling into Python
        if (!Py_IsInitialized()) {
            return std::string("<nb::object (Python not initialized)>");
        }
        nb::gil_scoped_acquire guard;
        try {
            return nb::cast<std::string>(nb::str(*p));
        } catch (...) {
            return nb::cast<std::string>(nb::repr(*p));
        }
    }

    // Fallback: type name only (do not dereference unknown types)
    const char* tn = v.type().info ? v.type().info->name() : "<unknown>";
    std::ostringstream oss;
    oss << "<value type=" << tn << ">";
    return oss.str();
}

std::string to_string(const TsEventAny& e) {
    std::ostringstream oss;
    oss << "TsEventAny{";
    // print time as microseconds since epoch
    using namespace std::chrono;
    auto us = duration_cast<microseconds>(e.time.time_since_epoch()).count();
    oss << "time=" << us << "us_since_epoch";
    oss << ", kind=" << kind_to_cstr(e.kind);
    if (e.kind == TsEventKind::Modify) {
        oss << ", value=" << to_string(e.value);
    }
    oss << "}";
    return oss.str();
}

std::string to_string(const TsValueAny& v) {
    std::ostringstream oss;
    oss << "TsValueAny{";
    if (!v.has_value) {
        oss << "none";
    } else {
        oss << "value=" << to_string(v.value);
    }
    oss << "}";
    return oss.str();
}

// Safety check to ensure the configured SBO matches nb::object size as agreed.
static_assert(HGRAPH_TS_VALUE_SBO == sizeof(nanobind::object),
              "HGRAPH_TS_VALUE_SBO must equal sizeof(nanobind::object)");

} // namespace hgraph
