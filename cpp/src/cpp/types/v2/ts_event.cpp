#include <hgraph/types/v2/ts_event.h>
#include <sstream>
#include <chrono>
#include <iomanip>
#if defined(HGRAPH_WITH_PYTHON_TOSTRING)
#include <nanobind/stl/string.h>
#include <Python.h>
#endif

namespace hgraph
{
    bool operator==(TypeId a, TypeId b) { return a.info == b.info; }

    TsEventAny TsEventAny::none(engine_time_t t) { return {t, TsEventKind::None, {}}; }
    TsEventAny TsEventAny::invalidate(engine_time_t t) { return {t, TsEventKind::Invalidate, {}}; }
    TsEventAny TsEventAny::recover(engine_time_t t) { return {t, TsEventKind::Recover, {}}; }

    TsValueAny TsValueAny::none() { return {}; }

    static const char *kind_to_cstr(TsEventKind k) {
        switch (k) {
            case TsEventKind::None: return "None";
            case TsEventKind::Recover: return "Recover";
            case TsEventKind::Invalidate: return "Invalidate";
            case TsEventKind::Modify: return "Modify";
            default: return "?";
        }
    }

    std::string to_string(const AnyValue<> &v) {
        if (!v.has_value()) return std::string("<empty>");

        // Use visitor pattern for type-safe dispatch
        std::string result;

        // Try each supported type using visit_as
        if (v.visit_as<bool>([&result](bool val) { result = val ? "true" : "false"; })) return result;

        if (v.visit_as<int64_t>([&result](int64_t val) { result = std::to_string(val); })) return result;

        if (v.visit_as<double>([&result](double val) { result = std::to_string(val); })) return result;

        if (v.visit_as<std::string>([&result](const std::string &val) { result = val; })) return result;

        // engine_date_t (YYYY-MM-DD)
        if (v.visit_as<engine_date_t>([&result](const engine_date_t &val) {
            std::ostringstream oss;
            oss << std::setw(4) << std::setfill('0') << static_cast<int>(val.year())
                << '-' << std::setw(2) << std::setfill('0') << static_cast<unsigned>(val.month())
                << '-' << std::setw(2) << std::setfill('0') << static_cast<unsigned>(val.day());
            result = oss.str();
        }))
            return result;

        // engine_time_t (microseconds since epoch)
        if (v.visit_as<engine_time_t>([&result](const engine_time_t &val) {
            using namespace std::chrono;
            auto us = duration_cast<microseconds>(val.time_since_epoch()).count();
            result  = std::to_string(us) + "us_since_epoch";
        }))
            return result;

        // engine_time_delta_t (microseconds)
        if (v.visit_as<engine_time_delta_t>([&result](const engine_time_delta_t &val) {
            using namespace std::chrono;
            auto us = duration_cast<microseconds>(val).count();
            result  = std::to_string(us) + "us";
        }))
            return result;

        // nb::object: render via Python str with fallback to repr (enabled only when HGRAPH_WITH_PYTHON_TOSTRING)
        #if defined(HGRAPH_WITH_PYTHON_TOSTRING)
        if (v.visit_as<nb::object>([&result](const nb::object &val) {
            // If the interpreter isn't initialized, avoid calling into Python
            if (!Py_IsInitialized()) {
                result = "<nb::object (Python not initialized)>";
                return;
            }
            nb::gil_scoped_acquire guard;
            try { result = nb::cast<std::string>(nb::str(val)); } catch (...) { result = nb::cast<std::string>(nb::repr(val)); }
        }))
            return result;
        #else
        if (v.type().info == &typeid(nb::object)) { return std::string("<nb::object>"); }
        #endif

        // Fallback: use visit_untyped to get type name safely
        v.visit_untyped([&result](const void *, const std::type_info &ti) {
            std::ostringstream oss;
            oss << "<value type=" << ti.name() << ">";
            result = oss.str();
        });

        return result;
    }

    std::string to_string(const TsEventAny &e) {
        std::ostringstream oss;
        oss << "TsEventAny{";
        // print time as microseconds since epoch
        using namespace std::chrono;
        auto us = duration_cast<microseconds>(e.time.time_since_epoch()).count();
        oss << "time=" << us << "us_since_epoch";
        oss << ", kind=" << kind_to_cstr(e.kind);
        if (e.kind == TsEventKind::Modify || (e.kind == TsEventKind::Recover && e.value.has_value())) {
            oss << ", value=" << to_string(e.value);
        }
        oss << "}";
        return oss.str();
    }

    std::string to_string(const TsValueAny &v) {
        std::ostringstream oss;
        oss << "TsValueAny{";
        if (!v.has_value) { oss << "none"; } else { oss << "value=" << to_string(v.value); }
        oss << "}";
        return oss.str();
    }

    TsCollectionEventAny TsCollectionEventAny::none(engine_time_t t) { return {t, TsEventKind::None, {}}; }
    TsCollectionEventAny TsCollectionEventAny::invalidate(engine_time_t t) { return {t, TsEventKind::Invalidate, {}}; }
    TsCollectionEventAny TsCollectionEventAny::modify(engine_time_t t) { return {t, TsEventKind::Modify, {}}; }
    TsCollectionEventAny TsCollectionEventAny::recover(engine_time_t t) { return {t, TsEventKind::Recover, {}}; }

    TsCollectionEventAny &TsCollectionEventAny::add_modify(AnyKey key, AnyValue<> value) {
        if (kind != TsEventKind::Modify) kind = TsEventKind::Modify;
        CollectionItem item;
        item.key   = std::move(key);
        item.kind  = ColItemKind::Modify;
        item.value = std::move(value);
        items.emplace_back(std::move(item));
        return *this;
    }

    TsCollectionEventAny &TsCollectionEventAny::add_reset(AnyKey key) {
        if (kind != TsEventKind::Modify) kind = TsEventKind::Modify;
        CollectionItem item;
        item.key  = std::move(key);
        item.kind = ColItemKind::Reset;
        items.emplace_back(std::move(item));
        return *this;
    }

    TsCollectionEventAny &TsCollectionEventAny::remove(AnyKey key) {
        if (kind != TsEventKind::Modify) kind = TsEventKind::Modify;
        CollectionItem item;
        item.key  = std::move(key);
        item.kind = ColItemKind::Remove;
        items.emplace_back(std::move(item));
        return *this;
    }

    std::string to_string(const TsCollectionEventAny &e) {
        std::ostringstream oss;
        oss << "TsCollectionEventAny{";
        using namespace std::chrono;
        auto us = duration_cast<microseconds>(e.time.time_since_epoch()).count();
        oss << "time=" << us << "us_since_epoch";
        oss << ", kind=" << kind_to_cstr(e.kind);
        if (e.kind == TsEventKind::Modify) {
            oss << ", items=[";
            for (std::size_t i = 0; i < e.items.size(); ++i) {
                const auto &it = e.items[i];
                if (i) oss << ", ";
                oss << "{key=" << to_string(it.key) << ", kind=";
                switch (it.kind) {
                    case ColItemKind::Modify: oss << "Modify";
                        break;
                    case ColItemKind::Reset: oss << "Reset";
                        break;
                    case ColItemKind::Remove: oss << "Remove";
                        break;
                }
                if (it.kind == ColItemKind::Modify) { oss << ", value=" << to_string(it.value); }
                oss << "}";
            }
            oss << "]";
        }
        oss << "}";
        return oss.str();
    }

    // Safety check to ensure the configured SBO matches nb::object size as agreed.
    static_assert(HGRAPH_TS_VALUE_SBO == sizeof(nanobind::object),
                  "HGRAPH_TS_VALUE_SBO must equal sizeof(nanobind::object)");
} // namespace hgraph