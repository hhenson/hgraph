#ifndef HGRAPH_CPP_ROOT_TS_EVENT_H
#define HGRAPH_CPP_ROOT_TS_EVENT_H

#include <cstddef>
#include <cstdint>
#include <typeinfo>
#include <utility>
#include <new>
#include <vector>
#include <string>
#include <cstring>
#include <type_traits>
#include <functional>

#include "hgraph/util/date_time.h"
#include "hgraph/hgraph_export.h"
#include <nanobind/nanobind.h>
#include "hgraph/types/v2/any_value.h"

namespace hgraph {

    namespace nb = nanobind;


    enum class TsEventKind : std::uint8_t { None = 0, Recover = 1, Invalidate = 2, Modify = 3 };

    struct HGRAPH_EXPORT TsEventAny {
        engine_time_t time{};
        TsEventKind kind{TsEventKind::None};
        AnyValue<> value; // engaged when kind==Modify or when kind==Recover and valid

        static TsEventAny none(engine_time_t t);
        static TsEventAny invalidate(engine_time_t t);
        static TsEventAny recover(engine_time_t t);
        template <class T>
        static TsEventAny modify(engine_time_t t, T&& v) {
            TsEventAny e{t, TsEventKind::Modify, {}};
            e.value.template emplace<std::decay_t<T>>(std::forward<T>(v));
            return e;
        }
        template <class T>
        static TsEventAny recover(engine_time_t t, T&& v) {
            TsEventAny e{t, TsEventKind::Recover, {}};
            e.value.template emplace<std::decay_t<T>>(std::forward<T>(v));
            return e;
        }
    };

    struct HGRAPH_EXPORT TsValueAny {
        bool has_value{false};
        AnyValue<> value;

        static TsValueAny none();
        template <class T>
        static TsValueAny of(T&& v) {
            TsValueAny sv; sv.has_value = true; sv.value.template emplace<std::decay_t<T>>(std::forward<T>(v)); return sv;
        }
    };

    // Collection event support (type-erased keys and values)
    using AnyKey = AnyValue<>; // alias for readability

    enum class ColItemKind : std::uint8_t { Reset = 0, Modify = 1, Remove = 2 };

    struct CollectionItem {
        AnyKey key;
        ColItemKind kind{ColItemKind::Modify};
        AnyValue<> value; // engaged only when kind==Modify
    };

    struct HGRAPH_EXPORT TsCollectionEventAny {
        engine_time_t time{};
        TsEventKind kind{TsEventKind::None}; // None, Invalidate, Modify
        std::vector<CollectionItem> items;   // when kind==Modify

        static TsCollectionEventAny none(engine_time_t t);
        static TsCollectionEventAny invalidate(engine_time_t t);
        static TsCollectionEventAny modify(engine_time_t t);
        static TsCollectionEventAny recover(engine_time_t t);

        // Builders (valid only when kind==Modify)
        void add_modify(AnyKey key, AnyValue<> value);
        void add_reset(AnyKey key);
        void remove(AnyKey key);
    };

    // String formatting helpers (exported API)
    HGRAPH_EXPORT std::string to_string(const TsEventAny& e);
    HGRAPH_EXPORT std::string to_string(const TsValueAny& v);
    HGRAPH_EXPORT std::string to_string(const TsCollectionEventAny& e);

} // namespace hgraph

#endif // HGRAPH_CPP_ROOT_TS_EVENT_H
