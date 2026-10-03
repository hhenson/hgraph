#ifndef HGL_WIRING_DELTA_TRACE_H
#define HGL_WIRING_DELTA_TRACE_H

#include <hgl/ordinary_values.h>
#include <unordered_map>
#include <unordered_set>

namespace hgl::wiring {
    // Construction-time admission state. Payload values are deliberately not
    // retained: only membership and recursively unchanged child state matter.
    class DeltaTrace {
      public:
        void accept(const hgraph::TSValueTypeMetaData *shape, const hgraph::ValueView &native) {
            using namespace hgraph;
            switch (shape->kind) {
                case TSTypeKind::TS: return;
                case TSTypeKind::TSS: {
                    const auto parts = native.as_bundle();
                    const auto added = parts.at(0).as_set();
                    const auto removed = parts.at(1).as_set();
                    require(!added.empty() || !removed.empty(), "empty set publication");
                    const auto key = [&](const ValueView &value) -> Int {
                        return shape->value_schema->element_type == scalar_descriptor<Bool>::value_meta()
                            ? static_cast<Int>(value.checked_as<Bool>()) : value.checked_as<Int>();
                    };
                    for (const auto value : added) {
                        require(!members_.contains(key(value)), "addition of a present set member");
                        require(!removed.contains(value), "overlapping set membership changes");
                    }
                    for (const auto value : removed) { require(members_.contains(key(value)), "removal of an absent set member"); }
                    for (const auto value : added) { members_.insert(key(value)); }
                    for (const auto value : removed) { members_.erase(key(value)); }
                    return;
                }
                case TSTypeKind::TSL: {
                    const auto entries = native.as_map();
                    require(!entries.empty(), "empty fixed-list publication");
                    for (const auto [key, child] : entries) {
                        const auto index = key.checked_as<Int>();
                        require(index >= 0 && static_cast<std::uint64_t>(index) < shape->value_schema->fixed_size,
                                "fixed-list index outside its extent");
                        children_[index].accept(shape->element_ts(), child);
                    }
                    return;
                }
                case TSTypeKind::TSB: {
                    const auto entries = native.as_bundle();
                    bool any = false;
                    for (std::size_t i = 0; i < shape->field_count(); ++i) {
                        if (!entries.element_valid(i)) { continue; }
                        any = true;
                        children_[static_cast<Int>(i)].accept(shape->fields()[i].type, entries.at(i));
                    }
                    require(any, "empty bundle publication");
                    return;
                }
                case TSTypeKind::TSD: {
                    const auto parts = native.as_bundle();
                    const auto removed = parts.at(0).as_set();
                    const auto modified = parts.at(1).as_map();
                    require(!removed.empty() || !modified.empty(), "empty map publication");
                    for (const auto key : removed) {
                        require(children_.contains(key.checked_as<Int>()), "removal of an absent map key");
                        require(!modified.contains(key), "overlapping map key changes");
                    }
                    for (const auto key : removed) { children_.erase(key.checked_as<Int>()); }
                    for (const auto [key, child] : modified) { children_[key.checked_as<Int>()].accept(shape->element_ts(), child); }
                    return;
                }
                default: throw std::invalid_argument("unsupported publication shape");
            }
        }
      private:
        static void require(bool condition, const char *message) {
            if (!condition) { throw std::invalid_argument(message); }
        }
        std::unordered_set<hgraph::Int> members_;
        std::unordered_map<hgraph::Int, DeltaTrace> children_;
    };
}
#endif
