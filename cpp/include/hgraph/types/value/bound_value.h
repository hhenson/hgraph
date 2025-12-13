//
// Created by Howard Henson on 13/12/2025.
//
// BoundValue - Result of schema-driven binding for REF dereferencing
//

#ifndef HGRAPH_VALUE_BOUND_VALUE_H
#define HGRAPH_VALUE_BOUND_VALUE_H

#include <hgraph/types/value/deref_time_series_value.h>
#include <hgraph/types/value/time_series_value.h>
#include <hgraph/util/date_time.h>
#include <memory>
#include <vector>
#include <variant>

namespace hgraph::value {

    /**
     * BoundValueKind - Type of binding created during schema matching
     */
    enum class BoundValueKind {
        Peer,       // Direct match: TS[X] → TS[X] (no dereferencing)
        Deref,      // Dereference: REF[TS[X]] → TS[X]
        Composite   // Composite: TSB/TSL with mixed fields requiring per-element binding
    };

    /**
     * BoundValue - Result of binding an input schema to an output value
     *
     * Represents the binding relationship between what an input expects
     * and what an output provides. Handles three cases:
     *
     * 1. Peer: Direct match where output type equals input type.
     *    No transformation needed - just points to the source value.
     *
     * 2. Deref: Output is REF[X] and input expects X.
     *    Creates a DerefTimeSeriesValue wrapper for transparent dereferencing.
     *
     * 3. Composite: Output is TSB/TSL with some fields requiring binding.
     *    Creates child BoundValues for each field/element.
     *
     * Lifecycle:
     *   bound.begin_evaluation(time);
     *   if (bound.modified_at(time)) {
     *       auto value = bound.value();
     *       // Use value...
     *   }
     *   bound.end_evaluation();
     */
    class BoundValue {
    public:
        BoundValue() = default;

        // Move only (owns unique_ptr and vector)
        BoundValue(BoundValue&&) noexcept = default;
        BoundValue& operator=(BoundValue&&) noexcept = default;
        BoundValue(const BoundValue&) = delete;
        BoundValue& operator=(const BoundValue&) = delete;

        // Factory methods

        /**
         * Create a peer binding (direct match)
         */
        static BoundValue make_peer(TimeSeriesValue* source) {
            BoundValue bv;
            bv._kind = BoundValueKind::Peer;
            bv._schema = source ? source->schema() : nullptr;
            bv._data = source;
            return bv;
        }

        /**
         * Create a deref binding (REF dereferencing)
         */
        static BoundValue make_deref(std::unique_ptr<DerefTimeSeriesValue> deref, const TypeMeta* schema) {
            BoundValue bv;
            bv._kind = BoundValueKind::Deref;
            bv._schema = schema;
            bv._data = std::move(deref);
            return bv;
        }

        /**
         * Create a composite binding (TSB/TSL with child bindings)
         */
        static BoundValue make_composite(const TypeMeta* schema, std::vector<BoundValue> children) {
            BoundValue bv;
            bv._kind = BoundValueKind::Composite;
            bv._schema = schema;
            bv._data = std::move(children);
            return bv;
        }

        // Query
        [[nodiscard]] BoundValueKind kind() const { return _kind; }
        [[nodiscard]] const TypeMeta* schema() const { return _schema; }
        [[nodiscard]] bool valid() const { return _schema != nullptr; }

        /**
         * Get the current value
         *
         * For Peer: returns the source value directly
         * For Deref: returns the dereferenced target value
         * For Composite: returns the composite value (children must be accessed separately)
         */
        [[nodiscard]] ConstValueView value() const {
            switch (_kind) {
                case BoundValueKind::Peer: {
                    auto* source = std::get<TimeSeriesValue*>(_data);
                    return source ? source->value() : ConstValueView{};
                }
                case BoundValueKind::Deref: {
                    auto& deref = std::get<std::unique_ptr<DerefTimeSeriesValue>>(_data);
                    return deref ? deref->target_value() : ConstValueView{};
                }
                case BoundValueKind::Composite: {
                    // For composite, return the peer source if available
                    // (composite typically wraps a peer with per-element bindings)
                    return ConstValueView{};  // Must access via children
                }
            }
            return {};
        }

        /**
         * Unified modification tracking
         *
         * For Peer: checks if source was modified
         * For Deref: checks if ref changed OR underlying value modified
         * For Composite: checks if any child was modified
         */
        [[nodiscard]] bool modified_at(engine_time_t time) const {
            switch (_kind) {
                case BoundValueKind::Peer: {
                    auto* source = std::get<TimeSeriesValue*>(_data);
                    return source && source->modified_at(time);
                }
                case BoundValueKind::Deref: {
                    auto& deref = std::get<std::unique_ptr<DerefTimeSeriesValue>>(_data);
                    return deref && deref->modified_at(time);
                }
                case BoundValueKind::Composite: {
                    auto& children = std::get<std::vector<BoundValue>>(_data);
                    for (const auto& child : children) {
                        if (child.modified_at(time)) return true;
                    }
                    return false;
                }
            }
            return false;
        }

        /**
         * Check if this value has any valid data
         */
        [[nodiscard]] bool has_value() const {
            switch (_kind) {
                case BoundValueKind::Peer: {
                    auto* source = std::get<TimeSeriesValue*>(_data);
                    return source && source->has_value();
                }
                case BoundValueKind::Deref: {
                    auto& deref = std::get<std::unique_ptr<DerefTimeSeriesValue>>(_data);
                    return deref && deref->current_target().valid();
                }
                case BoundValueKind::Composite: {
                    auto& children = std::get<std::vector<BoundValue>>(_data);
                    for (const auto& child : children) {
                        if (child.has_value()) return true;
                    }
                    return false;
                }
            }
            return false;
        }

        // Child access for composite bindings

        [[nodiscard]] size_t child_count() const {
            if (_kind != BoundValueKind::Composite) return 0;
            return std::get<std::vector<BoundValue>>(_data).size();
        }

        [[nodiscard]] BoundValue* child(size_t index) {
            if (_kind != BoundValueKind::Composite) return nullptr;
            auto& children = std::get<std::vector<BoundValue>>(_data);
            if (index >= children.size()) return nullptr;
            return &children[index];
        }

        [[nodiscard]] const BoundValue* child(size_t index) const {
            if (_kind != BoundValueKind::Composite) return nullptr;
            auto& children = std::get<std::vector<BoundValue>>(_data);
            if (index >= children.size()) return nullptr;
            return &children[index];
        }

        // Deref-specific access

        [[nodiscard]] DerefTimeSeriesValue* deref() {
            if (_kind != BoundValueKind::Deref) return nullptr;
            return std::get<std::unique_ptr<DerefTimeSeriesValue>>(_data).get();
        }

        [[nodiscard]] const DerefTimeSeriesValue* deref() const {
            if (_kind != BoundValueKind::Deref) return nullptr;
            return std::get<std::unique_ptr<DerefTimeSeriesValue>>(_data).get();
        }

        // Peer-specific access

        [[nodiscard]] TimeSeriesValue* peer_source() {
            if (_kind != BoundValueKind::Peer) return nullptr;
            return std::get<TimeSeriesValue*>(_data);
        }

        [[nodiscard]] const TimeSeriesValue* peer_source() const {
            if (_kind != BoundValueKind::Peer) return nullptr;
            return std::get<TimeSeriesValue*>(_data);
        }

        // Lifecycle management

        /**
         * Begin evaluation cycle
         *
         * Updates all deref bindings to check for ref changes.
         */
        void begin_evaluation(engine_time_t time) {
            switch (_kind) {
                case BoundValueKind::Peer:
                    // Peer bindings don't need special handling
                    break;
                case BoundValueKind::Deref: {
                    auto& deref = std::get<std::unique_ptr<DerefTimeSeriesValue>>(_data);
                    if (deref) deref->begin_evaluation(time);
                    break;
                }
                case BoundValueKind::Composite: {
                    auto& children = std::get<std::vector<BoundValue>>(_data);
                    for (auto& child : children) {
                        child.begin_evaluation(time);
                    }
                    break;
                }
            }
        }

        /**
         * End evaluation cycle
         *
         * Clears previous targets from deref bindings.
         */
        void end_evaluation() {
            switch (_kind) {
                case BoundValueKind::Peer:
                    // Peer bindings don't need special handling
                    break;
                case BoundValueKind::Deref: {
                    auto& deref = std::get<std::unique_ptr<DerefTimeSeriesValue>>(_data);
                    if (deref) deref->end_evaluation();
                    break;
                }
                case BoundValueKind::Composite: {
                    auto& children = std::get<std::vector<BoundValue>>(_data);
                    for (auto& child : children) {
                        child.end_evaluation();
                    }
                    break;
                }
            }
        }

    private:
        BoundValueKind _kind{BoundValueKind::Peer};
        const TypeMeta* _schema{nullptr};

        // Data storage using variant
        std::variant<
            TimeSeriesValue*,                       // Peer
            std::unique_ptr<DerefTimeSeriesValue>,  // Deref
            std::vector<BoundValue>                 // Composite
        > _data;
    };

} // namespace hgraph::value

#endif // HGRAPH_VALUE_BOUND_VALUE_H
