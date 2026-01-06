//
// Created by Claude on 05/01/2025.
//

#ifndef HGRAPH_TS_TYPE_META_H
#define HGRAPH_TS_TYPE_META_H

#include <hgraph/types/value/type_meta.h>
#include <hgraph/util/date_time.h>
#include <nanobind/nanobind.h>
#include <string>
#include <vector>
#include <memory>

namespace nb = nanobind;

namespace hgraph {

    // Forward declarations
    class Node;
    struct TimeSeriesOutput;
    struct TimeSeriesInput;

    /**
     * @brief Enumeration of time-series type kinds.
     *
     * Each kind represents a different time-series semantic.
     */
    enum class TSTypeKind : uint8_t {
        TS,       ///< Simple time-series of scalar value: TS[T]
        TSB,      ///< Bundle of named time-series fields: TSB[schema]
        TSL,      ///< List of time-series elements: TSL[TS[T], Size]
        TSD,      ///< Dictionary mapping scalar keys to time-series values: TSD[K, TS[V]]
        TSS,      ///< Set time-series (set of scalar values): TSS[T]
        TSW,      ///< Sliding window over values: TSW[T, Size, MinSize]
        REF,      ///< Reference to another time-series: REF[TS[T]]
        SIGNAL,   ///< Signal (tick with no value)
    };

    /**
     * @brief Field information for TSB (bundle) types.
     */
    struct TSBFieldInfo {
        std::string name;           ///< Field name
        size_t index;               ///< Position (0-based)
        const struct TSMeta* type;  ///< Time-series type of this field
    };

    /**
     * @brief Base class for time-series type metadata.
     *
     * TSMeta describes the time-series structure overlaid on the value schema.
     * Each TSMeta node is a point where:
     * - Modification can be independently tracked
     * - Observers can subscribe for notifications
     *
     * The value_schema() returns the underlying TypeMeta for data storage.
     */
    struct TSMeta {
        using s_ptr = std::shared_ptr<const TSMeta>;

        virtual ~TSMeta() = default;

        /**
         * @brief Get the kind of this time-series type.
         */
        [[nodiscard]] virtual TSTypeKind kind() const = 0;

        /**
         * @brief Get the underlying value schema for data storage.
         *
         * Returns nullptr for SIGNAL (no value).
         */
        [[nodiscard]] virtual const value::TypeMeta* value_schema() const = 0;

        /**
         * @brief Check if this is a scalar time-series (TS[T]).
         */
        [[nodiscard]] bool is_scalar_ts() const { return kind() == TSTypeKind::TS; }

        /**
         * @brief Check if this is a bundle (TSB).
         */
        [[nodiscard]] bool is_bundle() const { return kind() == TSTypeKind::TSB; }

        /**
         * @brief Check if this is a collection type (TSL, TSD, TSS).
         */
        [[nodiscard]] bool is_collection() const {
            auto k = kind();
            return k == TSTypeKind::TSL || k == TSTypeKind::TSD || k == TSTypeKind::TSS;
        }

        /**
         * @brief Check if this is a reference type (REF).
         */
        [[nodiscard]] bool is_reference() const { return kind() == TSTypeKind::REF; }

        /**
         * @brief Check if this is a signal (no value).
         */
        [[nodiscard]] bool is_signal() const { return kind() == TSTypeKind::SIGNAL; }

        // Factory methods - implemented by subclasses
        // These will be used by builders to create instances

        /**
         * @brief String representation for debugging.
         */
        [[nodiscard]] virtual std::string to_string() const = 0;

    protected:
        TSMeta() = default;
    };

    /**
     * @brief Time-series of a scalar value: TS[T]
     */
    struct TSValueMeta final : TSMeta {
        explicit TSValueMeta(const value::TypeMeta* scalar_schema)
            : _scalar_schema(scalar_schema) {}

        [[nodiscard]] TSTypeKind kind() const override { return TSTypeKind::TS; }
        [[nodiscard]] const value::TypeMeta* value_schema() const override { return _scalar_schema; }
        [[nodiscard]] std::string to_string() const override;

        /**
         * @brief Get the scalar type (same as value_schema for TS).
         */
        [[nodiscard]] const value::TypeMeta* scalar_schema() const { return _scalar_schema; }

    private:
        const value::TypeMeta* _scalar_schema;
    };

    /**
     * @brief Bundle of named time-series fields: TSB[schema]
     */
    struct TSBTypeMeta final : TSMeta {
        TSBTypeMeta(std::vector<TSBFieldInfo> fields,
                    const value::TypeMeta* bundle_schema,
                    std::string name = "");

        [[nodiscard]] TSTypeKind kind() const override { return TSTypeKind::TSB; }
        [[nodiscard]] const value::TypeMeta* value_schema() const override { return _bundle_schema; }
        [[nodiscard]] std::string to_string() const override;

        /**
         * @brief Get the bundle's name (may be empty for anonymous bundles).
         */
        [[nodiscard]] const std::string& name() const { return _name; }

        /**
         * @brief Get the number of fields.
         */
        [[nodiscard]] size_t field_count() const { return _fields.size(); }

        /**
         * @brief Get field info by index.
         */
        [[nodiscard]] const TSBFieldInfo& field(size_t index) const { return _fields[index]; }

        /**
         * @brief Get field info by name.
         * @returns nullptr if field not found.
         */
        [[nodiscard]] const TSBFieldInfo* field(const std::string& name) const;

        /**
         * @brief Get the TSMeta for a field by index.
         */
        [[nodiscard]] const TSMeta* field_meta(size_t index) const {
            return _fields[index].type;
        }

        /**
         * @brief Get the TSMeta for a field by name.
         */
        [[nodiscard]] const TSMeta* field_meta(const std::string& name) const {
            auto* f = field(name);
            return f ? f->type : nullptr;
        }

        /**
         * @brief Get all fields.
         */
        [[nodiscard]] const std::vector<TSBFieldInfo>& fields() const { return _fields; }

    private:
        std::vector<TSBFieldInfo> _fields;
        const value::TypeMeta* _bundle_schema;
        std::string _name;
    };

    /**
     * @brief List of time-series elements: TSL[TS[T], Size]
     */
    struct TSLTypeMeta final : TSMeta {
        TSLTypeMeta(const TSMeta* element_type, size_t fixed_size,
                    const value::TypeMeta* list_schema)
            : _element_type(element_type), _fixed_size(fixed_size),
              _list_schema(list_schema) {}

        [[nodiscard]] TSTypeKind kind() const override { return TSTypeKind::TSL; }
        [[nodiscard]] const value::TypeMeta* value_schema() const override { return _list_schema; }
        [[nodiscard]] std::string to_string() const override;

        /**
         * @brief Get the element's time-series type.
         */
        [[nodiscard]] const TSMeta* element_type() const { return _element_type; }

        /**
         * @brief Get the fixed size (0 = dynamic).
         */
        [[nodiscard]] size_t fixed_size() const { return _fixed_size; }

        /**
         * @brief Check if this is a fixed-size list.
         */
        [[nodiscard]] bool is_fixed_size() const { return _fixed_size > 0; }

    private:
        const TSMeta* _element_type;
        size_t _fixed_size;
        const value::TypeMeta* _list_schema;
    };

    /**
     * @brief Dictionary mapping scalar keys to time-series values: TSD[K, TS[V]]
     */
    struct TSDTypeMeta final : TSMeta {
        TSDTypeMeta(const value::TypeMeta* key_type, const TSMeta* value_type,
                    const value::TypeMeta* dict_schema)
            : _key_type(key_type), _value_type(value_type),
              _dict_schema(dict_schema) {}

        [[nodiscard]] TSTypeKind kind() const override { return TSTypeKind::TSD; }
        [[nodiscard]] const value::TypeMeta* value_schema() const override { return _dict_schema; }
        [[nodiscard]] std::string to_string() const override;

        /**
         * @brief Get the key's scalar type.
         */
        [[nodiscard]] const value::TypeMeta* key_type() const { return _key_type; }

        /**
         * @brief Get the value's time-series type.
         */
        [[nodiscard]] const TSMeta* value_ts_type() const { return _value_type; }

    private:
        const value::TypeMeta* _key_type;
        const TSMeta* _value_type;
        const value::TypeMeta* _dict_schema;
    };

    /**
     * @brief Set time-series (set of scalar values): TSS[T]
     */
    struct TSSTypeMeta final : TSMeta {
        TSSTypeMeta(const value::TypeMeta* element_type, const value::TypeMeta* set_schema)
            : _element_type(element_type), _set_schema(set_schema) {}

        [[nodiscard]] TSTypeKind kind() const override { return TSTypeKind::TSS; }
        [[nodiscard]] const value::TypeMeta* value_schema() const override { return _set_schema; }
        [[nodiscard]] std::string to_string() const override;

        /**
         * @brief Get the element's scalar type.
         */
        [[nodiscard]] const value::TypeMeta* element_type() const { return _element_type; }

    private:
        const value::TypeMeta* _element_type;
        const value::TypeMeta* _set_schema;
    };

    /**
     * @brief Sliding window over values: TSW[T, Size, MinSize]
     *
     * Supports two window types:
     * - Size-based: Window holds a fixed number of ticks
     * - Duration-based: Window holds values within a time duration
     */
    struct TSWTypeMeta final : TSMeta {
        /**
         * @brief Construct a size-based (tick count) window.
         */
        TSWTypeMeta(const value::TypeMeta* value_type, size_t size, size_t min_size,
                    const value::TypeMeta* window_schema)
            : _value_type(value_type), _size(size), _min_size(min_size),
              _time_range(), _min_time_range(), _is_time_based(false),
              _window_schema(window_schema) {}

        /**
         * @brief Construct a duration-based (timedelta) window.
         */
        TSWTypeMeta(const value::TypeMeta* value_type, engine_time_delta_t time_range, engine_time_delta_t min_time_range,
                    const value::TypeMeta* window_schema, bool /* time_based_tag */)
            : _value_type(value_type), _size(0), _min_size(0),
              _time_range(time_range), _min_time_range(min_time_range), _is_time_based(true),
              _window_schema(window_schema) {}

        [[nodiscard]] TSTypeKind kind() const override { return TSTypeKind::TSW; }
        [[nodiscard]] const value::TypeMeta* value_schema() const override { return _window_schema; }
        [[nodiscard]] std::string to_string() const override;

        /**
         * @brief Get the value's scalar type.
         */
        [[nodiscard]] const value::TypeMeta* element_type() const { return _value_type; }

        /**
         * @brief Check if this is a time-based (duration) window.
         */
        [[nodiscard]] bool is_time_based() const { return _is_time_based; }

        /**
         * @brief Get the window size (tick count). Only valid if !is_time_based().
         */
        [[nodiscard]] size_t size() const { return _size; }

        /**
         * @brief Get the minimum size before window is valid (tick count). Only valid if !is_time_based().
         */
        [[nodiscard]] size_t min_size() const { return _min_size; }

        /**
         * @brief Get the time range duration. Only valid if is_time_based().
         */
        [[nodiscard]] engine_time_delta_t time_range() const { return _time_range; }

        /**
         * @brief Get the minimum time range duration. Only valid if is_time_based().
         */
        [[nodiscard]] engine_time_delta_t min_time_range() const { return _min_time_range; }

    private:
        const value::TypeMeta* _value_type;
        size_t _size;                       // For size-based windows
        size_t _min_size;                   // For size-based windows
        engine_time_delta_t _time_range;    // For duration-based windows
        engine_time_delta_t _min_time_range; // For duration-based windows
        bool _is_time_based;
        const value::TypeMeta* _window_schema;
    };

    /**
     * @brief Reference to another time-series: REF[TS[T]]
     */
    struct REFTypeMeta final : TSMeta {
        explicit REFTypeMeta(const TSMeta* referenced_type)
            : _referenced_type(referenced_type) {}

        [[nodiscard]] TSTypeKind kind() const override { return TSTypeKind::REF; }
        [[nodiscard]] const value::TypeMeta* value_schema() const override;
        [[nodiscard]] std::string to_string() const override;

        /**
         * @brief Get the referenced time-series type.
         */
        [[nodiscard]] const TSMeta* referenced_type() const { return _referenced_type; }

    private:
        const TSMeta* _referenced_type;
    };

    /**
     * @brief Signal (tick with no value).
     */
    struct SignalTypeMeta final : TSMeta {
        SignalTypeMeta() = default;

        [[nodiscard]] TSTypeKind kind() const override { return TSTypeKind::SIGNAL; }
        [[nodiscard]] const value::TypeMeta* value_schema() const override { return nullptr; }
        [[nodiscard]] std::string to_string() const override { return "SIGNAL"; }
    };

    /**
     * @brief Register TSMeta types with nanobind.
     */
    void register_ts_type_meta_with_nanobind(nb::module_& m);

} // namespace hgraph

#endif // HGRAPH_TS_TYPE_META_H
