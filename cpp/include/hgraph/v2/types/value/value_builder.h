#ifndef HGRAPH_CPP_ROOT_VALUE_BUILDER_H
#define HGRAPH_CPP_ROOT_VALUE_BUILDER_H

#include <hgraph/v2/types/metadata/type_registry.h>
#include <hgraph/v2/types/value/container_storage.h>
#include <hgraph/v2/types/value/value_builder_ops.h>

#include <compare>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <vector>

namespace hgraph::v2
{
    struct Value;
    struct ValueView;

    /**
     * Cached binder from a v2 value schema to owning storage and lightweight view behavior.
     *
     * A builder is the one object that ties together:
     * - the interned `ValueTypeBinding` used by non-owning `ValueView`
     * - the bound storage plan used by owning `Value`
     */
    struct ValueBuilder
    {
        ValueBuilder() = default;

        explicit ValueBuilder(ValueBuilderOps ops) : m_ops(ops) {
            if (!m_ops.valid()) { throw std::logic_error("ValueBuilder requires a bound type binding"); }
        }

        [[nodiscard]] const ValueTypeBinding  *binding() const noexcept { return m_ops.binding; }
        [[nodiscard]] const ValueTypeMetaData *type() const noexcept { return m_ops.binding ? m_ops.binding->type_meta : nullptr; }
        [[nodiscard]] const MemoryUtils::StoragePlan *plan() const noexcept {
            return m_ops.binding ? m_ops.binding->plan() : nullptr;
        }
        [[nodiscard]] const MemoryUtils::LifecycleOps *lifecycle() const noexcept {
            return m_ops.binding ? m_ops.binding->lifecycle() : nullptr;
        }
        [[nodiscard]] const ValueOps *ops() const noexcept { return m_ops.binding ? m_ops.binding->ops : nullptr; }

        [[nodiscard]] const ValueTypeMetaData &checked_type() const {
            if (const ValueTypeMetaData *value_type = type(); value_type != nullptr) { return *value_type; }
            throw std::logic_error("ValueBuilder is not bound to a value type");
        }

        [[nodiscard]] const MemoryUtils::StoragePlan  &checked_plan() const { return checked_binding().checked_plan(); }
        [[nodiscard]] const ValueTypeBinding          &checked_binding() const { return m_ops.type_binding(); }
        [[nodiscard]] const MemoryUtils::LifecycleOps &checked_lifecycle() const { return checked_binding().checked_lifecycle(); }
        [[nodiscard]] const ValueOps                  &checked_ops() const { return checked_binding().checked_ops(); }

        [[nodiscard]] static const ValueBuilder *find(const ValueTypeMetaData *type);
        [[nodiscard]] static const ValueBuilder &checked(const ValueTypeMetaData *type);

      private:
        ValueBuilderOps m_ops{};
    };

    namespace detail
    {
        template <typename T> using remove_cvref_t = std::remove_cv_t<std::remove_reference_t<T>>;

        template <typename T> struct ScalarTypeName
        {
            static constexpr std::string_view value{};
        };

        template <> struct ScalarTypeName<bool>
        {
            static constexpr std::string_view value{"bool"};
        };
        template <> struct ScalarTypeName<char>
        {
            static constexpr std::string_view value{"char"};
        };
        template <> struct ScalarTypeName<signed char>
        {
            static constexpr std::string_view value{"signed char"};
        };
        template <> struct ScalarTypeName<unsigned char>
        {
            static constexpr std::string_view value{"unsigned char"};
        };
        template <> struct ScalarTypeName<short>
        {
            static constexpr std::string_view value{"short"};
        };
        template <> struct ScalarTypeName<unsigned short>
        {
            static constexpr std::string_view value{"unsigned short"};
        };
        template <> struct ScalarTypeName<int>
        {
            static constexpr std::string_view value{"int"};
        };
        template <> struct ScalarTypeName<unsigned int>
        {
            static constexpr std::string_view value{"unsigned int"};
        };
        template <> struct ScalarTypeName<long>
        {
            static constexpr std::string_view value{"long"};
        };
        template <> struct ScalarTypeName<unsigned long>
        {
            static constexpr std::string_view value{"unsigned long"};
        };
        template <> struct ScalarTypeName<long long>
        {
            static constexpr std::string_view value{"long long"};
        };
        template <> struct ScalarTypeName<unsigned long long>
        {
            static constexpr std::string_view value{"unsigned long long"};
        };
        template <> struct ScalarTypeName<float>
        {
            static constexpr std::string_view value{"float"};
        };
        template <> struct ScalarTypeName<double>
        {
            static constexpr std::string_view value{"double"};
        };
        template <> struct ScalarTypeName<long double>
        {
            static constexpr std::string_view value{"long double"};
        };
        template <> struct ScalarTypeName<std::string>
        {
            static constexpr std::string_view value{"string"};
        };

        class ValueBuilderRegistry
        {
          public:
            [[nodiscard]] const ValueBuilder *find(const ValueTypeMetaData *type) const noexcept {
                if (type == nullptr) { return nullptr; }

                std::lock_guard<std::mutex> lock(m_mutex);
                const auto                  it = m_builders.find(type);
                return it == m_builders.end() ? nullptr : it->second;
            }

            [[nodiscard]] const ValueBuilder &store_if_absent(const ValueTypeMetaData &type, ValueBuilderOps ops) {
                if (const ValueBuilder *builder = find(&type); builder != nullptr) { return *builder; }

                auto                builder = std::make_unique<ValueBuilder>(ops);
                const ValueBuilder *raw     = builder.get();

                std::lock_guard<std::mutex> lock(m_mutex);
                if (const auto it = m_builders.find(&type); it != m_builders.end()) { return *it->second; }

                m_storage.push_back(std::move(builder));
                m_builders.emplace(&type, raw);
                return *raw;
            }

          private:
            mutable std::mutex                                                  m_mutex{};
            std::unordered_map<const ValueTypeMetaData *, const ValueBuilder *> m_builders{};
            std::vector<std::unique_ptr<ValueBuilder>>                          m_storage{};
        };

        [[nodiscard]] inline ValueBuilderRegistry &value_builder_registry() noexcept {
            static ValueBuilderRegistry registry;
            return registry;
        }

        [[nodiscard]] inline const ValueTypeBinding &checked_child_binding(const ValueTypeMetaData *type) {
            return ValueBuilder::checked(type).checked_binding();
        }

        [[nodiscard]] inline size_t hash_child_value(const void *data, const ValueTypeMetaData *type) {
            const ValueTypeBinding &binding = checked_child_binding(type);
            return binding.checked_ops().hash_of(data, binding);
        }

        [[nodiscard]] inline bool equal_child_value(const void *lhs, const void *rhs, const ValueTypeMetaData *type) {
            const ValueTypeBinding &binding = checked_child_binding(type);
            return binding.checked_ops().equals_of(lhs, rhs, binding);
        }

        [[nodiscard]] inline std::partial_ordering compare_child_value(const void *lhs, const void *rhs,
                                                                       const ValueTypeMetaData *type) {
            const ValueTypeBinding &binding = checked_child_binding(type);
            return binding.checked_ops().compare_of(lhs, rhs, binding);
        }

        [[nodiscard]] inline std::string child_to_string(const void *data, const ValueTypeMetaData *type) {
            const ValueTypeBinding &binding = checked_child_binding(type);
            return binding.checked_ops().to_string_of(data, binding);
        }

        [[nodiscard]] inline size_t tuple_hash(const void *data, const ValueTypeBinding &binding) {
            const ValueTypeMetaData &type = binding.checked_type();
            size_t                   seed = 0;
            for (size_t index = 0; index < type.field_count; ++index) {
                const auto &field = type.fields[index];
                seed              = combine_hash(seed, hash_child_value(MemoryUtils::advance(data, field.offset), field.type));
            }
            return seed;
        }

        [[nodiscard]] inline bool tuple_equals(const void *lhs, const void *rhs, const ValueTypeBinding &binding) {
            const ValueTypeMetaData &type = binding.checked_type();
            for (size_t index = 0; index < type.field_count; ++index) {
                const auto &field = type.fields[index];
                if (!equal_child_value(MemoryUtils::advance(lhs, field.offset), MemoryUtils::advance(rhs, field.offset),
                                       field.type)) {
                    return false;
                }
            }
            return true;
        }

        [[nodiscard]] inline std::partial_ordering tuple_compare(const void *lhs, const void *rhs,
                                                                 const ValueTypeBinding &binding) {
            const ValueTypeMetaData &type = binding.checked_type();
            for (size_t index = 0; index < type.field_count; ++index) {
                const auto &field = type.fields[index];
                const auto  order = compare_child_value(MemoryUtils::advance(lhs, field.offset),
                                                        MemoryUtils::advance(rhs, field.offset), field.type);
                if (order == std::partial_ordering::unordered) { return order; }
                if (order != std::partial_ordering::equivalent) { return order; }
            }
            return std::partial_ordering::equivalent;
        }

        [[nodiscard]] inline std::string tuple_to_string(const void *data, const ValueTypeBinding &binding) {
            const ValueTypeMetaData &type = binding.checked_type();
            std::ostringstream       out;
            out << '(';
            for (size_t index = 0; index < type.field_count; ++index) {
                if (index != 0) { out << ", "; }
                const auto &field = type.fields[index];
                out << child_to_string(MemoryUtils::advance(data, field.offset), field.type);
            }
            out << ')';
            return out.str();
        }

        [[nodiscard]] inline size_t bundle_hash(const void *data, const ValueTypeBinding &binding) {
            return tuple_hash(data, binding);
        }

        [[nodiscard]] inline bool bundle_equals(const void *lhs, const void *rhs, const ValueTypeBinding &binding) {
            return tuple_equals(lhs, rhs, binding);
        }

        [[nodiscard]] inline std::partial_ordering bundle_compare(const void *lhs, const void *rhs,
                                                                  const ValueTypeBinding &binding) {
            return tuple_compare(lhs, rhs, binding);
        }

        [[nodiscard]] inline std::string bundle_to_string(const void *data, const ValueTypeBinding &binding) {
            const ValueTypeMetaData &type = binding.checked_type();
            std::ostringstream       out;
            out << (type.display_name != nullptr ? type.display_name : "bundle") << '{';
            for (size_t index = 0; index < type.field_count; ++index) {
                if (index != 0) { out << ", "; }
                const auto &field = type.fields[index];
                out << (field.name != nullptr ? field.name : "?") << ": "
                    << child_to_string(MemoryUtils::advance(data, field.offset), field.type);
            }
            out << '}';
            return out.str();
        }

        [[nodiscard]] inline size_t list_size(const void *data, const ValueTypeBinding &binding) {
            const ValueTypeMetaData &type = binding.checked_type();
            if (type.fixed_size != 0) { return type.fixed_size; }
            return static_cast<const DynamicListStorage *>(data)->size;
        }

        [[nodiscard]] inline const void *list_element_memory(const void *data, size_t index, const ValueTypeBinding &binding) {
            const ValueTypeMetaData &type = binding.checked_type();
            if (type.fixed_size != 0) { return MemoryUtils::advance(data, binding.checked_plan().element_offset(index)); }
            const auto *storage = static_cast<const DynamicListStorage *>(data);
            if (index >= storage->size) { throw std::out_of_range("List index out of range"); }
            return storage->values.value_memory(index);
        }

        [[nodiscard]] inline size_t list_hash(const void *data, const ValueTypeBinding &binding) {
            const auto  *element_type = binding.checked_type().element_type;
            size_t       seed         = 0;
            const size_t count        = list_size(data, binding);
            for (size_t index = 0; index < count; ++index) {
                seed = combine_hash(seed, hash_child_value(list_element_memory(data, index, binding), element_type));
            }
            return seed;
        }

        [[nodiscard]] inline bool list_equals(const void *lhs, const void *rhs, const ValueTypeBinding &binding) {
            const auto  *element_type = binding.checked_type().element_type;
            const size_t count        = list_size(lhs, binding);
            if (count != list_size(rhs, binding)) { return false; }
            for (size_t index = 0; index < count; ++index) {
                if (!equal_child_value(list_element_memory(lhs, index, binding), list_element_memory(rhs, index, binding),
                                       element_type)) {
                    return false;
                }
            }
            return true;
        }

        [[nodiscard]] inline std::partial_ordering list_compare(const void *lhs, const void *rhs, const ValueTypeBinding &binding) {
            const auto  *element_type = binding.checked_type().element_type;
            const size_t lhs_size     = list_size(lhs, binding);
            const size_t rhs_size     = list_size(rhs, binding);
            const size_t shared       = std::min(lhs_size, rhs_size);
            for (size_t index = 0; index < shared; ++index) {
                const auto order = compare_child_value(list_element_memory(lhs, index, binding),
                                                       list_element_memory(rhs, index, binding), element_type);
                if (order == std::partial_ordering::unordered) { return order; }
                if (order != std::partial_ordering::equivalent) { return order; }
            }
            if (lhs_size == rhs_size) { return std::partial_ordering::equivalent; }
            return lhs_size < rhs_size ? std::partial_ordering::less : std::partial_ordering::greater;
        }

        [[nodiscard]] inline std::string list_to_string(const void *data, const ValueTypeBinding &binding) {
            const auto        *element_type = binding.checked_type().element_type;
            const size_t       count        = list_size(data, binding);
            std::ostringstream out;
            out << '[';
            for (size_t index = 0; index < count; ++index) {
                if (index != 0) { out << ", "; }
                out << child_to_string(list_element_memory(data, index, binding), element_type);
            }
            out << ']';
            return out.str();
        }

        [[nodiscard]] inline size_t set_hash(const void *data, const ValueTypeBinding &binding) {
            const auto &storage      = *static_cast<const SetStorage *>(data);
            const auto *element_type = binding.checked_type().element_type;
            size_t      seed         = 0;
            for_each_live_slot(storage.keys,
                               [&](size_t slot) { seed += combine_hash(0, hash_child_value(storage.keys[slot], element_type)); });
            return seed;
        }

        [[nodiscard]] inline bool set_equals(const void *lhs, const void *rhs, const ValueTypeBinding &binding) {
            const auto &lhs_storage = *static_cast<const SetStorage *>(lhs);
            const auto &rhs_storage = *static_cast<const SetStorage *>(rhs);
            if (lhs_storage.keys.size() != rhs_storage.keys.size()) { return false; }
            bool all_present = true;
            for_each_live_slot(lhs_storage.keys, [&](size_t slot) {
                if (all_present) { all_present = rhs_storage.keys.contains(lhs_storage.keys[slot]); }
            });
            return all_present;
        }

        [[nodiscard]] inline std::partial_ordering set_compare(const void *lhs, const void *rhs, const ValueTypeBinding &binding) {
            return set_equals(lhs, rhs, binding) ? std::partial_ordering::equivalent : std::partial_ordering::unordered;
        }

        [[nodiscard]] inline std::string set_to_string(const void *data, const ValueTypeBinding &binding) {
            const auto        &storage      = *static_cast<const SetStorage *>(data);
            const auto        *element_type = binding.checked_type().element_type;
            std::ostringstream out;
            out << '{';
            bool first = true;
            for_each_live_slot(storage.keys, [&](size_t slot) {
                if (!first) { out << ", "; }
                first = false;
                out << child_to_string(storage.keys[slot], element_type);
            });
            out << '}';
            return out.str();
        }

        [[nodiscard]] inline size_t map_hash(const void *data, const ValueTypeBinding &binding) {
            const auto &storage = *static_cast<const MapStorage *>(data);
            const auto &type    = binding.checked_type();
            size_t      seed    = 0;
            for_each_live_slot(storage.keys, [&](size_t slot) {
                const size_t key_hash   = hash_child_value(storage.keys[slot], type.key_type);
                const size_t value_hash = hash_child_value(storage.values.value_memory(slot), type.element_type);
                seed += combine_hash(key_hash, value_hash);
            });
            return seed;
        }

        [[nodiscard]] inline bool map_equals(const void *lhs, const void *rhs, const ValueTypeBinding &binding) {
            const auto &lhs_storage = *static_cast<const MapStorage *>(lhs);
            const auto &rhs_storage = *static_cast<const MapStorage *>(rhs);
            const auto &type        = binding.checked_type();
            if (lhs_storage.keys.size() != rhs_storage.keys.size()) { return false; }

            bool equal = true;
            for_each_live_slot(lhs_storage.keys, [&](size_t slot) {
                if (!equal) { return; }
                const size_t rhs_slot = rhs_storage.keys.find_slot(lhs_storage.keys[slot]);
                if (rhs_slot == KeySlotStore::npos) {
                    equal = false;
                    return;
                }
                equal = equal_child_value(lhs_storage.values.value_memory(slot), rhs_storage.values.value_memory(rhs_slot),
                                          type.element_type);
            });
            return equal;
        }

        [[nodiscard]] inline std::partial_ordering map_compare(const void *lhs, const void *rhs, const ValueTypeBinding &binding) {
            return map_equals(lhs, rhs, binding) ? std::partial_ordering::equivalent : std::partial_ordering::unordered;
        }

        [[nodiscard]] inline std::string map_to_string(const void *data, const ValueTypeBinding &binding) {
            const auto        &storage = *static_cast<const MapStorage *>(data);
            const auto        &type    = binding.checked_type();
            std::ostringstream out;
            out << '{';
            bool first = true;
            for_each_live_slot(storage.keys, [&](size_t slot) {
                if (!first) { out << ", "; }
                first = false;
                out << child_to_string(storage.keys[slot], type.key_type) << ": "
                    << child_to_string(storage.values.value_memory(slot), type.element_type);
            });
            out << '}';
            return out.str();
        }

        [[nodiscard]] inline size_t cyclic_buffer_hash(const void *data, const ValueTypeBinding &binding) {
            const auto &storage      = *static_cast<const CyclicBufferStorage *>(data);
            const auto *element_type = binding.checked_type().element_type;
            size_t      seed         = 0;
            for (size_t index = 0; index < storage.size; ++index) {
                seed =
                    combine_hash(seed, hash_child_value(storage.values.value_memory(storage.slot_for_index(index)), element_type));
            }
            return seed;
        }

        [[nodiscard]] inline bool cyclic_buffer_equals(const void *lhs, const void *rhs, const ValueTypeBinding &binding) {
            const auto &lhs_storage  = *static_cast<const CyclicBufferStorage *>(lhs);
            const auto &rhs_storage  = *static_cast<const CyclicBufferStorage *>(rhs);
            const auto *element_type = binding.checked_type().element_type;
            if (lhs_storage.size != rhs_storage.size) { return false; }
            for (size_t index = 0; index < lhs_storage.size; ++index) {
                if (!equal_child_value(lhs_storage.values.value_memory(lhs_storage.slot_for_index(index)),
                                       rhs_storage.values.value_memory(rhs_storage.slot_for_index(index)), element_type)) {
                    return false;
                }
            }
            return true;
        }

        [[nodiscard]] inline std::partial_ordering cyclic_buffer_compare(const void *lhs, const void *rhs,
                                                                         const ValueTypeBinding &binding) {
            const auto  &lhs_storage  = *static_cast<const CyclicBufferStorage *>(lhs);
            const auto  &rhs_storage  = *static_cast<const CyclicBufferStorage *>(rhs);
            const auto  *element_type = binding.checked_type().element_type;
            const size_t shared       = std::min(lhs_storage.size, rhs_storage.size);
            for (size_t index = 0; index < shared; ++index) {
                const auto order =
                    compare_child_value(lhs_storage.values.value_memory(lhs_storage.slot_for_index(index)),
                                        rhs_storage.values.value_memory(rhs_storage.slot_for_index(index)), element_type);
                if (order == std::partial_ordering::unordered) { return order; }
                if (order != std::partial_ordering::equivalent) { return order; }
            }
            if (lhs_storage.size == rhs_storage.size) { return std::partial_ordering::equivalent; }
            return lhs_storage.size < rhs_storage.size ? std::partial_ordering::less : std::partial_ordering::greater;
        }

        [[nodiscard]] inline std::string cyclic_buffer_to_string(const void *data, const ValueTypeBinding &binding) {
            const auto        &storage      = *static_cast<const CyclicBufferStorage *>(data);
            const auto        *element_type = binding.checked_type().element_type;
            std::ostringstream out;
            out << "CyclicBuffer[";
            for (size_t index = 0; index < storage.size; ++index) {
                if (index != 0) { out << ", "; }
                out << child_to_string(storage.values.value_memory(storage.slot_for_index(index)), element_type);
            }
            out << ']';
            return out.str();
        }

        [[nodiscard]] inline size_t queue_hash(const void *data, const ValueTypeBinding &binding) {
            const auto &storage      = *static_cast<const QueueStorage *>(data);
            const auto *element_type = binding.checked_type().element_type;
            size_t      seed         = 0;
            for (const size_t slot : storage.order) {
                seed = combine_hash(seed, hash_child_value(storage.values.value_memory(slot), element_type));
            }
            return seed;
        }

        [[nodiscard]] inline bool queue_equals(const void *lhs, const void *rhs, const ValueTypeBinding &binding) {
            const auto &lhs_storage  = *static_cast<const QueueStorage *>(lhs);
            const auto &rhs_storage  = *static_cast<const QueueStorage *>(rhs);
            const auto *element_type = binding.checked_type().element_type;
            if (lhs_storage.order.size() != rhs_storage.order.size()) { return false; }
            for (size_t index = 0; index < lhs_storage.order.size(); ++index) {
                if (!equal_child_value(lhs_storage.values.value_memory(lhs_storage.order[index]),
                                       rhs_storage.values.value_memory(rhs_storage.order[index]), element_type)) {
                    return false;
                }
            }
            return true;
        }

        [[nodiscard]] inline std::partial_ordering queue_compare(const void *lhs, const void *rhs,
                                                                 const ValueTypeBinding &binding) {
            const auto  &lhs_storage  = *static_cast<const QueueStorage *>(lhs);
            const auto  &rhs_storage  = *static_cast<const QueueStorage *>(rhs);
            const auto  *element_type = binding.checked_type().element_type;
            const size_t shared       = std::min(lhs_storage.order.size(), rhs_storage.order.size());
            for (size_t index = 0; index < shared; ++index) {
                const auto order = compare_child_value(lhs_storage.values.value_memory(lhs_storage.order[index]),
                                                       rhs_storage.values.value_memory(rhs_storage.order[index]), element_type);
                if (order == std::partial_ordering::unordered) { return order; }
                if (order != std::partial_ordering::equivalent) { return order; }
            }
            if (lhs_storage.order.size() == rhs_storage.order.size()) { return std::partial_ordering::equivalent; }
            return lhs_storage.order.size() < rhs_storage.order.size() ? std::partial_ordering::less
                                                                       : std::partial_ordering::greater;
        }

        [[nodiscard]] inline std::string queue_to_string(const void *data, const ValueTypeBinding &binding) {
            const auto        &storage      = *static_cast<const QueueStorage *>(data);
            const auto        *element_type = binding.checked_type().element_type;
            std::ostringstream out;
            out << "Queue[";
            for (size_t index = 0; index < storage.order.size(); ++index) {
                if (index != 0) { out << ", "; }
                out << child_to_string(storage.values.value_memory(storage.order[index]), element_type);
            }
            out << ']';
            return out.str();
        }

        [[nodiscard]] inline const ValueOps &tuple_value_ops() noexcept {
            static const ValueOps ops{
                .hash      = &tuple_hash,
                .equals    = &tuple_equals,
                .compare   = &tuple_compare,
                .to_string = &tuple_to_string,
            };
            return ops;
        }

        [[nodiscard]] inline const ValueOps &bundle_value_ops() noexcept {
            static const ValueOps ops{
                .hash      = &bundle_hash,
                .equals    = &bundle_equals,
                .compare   = &bundle_compare,
                .to_string = &bundle_to_string,
            };
            return ops;
        }

        [[nodiscard]] inline const ValueOps &list_value_ops() noexcept {
            static const ValueOps ops{
                .hash      = &list_hash,
                .equals    = &list_equals,
                .compare   = &list_compare,
                .to_string = &list_to_string,
            };
            return ops;
        }

        [[nodiscard]] inline const ValueOps &set_value_ops() noexcept {
            static const ValueOps ops{
                .hash      = &set_hash,
                .equals    = &set_equals,
                .compare   = &set_compare,
                .to_string = &set_to_string,
            };
            return ops;
        }

        [[nodiscard]] inline const ValueOps &map_value_ops() noexcept {
            static const ValueOps ops{
                .hash      = &map_hash,
                .equals    = &map_equals,
                .compare   = &map_compare,
                .to_string = &map_to_string,
            };
            return ops;
        }

        [[nodiscard]] inline const ValueOps &cyclic_buffer_value_ops() noexcept {
            static const ValueOps ops{
                .hash      = &cyclic_buffer_hash,
                .equals    = &cyclic_buffer_equals,
                .compare   = &cyclic_buffer_compare,
                .to_string = &cyclic_buffer_to_string,
            };
            return ops;
        }

        [[nodiscard]] inline const ValueOps &queue_value_ops() noexcept {
            static const ValueOps ops{
                .hash      = &queue_hash,
                .equals    = &queue_equals,
                .compare   = &queue_compare,
                .to_string = &queue_to_string,
            };
            return ops;
        }

        [[nodiscard]] inline const MemoryUtils::StoragePlan &make_tuple_plan(const ValueTypeMetaData &type) {
            auto builder = MemoryUtils::tuple();
            builder.reserve(type.field_count);
            for (size_t index = 0; index < type.field_count; ++index) {
                builder.add_plan(ValueBuilder::checked(type.fields[index].type).checked_plan());
            }
            return builder.build();
        }

        [[nodiscard]] inline const MemoryUtils::StoragePlan &make_bundle_plan(const ValueTypeMetaData &type) {
            // Bundles are stored with the same field-name-preserving layout as
            // MemoryUtils named tuples, while still exposing Bundle as the
            // user-facing value kind.
            auto builder = MemoryUtils::named_tuple();
            builder.reserve(type.field_count);
            for (size_t index = 0; index < type.field_count; ++index) {
                const auto &field = type.fields[index];
                builder.add_field(field.name != nullptr ? field.name : "", ValueBuilder::checked(field.type).checked_plan());
            }
            return builder.build();
        }

        [[nodiscard]] inline const ValueBuilder &
        register_scalar_binding(const ValueTypeMetaData &type, const MemoryUtils::StoragePlan &plan, const ValueOps &ops) {
            const ValueTypeBinding &binding = ValueTypeBinding::intern(type, plan, ops);
            return value_builder_registry().store_if_absent(type, ValueBuilderOps{.binding = &binding});
        }

        [[nodiscard]] inline const ValueBuilder &register_value_builder(const ValueTypeMetaData &type) {
            if (const ValueBuilder *builder = value_builder_registry().find(&type); builder != nullptr) { return *builder; }

            switch (type.kind) {
                case ValueTypeKind::Tuple:
                    {
                        const auto             &plan    = make_tuple_plan(type);
                        const ValueTypeBinding &binding = ValueTypeBinding::intern(type, plan, tuple_value_ops());
                        return value_builder_registry().store_if_absent(type, ValueBuilderOps{.binding = &binding});
                    }
                case ValueTypeKind::Bundle:
                    {
                        const auto             &plan    = make_bundle_plan(type);
                        const ValueTypeBinding &binding = ValueTypeBinding::intern(type, plan, bundle_value_ops());
                        return value_builder_registry().store_if_absent(type, ValueBuilderOps{.binding = &binding});
                    }
                case ValueTypeKind::List:
                    {
                        const ValueTypeBinding &element_binding = ValueBuilder::checked(type.element_type).checked_binding();
                        const auto &plan = type.fixed_size != 0
                                               ? MemoryUtils::array_plan(element_binding.checked_plan(), type.fixed_size)
                                               : dynamic_list_plan(element_binding);
                        const ValueTypeBinding &binding = ValueTypeBinding::intern(type, plan, list_value_ops());
                        return value_builder_registry().store_if_absent(type, ValueBuilderOps{.binding = &binding});
                    }
                case ValueTypeKind::Set:
                    {
                        const ValueTypeBinding &element_binding = ValueBuilder::checked(type.element_type).checked_binding();
                        const auto             &plan            = set_plan(element_binding);
                        const ValueTypeBinding &binding         = ValueTypeBinding::intern(type, plan, set_value_ops());
                        return value_builder_registry().store_if_absent(type, ValueBuilderOps{.binding = &binding});
                    }
                case ValueTypeKind::Map:
                    {
                        const ValueTypeBinding &key_binding   = ValueBuilder::checked(type.key_type).checked_binding();
                        const ValueTypeBinding &value_binding = ValueBuilder::checked(type.element_type).checked_binding();
                        const auto             &plan          = map_plan(key_binding, value_binding);
                        const ValueTypeBinding &binding       = ValueTypeBinding::intern(type, plan, map_value_ops());
                        return value_builder_registry().store_if_absent(type, ValueBuilderOps{.binding = &binding});
                    }
                case ValueTypeKind::CyclicBuffer:
                    {
                        const ValueTypeBinding &element_binding = ValueBuilder::checked(type.element_type).checked_binding();
                        const auto             &plan            = cyclic_buffer_plan(element_binding, type.fixed_size);
                        const ValueTypeBinding &binding         = ValueTypeBinding::intern(type, plan, cyclic_buffer_value_ops());
                        return value_builder_registry().store_if_absent(type, ValueBuilderOps{.binding = &binding});
                    }
                case ValueTypeKind::Queue:
                    {
                        const ValueTypeBinding &element_binding = ValueBuilder::checked(type.element_type).checked_binding();
                        const auto             &plan            = queue_plan(element_binding, type.fixed_size);
                        const ValueTypeBinding &binding         = ValueTypeBinding::intern(type, plan, queue_value_ops());
                        return value_builder_registry().store_if_absent(type, ValueBuilderOps{.binding = &binding});
                    }
                case ValueTypeKind::Atomic: break;
            }

            throw std::logic_error("No v2 ValueBuilder is registered for this atomic value type; bind scalars via "
                                   "hgraph::v2::value::scalar_type_meta<T>()");
        }

        template <typename T> [[nodiscard]] const ValueBuilder &register_scalar_value_builder(std::string_view name) {
            using Type                       = remove_cvref_t<T>;
            const ValueTypeMetaData *meta    = TypeRegistry::instance().register_scalar<Type>(name);
            static const auto       &plan    = MemoryUtils::plan_for<Type>();
            static const auto       &binding = ValueTypeBinding::intern(*meta, plan, scalar_value_ops<Type>());
            return value_builder_registry().store_if_absent(*meta, ValueBuilderOps{.binding = &binding});
        }
    }  // namespace detail

    inline const ValueBuilder *ValueBuilder::find(const ValueTypeMetaData *type) {
        return detail::value_builder_registry().find(type);
    }

    inline const ValueBuilder &ValueBuilder::checked(const ValueTypeMetaData *type) {
        if (type == nullptr) { throw std::logic_error("ValueBuilder::checked requires a non-null value type"); }
        if (const ValueBuilder *builder = find(type); builder != nullptr) { return *builder; }
        if (type->kind != ValueTypeKind::Atomic) { return detail::register_value_builder(*type); }
        throw std::logic_error("No v2 ValueBuilder is registered for this atomic value type; bind scalars via "
                               "hgraph::v2::value::scalar_type_meta<T>()");
    }

    namespace value
    {
        /**
         * Return the cached scalar builder for `T`.
         *
         * This is the bridge between the v2 metadata registry and the new
         * value/view layer. Callers that need a scalar schema for owning
         * `Value` or non-owning `ValueView` should prefer this path over
         * calling `TypeRegistry::register_scalar<T>()` directly.
         */
        template <typename T>
        [[nodiscard]] const ValueBuilder &
        scalar_value_builder(std::string_view name = detail::ScalarTypeName<detail::remove_cvref_t<T>>::value) {
            return detail::register_scalar_value_builder<detail::remove_cvref_t<T>>(name);
        }

        template <typename T>
        [[nodiscard]] const ValueTypeMetaData *
        scalar_type_meta(std::string_view name = detail::ScalarTypeName<detail::remove_cvref_t<T>>::value) {
            return scalar_value_builder<detail::remove_cvref_t<T>>(name).type();
        }
    }  // namespace value
}  // namespace hgraph::v2

#endif  // HGRAPH_CPP_ROOT_VALUE_BUILDER_H
