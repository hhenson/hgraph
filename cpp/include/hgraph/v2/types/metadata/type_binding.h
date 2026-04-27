#ifndef HGRAPH_CPP_ROOT_TYPE_BINDING_H
#define HGRAPH_CPP_ROOT_TYPE_BINDING_H

#include <hgraph/v2/types/utils/intern_table.h>
#include <hgraph/v2/types/utils/memory_utils.h>

#include <functional>
#include <stdexcept>

namespace hgraph::v2
{
    /**
     * Interned binding from one schema descriptor to lifecycle and runtime ops.
     *
     * The binding keeps schema identity, storage layout, lifecycle hooks, and
     * runtime ops together behind one stable pointer. Lightweight views and
     * bound storage handles can then recover everything they need from the
     * same interned binding object.
     */
    template <typename TypeMeta, typename Ops> struct TypeBinding
    {
        const TypeMeta                 *type_meta{nullptr};
        const MemoryUtils::StoragePlan *storage_plan{nullptr};
        const Ops                      *ops{nullptr};

        [[nodiscard]] constexpr bool valid() const noexcept {
            return type_meta != nullptr && storage_plan != nullptr && ops != nullptr;
        }

        [[nodiscard]] const TypeMeta &checked_type() const {
            if (type_meta == nullptr) { throw std::logic_error("TypeBinding is missing type metadata"); }
            return *type_meta;
        }

        [[nodiscard]] const MemoryUtils::StoragePlan *plan() const noexcept { return storage_plan; }

        [[nodiscard]] const MemoryUtils::StoragePlan &checked_plan() const {
            if (const auto *bound_plan = plan(); bound_plan != nullptr) { return *bound_plan; }
            throw std::logic_error("TypeBinding is missing a storage plan");
        }

        [[nodiscard]] const MemoryUtils::LifecycleOps &checked_lifecycle() const { return checked_plan().lifecycle; }

        [[nodiscard]] const Ops &checked_ops() const {
            if (ops == nullptr) { throw std::logic_error("TypeBinding is missing runtime operations"); }
            return *ops;
        }

        [[nodiscard]] const MemoryUtils::LifecycleOps *lifecycle() const noexcept {
            return plan() != nullptr ? &plan()->lifecycle : nullptr;
        }

        [[nodiscard]] const void *lifecycle_context() const noexcept {
            return plan() != nullptr ? plan()->lifecycle_context : nullptr;
        }

        void default_construct_at(void *memory) const { checked_plan().default_construct(memory); }

        void destroy_at(void *memory) const noexcept { checked_plan().destroy(memory); }

        void copy_construct_at(void *dst, const void *src) const { checked_plan().copy_construct(dst, src); }

        void move_construct_at(void *dst, void *src) const { checked_plan().move_construct(dst, src); }

        void copy_assign_at(void *dst, const void *src) const { checked_plan().copy_assign(dst, src); }

        void move_assign_at(void *dst, void *src) const { checked_plan().move_assign(dst, src); }

        struct Key
        {
            const TypeMeta                 *type_meta{nullptr};
            const MemoryUtils::StoragePlan *storage_plan{nullptr};
            const Ops                      *ops{nullptr};

            [[nodiscard]] bool operator==(const Key &) const noexcept = default;
        };

        struct KeyHash
        {
            [[nodiscard]] size_t operator()(const Key &key) const noexcept {
                size_t seed = std::hash<const TypeMeta *>{}(key.type_meta);
                combine(seed, std::hash<const MemoryUtils::StoragePlan *>{}(key.storage_plan));
                combine(seed, std::hash<const Ops *>{}(key.ops));
                return seed;
            }

          private:
            static void combine(size_t &seed, size_t value) noexcept {
                seed ^= value + 0x9e3779b97f4a7c15ULL + (seed << 6U) + (seed >> 2U);
            }
        };

        [[nodiscard]] static const TypeBinding &intern(const TypeMeta &type_meta, const MemoryUtils::StoragePlan &plan,
                                                       const Ops &ops) {
            return registry().emplace(
                Key{
                    .type_meta    = &type_meta,
                    .storage_plan = &plan,
                    .ops          = &ops,
                },
                &type_meta, &plan, &ops);
        }

        [[nodiscard]] static const TypeBinding *find(const TypeMeta *type_meta, const MemoryUtils::StoragePlan *storage_plan,
                                                     const Ops *ops) noexcept {
            return registry().find(Key{
                .type_meta    = type_meta,
                .storage_plan = storage_plan,
                .ops          = ops,
            });
        }

      private:
        [[nodiscard]] static InternTable<Key, TypeBinding, KeyHash> &registry() noexcept {
            static InternTable<Key, TypeBinding, KeyHash> registry;
            return registry;
        }
    };
}  // namespace hgraph::v2

#endif  // HGRAPH_CPP_ROOT_TYPE_BINDING_H
