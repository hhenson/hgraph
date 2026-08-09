#ifndef HGRAPH_TYPES_UTILS_SMALL_DENSE_PTR_MAP_H
#define HGRAPH_TYPES_UTILS_SMALL_DENSE_PTR_MAP_H

#include <hgraph/types/storage_metrics.h>

#include <ankerl/unordered_dense.h>

#include <algorithm>
#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <new>
#include <span>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace hgraph::detail
{
    /**
     * Compact sparse owner map for stable heap objects.
     *
     * The outer handle is one tagged pointer to the active representation's
     * memory. Empty maps need no allocation; one entry uses a direct holder;
     * small maps use a compact trailing entry block; wider maps use
     * ``ankerl::unordered_dense::map``. Representation dispatch is an inline
     * switch so compilers can inline the selected operation. Only insertion can
     * replace a non-empty representation.
     *
     * Representation changes move ``unique_ptr`` owners, never pointees, so
     * pointers returned by ``find`` and ``ensure`` remain stable. Small and
     * dense representations deliberately do not demote after erase, avoiding
     * churn when keyed runtime structures are reused.
     */
    template <typename Key,
              typename Pointee,
              typename Deleter = std::default_delete<Pointee>,
              typename Hash = ankerl::unordered_dense::hash<Key>,
              typename KeyEqual = std::equal_to<Key>,
              std::size_t SmallLimit = 8>
    class SmallDensePtrMap
    {
        static_assert(SmallLimit >= 2);
        static_assert(std::is_nothrow_copy_constructible_v<Key>);
        static_assert(std::is_nothrow_move_constructible_v<Key>);
        static_assert(std::is_nothrow_move_assignable_v<Key>);
        static_assert(std::is_nothrow_default_constructible_v<Hash>);
        static_assert(std::is_nothrow_default_constructible_v<KeyEqual>);
        static_assert(std::is_nothrow_invocable_r_v<std::size_t, Hash, const Key &>);

        using pointer_type = std::unique_ptr<Pointee, Deleter>;
        using DenseStorage = ankerl::unordered_dense::map<Key, pointer_type, Hash, KeyEqual>;
        using value_type = typename DenseStorage::value_type;

        static_assert(std::is_nothrow_default_constructible_v<pointer_type>);
        static_assert(std::is_nothrow_move_assignable_v<value_type>);

        struct SmallStorage
        {
            std::size_t size{0};
            std::size_t capacity{0};

            [[nodiscard]] static constexpr std::size_t allocation_alignment() noexcept
            {
                return std::max(alignof(SmallStorage), alignof(value_type));
            }

            [[nodiscard]] static constexpr std::size_t entries_offset() noexcept
            {
                constexpr std::size_t alignment = alignof(value_type);
                return (sizeof(SmallStorage) + alignment - 1) & ~(alignment - 1);
            }

            [[nodiscard]] static constexpr std::size_t allocation_bytes(std::size_t capacity_) noexcept
            {
                return entries_offset() + capacity_ * sizeof(value_type);
            }

            [[nodiscard]] static SmallStorage *create(std::size_t capacity_)
            {
                assert(capacity_ >= 2 && capacity_ <= SmallLimit);
                void *memory = ::operator new(allocation_bytes(capacity_),
                                              std::align_val_t{allocation_alignment()});
                return std::construct_at(static_cast<SmallStorage *>(memory),
                                         SmallStorage{.size = 0, .capacity = capacity_});
            }

            static void destroy(SmallStorage *storage) noexcept
            {
                if (storage == nullptr) { return; }
                for (std::size_t index = storage->size; index > 0; --index)
                {
                    std::destroy_at(storage->entries() + (index - 1));
                }
                std::destroy_at(storage);
                ::operator delete(storage, std::align_val_t{allocation_alignment()});
            }

            [[nodiscard]] value_type *entries() noexcept
            {
                auto *bytes = reinterpret_cast<std::byte *>(this);
                return std::launder(reinterpret_cast<value_type *>(bytes + entries_offset()));
            }

            [[nodiscard]] const value_type *entries() const noexcept
            {
                const auto *bytes = reinterpret_cast<const std::byte *>(this);
                return std::launder(reinterpret_cast<const value_type *>(bytes + entries_offset()));
            }

            [[nodiscard]] std::span<value_type> values() noexcept
            {
                return {entries(), size};
            }

            [[nodiscard]] std::span<const value_type> values() const noexcept
            {
                return {entries(), size};
            }

            void append_key(const Key &key) noexcept
            {
                assert(size < capacity);
                std::construct_at(entries() + size, key, pointer_type{});
                ++size;
            }

            void erase_at(std::size_t index) noexcept
            {
                assert(index < size);
                for (std::size_t current = index; current + 1 < size; ++current)
                {
                    entries()[current] = std::move(entries()[current + 1]);
                }
                std::destroy_at(entries() + (size - 1));
                --size;
            }
        };

        struct SmallStorageDelete
        {
            void operator()(SmallStorage *storage) const noexcept { SmallStorage::destroy(storage); }
        };

        using SmallStorageOwner = std::unique_ptr<SmallStorage, SmallStorageDelete>;

        enum class Representation : std::uint8_t
        {
            Empty,
            Single,
            Small,
            Dense,
        };

        static constexpr std::uintptr_t representation_mask{0x3};

        static_assert(alignof(value_type) > representation_mask);
        static_assert(alignof(SmallStorage) > representation_mask);
        static_assert(alignof(DenseStorage) > representation_mask);

      public:
        SmallDensePtrMap() noexcept = default;
        SmallDensePtrMap(const SmallDensePtrMap &) = delete;
        SmallDensePtrMap &operator=(const SmallDensePtrMap &) = delete;

        SmallDensePtrMap(SmallDensePtrMap &&other) noexcept
            : tagged_storage_{std::exchange(other.tagged_storage_, 0)}
        {}

        SmallDensePtrMap &operator=(SmallDensePtrMap &&other) noexcept
        {
            if (this != &other)
            {
                destroy_representation(representation(), storage());
                tagged_storage_ = std::exchange(other.tagged_storage_, 0);
            }
            return *this;
        }

        ~SmallDensePtrMap() { destroy_representation(representation(), storage()); }

        [[nodiscard]] bool empty() const noexcept { return size() == 0; }
        [[nodiscard]] std::size_t size() const noexcept
        {
            switch (representation())
            {
            case Representation::Empty: return 0;
            case Representation::Single: return 1;
            case Representation::Small: return small_size(storage());
            case Representation::Dense: return dense_size(storage());
            }
            return 0;
        }

        [[nodiscard]] bool uses_dense_storage() const noexcept
        {
            return representation() == Representation::Dense;
        }

        [[nodiscard]] Pointee *find(const Key &key) noexcept
        {
            return std::as_const(*this).find(key);
        }

        [[nodiscard]] Pointee *find(const Key &key) const noexcept
        {
            switch (representation())
            {
            case Representation::Empty: return nullptr;
            case Representation::Single: return single_find(storage(), key);
            case Representation::Small: return small_find(storage(), key);
            case Representation::Dense: return dense_find(storage(), key);
            }
            return nullptr;
        }

        [[nodiscard]] bool contains(const Key &key) const noexcept { return find(key) != nullptr; }

        [[nodiscard]] std::pair<Pointee *, bool> insert(Key key, pointer_type value)
        {
            assert(value != nullptr && "SmallDensePtrMap owns non-null pointees");
            switch (representation())
            {
            case Representation::Empty: return nop_insert(*this, std::move(key), std::move(value));
            case Representation::Single: return single_insert(*this, std::move(key), std::move(value));
            case Representation::Small: return small_insert(*this, std::move(key), std::move(value));
            case Representation::Dense: return dense_insert(*this, std::move(key), std::move(value));
            }
            throw std::logic_error("SmallDensePtrMap has an invalid representation");
        }

        template <typename Factory>
        [[nodiscard]] Pointee &ensure(const Key &key, Factory &&factory)
        {
            if (auto *existing = find(key); existing != nullptr) { return *existing; }
            auto value = std::invoke(std::forward<Factory>(factory));
            assert(value != nullptr && "SmallDensePtrMap owns non-null pointees");
            return *insert_absent(Key{key}, std::move(value));
        }

        [[nodiscard]] bool erase(const Key &key) noexcept
        {
            switch (representation())
            {
            case Representation::Empty: return false;
            case Representation::Single: return single_erase(*this, key);
            case Representation::Small: return small_erase(*this, key);
            case Representation::Dense: return dense_erase(*this, key);
            }
            return false;
        }

        template <typename Function>
        void for_each(Function &&function)
        {
            for (const auto &[key, value] : entries())
            {
                std::invoke(function, std::as_const(key), *value);
            }
        }

        template <typename Function>
        void for_each(Function &&function) const
        {
            for (const auto &[key, value] : entries())
            {
                std::invoke(function, key, std::as_const(*value));
            }
        }

        template <typename Predicate>
        [[nodiscard]] bool any_of(Predicate &&predicate) const
        {
            for (const auto &[key, value] : entries())
            {
                if (std::invoke(predicate, key, std::as_const(*value))) { return true; }
            }
            return false;
        }

        /** Heap bytes owned by the active index/entry representation, excluding pointees. */
        [[nodiscard]] DynamicStorageMetrics dynamic_storage_metrics() const noexcept
        {
            switch (representation())
            {
            case Representation::Empty: return {};
            case Representation::Single: return single_metrics(storage());
            case Representation::Small: return small_metrics(storage());
            case Representation::Dense: return dense_metrics(storage());
            }
            return {};
        }

      private:
        [[nodiscard]] Representation representation() const noexcept
        {
            return static_cast<Representation>(tagged_storage_ & representation_mask);
        }

        [[nodiscard]] void *storage() const noexcept
        {
            return reinterpret_cast<void *>(tagged_storage_ & ~representation_mask);
        }

        void set_storage(Representation representation_, void *storage_) noexcept
        {
            const auto address = reinterpret_cast<std::uintptr_t>(storage_);
            assert((address & representation_mask) == 0);
            assert((representation_ == Representation::Empty) == (storage_ == nullptr));
            tagged_storage_ = address | static_cast<std::uintptr_t>(representation_);
        }

        static void destroy_representation(Representation representation_, void *storage_) noexcept
        {
            switch (representation_)
            {
            case Representation::Empty: return;
            case Representation::Single: single_destroy(storage_); return;
            case Representation::Small: small_destroy(storage_); return;
            case Representation::Dense: dense_destroy(storage_); return;
            }
        }

        [[nodiscard]] std::span<const value_type> entries() const noexcept
        {
            switch (representation())
            {
            case Representation::Empty: return {};
            case Representation::Single: return single_const_entries(storage());
            case Representation::Small: return small_const_entries(storage());
            case Representation::Dense: return dense_entries(storage());
            }
            return {};
        }

        [[nodiscard]] Pointee *insert_absent(Key key, pointer_type value)
        {
            switch (representation())
            {
            case Representation::Empty:
                return nop_insert_absent(*this, std::move(key), std::move(value));
            case Representation::Single:
                return single_insert_absent(*this, std::move(key), std::move(value));
            case Representation::Small:
                return small_insert_absent(*this, std::move(key), std::move(value));
            case Representation::Dense:
                return dense_insert_absent(*this, std::move(key), std::move(value));
            }
            throw std::logic_error("SmallDensePtrMap has an invalid representation");
        }

        [[nodiscard]] static Pointee *linear_find(std::span<const value_type> values,
                                                  const Key &key) noexcept
        {
            const auto found = std::find_if(values.begin(), values.end(), [&](const value_type &entry) {
                return KeyEqual{}(entry.first, key);
            });
            return found != values.end() ? found->second.get() : nullptr;
        }

        void replace_with(Representation replacement_representation,
                          void *replacement_storage) noexcept
        {
            const Representation old_representation = representation();
            void *old_storage = storage();
            set_storage(replacement_representation, replacement_storage);
            destroy_representation(old_representation, old_storage);
        }

        [[nodiscard]] static SmallStorageOwner make_small_shell(std::span<const value_type> existing,
                                                                 const Key &new_key,
                                                                 std::size_t capacity)
        {
            SmallStorageOwner replacement{SmallStorage::create(capacity)};
            for (const auto &entry : existing) { replacement->append_key(entry.first); }
            replacement->append_key(new_key);
            return replacement;
        }

        static void transfer_existing(std::span<value_type> source,
                                      std::span<value_type> target) noexcept
        {
            assert(source.size() <= target.size());
            for (std::size_t index = 0; index < source.size(); ++index)
            {
                target[index].second = std::move(source[index].second);
            }
        }

        // Empty representation.
        [[nodiscard]] static Pointee *nop_insert_absent(SmallDensePtrMap &owner,
                                                        Key key,
                                                        pointer_type value)
        {
            auto replacement = std::make_unique<value_type>(std::move(key), std::move(value));
            auto *result = replacement->second.get();
            owner.replace_with(Representation::Single, replacement.release());
            return result;
        }

        [[nodiscard]] static std::pair<Pointee *, bool> nop_insert(SmallDensePtrMap &owner,
                                                                   Key key,
                                                                   pointer_type value)
        {
            return {nop_insert_absent(owner, std::move(key), std::move(value)), true};
        }

        // Single-entry representation.
        static void single_destroy(void *storage) noexcept { delete static_cast<value_type *>(storage); }
        [[nodiscard]] static std::size_t single_size(const void *) noexcept { return 1; }
        [[nodiscard]] static Pointee *single_find(const void *storage, const Key &key) noexcept
        {
            const auto *entry = static_cast<const value_type *>(storage);
            return KeyEqual{}(entry->first, key) ? entry->second.get() : nullptr;
        }
        [[nodiscard]] static std::span<value_type> single_entries(void *storage) noexcept
        {
            return {static_cast<value_type *>(storage), 1};
        }
        [[nodiscard]] static std::span<const value_type> single_const_entries(const void *storage) noexcept
        {
            return {static_cast<const value_type *>(storage), 1};
        }
        [[nodiscard]] static DynamicStorageMetrics single_metrics(const void *) noexcept
        {
            return {.live_bytes = sizeof(value_type), .reserved_bytes = sizeof(value_type)};
        }

        [[nodiscard]] static Pointee *single_insert_absent(SmallDensePtrMap &owner,
                                                           Key key,
                                                           pointer_type value)
        {
            auto source = single_entries(owner.storage());
            auto replacement = make_small_shell(source, key, 2);
            transfer_existing(source, replacement->values());
            replacement->entries()[1].second = std::move(value);
            auto *result = replacement->entries()[1].second.get();
            owner.replace_with(Representation::Small, replacement.release());
            return result;
        }

        [[nodiscard]] static std::pair<Pointee *, bool> single_insert(SmallDensePtrMap &owner,
                                                                      Key key,
                                                                      pointer_type value)
        {
            if (auto *existing = single_find(owner.storage(), key); existing != nullptr)
            {
                return {existing, false};
            }
            return {single_insert_absent(owner, std::move(key), std::move(value)), true};
        }

        [[nodiscard]] static bool single_erase(SmallDensePtrMap &owner, const Key &key) noexcept
        {
            if (single_find(owner.storage(), key) == nullptr) { return false; }
            owner.replace_with(Representation::Empty, nullptr);
            return true;
        }

        // Compact linear representation.
        static void small_destroy(void *storage) noexcept
        {
            SmallStorage::destroy(static_cast<SmallStorage *>(storage));
        }
        [[nodiscard]] static std::size_t small_size(const void *storage) noexcept
        {
            return static_cast<const SmallStorage *>(storage)->size;
        }
        [[nodiscard]] static Pointee *small_find(const void *storage, const Key &key) noexcept
        {
            return linear_find(static_cast<const SmallStorage *>(storage)->values(), key);
        }
        [[nodiscard]] static std::span<const value_type> small_const_entries(const void *storage) noexcept
        {
            return static_cast<const SmallStorage *>(storage)->values();
        }
        [[nodiscard]] static DynamicStorageMetrics small_metrics(const void *storage) noexcept
        {
            const auto *small = static_cast<const SmallStorage *>(storage);
            return {
                .live_bytes = SmallStorage::allocation_bytes(small->size),
                .reserved_bytes = SmallStorage::allocation_bytes(small->capacity),
            };
        }

        [[nodiscard]] static Pointee *small_insert_absent(SmallDensePtrMap &owner,
                                                          Key key,
                                                          pointer_type value)
        {
            auto *small = static_cast<SmallStorage *>(owner.storage());
            if (small->size < SmallLimit)
            {
                if (small->size < small->capacity)
                {
                    small->append_key(key);
                    small->entries()[small->size - 1].second = std::move(value);
                    return small->entries()[small->size - 1].second.get();
                }

                const std::size_t capacity = std::min(SmallLimit, small->capacity * 2);
                auto replacement = make_small_shell(small->values(), key, capacity);
                transfer_existing(small->values(), replacement->values());
                replacement->entries()[replacement->size - 1].second = std::move(value);
                auto *result = replacement->entries()[replacement->size - 1].second.get();
                owner.replace_with(Representation::Small, replacement.release());
                return result;
            }

            auto replacement = std::make_unique<DenseStorage>(small->size + 1, Hash{}, KeyEqual{});
            replacement->reserve(small->size + 1);
            std::array<pointer_type *, SmallLimit> destination_owners{};
            std::size_t destination_index = 0;
            for (const auto &entry : small->values())
            {
                auto [destination, inserted] = replacement->emplace(entry.first, pointer_type{});
                if (!inserted) { throw std::logic_error("SmallDensePtrMap promotion found a duplicate key"); }
                destination_owners[destination_index++] = std::addressof(destination->second);
            }
            auto [new_entry, inserted] = replacement->emplace(key, pointer_type{});
            if (!inserted) { throw std::logic_error("SmallDensePtrMap promotion found a duplicate key"); }

            destination_index = 0;
            for (auto &entry : small->values())
            {
                *destination_owners[destination_index++] = std::move(entry.second);
            }
            new_entry->second = std::move(value);
            auto *result = new_entry->second.get();
            owner.replace_with(Representation::Dense, replacement.release());
            return result;
        }

        [[nodiscard]] static std::pair<Pointee *, bool> small_insert(SmallDensePtrMap &owner,
                                                                     Key key,
                                                                     pointer_type value)
        {
            if (auto *existing = small_find(owner.storage(), key); existing != nullptr)
            {
                return {existing, false};
            }
            return {small_insert_absent(owner, std::move(key), std::move(value)), true};
        }

        [[nodiscard]] static bool small_erase(SmallDensePtrMap &owner, const Key &key) noexcept
        {
            auto *small = static_cast<SmallStorage *>(owner.storage());
            const auto values = small->values();
            const auto found = std::find_if(values.begin(), values.end(), [&](const value_type &entry) {
                return KeyEqual{}(entry.first, key);
            });
            if (found == values.end()) { return false; }
            small->erase_at(static_cast<std::size_t>(found - values.begin()));
            return true;
        }

        // Dense representation.
        static void dense_destroy(void *storage) noexcept { delete static_cast<DenseStorage *>(storage); }
        [[nodiscard]] static std::size_t dense_size(const void *storage) noexcept
        {
            return static_cast<const DenseStorage *>(storage)->size();
        }
        [[nodiscard]] static Pointee *dense_find(const void *storage, const Key &key) noexcept
        {
            const auto *dense = static_cast<const DenseStorage *>(storage);
            const auto found = dense->find(key);
            return found != dense->end() ? found->second.get() : nullptr;
        }
        [[nodiscard]] static std::span<const value_type> dense_entries(const void *storage) noexcept
        {
            const auto &values = static_cast<const DenseStorage *>(storage)->values();
            return {values.data(), values.size()};
        }
        [[nodiscard]] static DynamicStorageMetrics dense_metrics(const void *storage) noexcept
        {
            const auto *dense = static_cast<const DenseStorage *>(storage);
            using bucket_type = typename DenseStorage::bucket_type;
            return {
                .live_bytes = sizeof(DenseStorage) +
                              dense->size() * (sizeof(value_type) + sizeof(bucket_type)),
                .reserved_bytes = sizeof(DenseStorage) +
                                  dense->values().capacity() * sizeof(value_type) +
                                  dense->bucket_count() * sizeof(bucket_type),
            };
        }

        [[nodiscard]] static Pointee *dense_insert_absent(SmallDensePtrMap &owner,
                                                          Key key,
                                                          pointer_type value)
        {
            auto *dense = static_cast<DenseStorage *>(owner.storage());
            auto [entry, inserted] = dense->emplace(std::move(key), std::move(value));
            assert(inserted);
            return entry->second.get();
        }

        [[nodiscard]] static std::pair<Pointee *, bool> dense_insert(SmallDensePtrMap &owner,
                                                                     Key key,
                                                                     pointer_type value)
        {
            auto *dense = static_cast<DenseStorage *>(owner.storage());
            if (const auto found = dense->find(key); found != dense->end())
            {
                return {found->second.get(), false};
            }
            return {dense_insert_absent(owner, std::move(key), std::move(value)), true};
        }

        [[nodiscard]] static bool dense_erase(SmallDensePtrMap &owner, const Key &key) noexcept
        {
            return static_cast<DenseStorage *>(owner.storage())->erase(key) != 0;
        }

        std::uintptr_t tagged_storage_{0};
    };
}  // namespace hgraph::detail

#endif  // HGRAPH_TYPES_UTILS_SMALL_DENSE_PTR_MAP_H
