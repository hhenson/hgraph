#ifndef HGRAPH_CPP_TS_DATA_STORAGE_H
#define HGRAPH_CPP_TS_DATA_STORAGE_H

#include <hgraph/types/time_series/ts_data/base_view.h>
#include <hgraph/types/utils/memory_utils.h>

namespace hgraph
{
    namespace detail
    {
        void attach_owned_ts_data_parents(TSDataView root);
        void invalidate_owned_ts_data_tree(TSDataView root) noexcept;
    }

    class TSDataOwnedStorage
    {
      public:
        TSDataOwnedStorage() noexcept = default;
        explicit TSDataOwnedStorage(TSRoleTypeRef type,
                                    const MemoryUtils::AllocatorOps &allocator = MemoryUtils::allocator());
        TSDataOwnedStorage(const TSDataOwnedStorage &other);
        TSDataOwnedStorage &operator=(const TSDataOwnedStorage &other);
        TSDataOwnedStorage(TSDataOwnedStorage &&other) noexcept;
        TSDataOwnedStorage &operator=(TSDataOwnedStorage &&other) noexcept;
        ~TSDataOwnedStorage() noexcept;

        [[nodiscard]] bool has_value() const noexcept { return data_ != nullptr; }
        [[nodiscard]] TSRoleTypeRef storage_type() const noexcept { return type_; }
        [[nodiscard]] void *data() noexcept { return data_; }
        [[nodiscard]] const void *data() const noexcept { return data_; }
        void reset() noexcept;

      private:
        void construct_default(TSRoleTypeRef type, const MemoryUtils::AllocatorOps &allocator);
        void construct_copy(const TSDataOwnedStorage &other);

        TSRoleTypeRef type_{};
        const MemoryUtils::AllocatorOps *allocator_{nullptr};
        void *data_{nullptr};
    };

    static_assert(sizeof(TSDataOwnedStorage) == sizeof(void *) * 3);

    /** Owning storage for one TSData root allocation. */
    class TSData
    {
      public:
        using storage_type = TSDataOwnedStorage;

        TSData() noexcept;
        explicit TSData(TSRoleTypeRef type) : storage_(type) {}
        explicit TSData(TSDataTypeRef type) : TSData(type.as_role()) {}
        explicit TSData(TSInputTypeRef type) : TSData(type.as_role()) {}
        explicit TSData(TSOutputTypeRef type) : TSData(type.as_role()) {}

        /** True when the storage owns a bound TSData allocation. */
        [[nodiscard]] bool has_value() const noexcept;

        /** Canonical type record and schema for the owned allocation. */
        [[nodiscard]] TSRoleTypeRef type_ref() const noexcept;
        [[nodiscard]] const TSValueTypeMetaData *schema() const noexcept;

        /** Borrowed root TSData view over the owned allocation. */
        [[nodiscard]] TSDataView view();
        [[nodiscard]] TSDataView view() const;

      private:
        storage_type storage_{};
    };

    static_assert(sizeof(TSData) == sizeof(void *) * 3);
}  // namespace hgraph

#endif  // HGRAPH_CPP_TS_DATA_STORAGE_H
