#ifndef HGRAPH_TYPES_METADATA_TYPE_RECORD_REGISTRY_H
#define HGRAPH_TYPES_METADATA_TYPE_RECORD_REGISTRY_H

#include <hgraph/types/metadata/type_record.h>

#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>

namespace hgraph
{
    struct TypeRecordKey
    {
        const SchemaHeader *schema{nullptr};
        TypeRole role{TypeRole::Invalid};
        const MemoryUtils::StoragePlan *plan{nullptr};
        const void *ops{nullptr};
        const DebugDescriptor *debug{nullptr};
        /** Stable representation identity. Equality is by string content;
            the registry owns the canonical copy. */
        std::string_view implementation_label{};

        [[nodiscard]] constexpr bool operator==(const TypeRecordKey &) const noexcept = default;
    };

    struct TypeRecordDefinition
    {
        TypeRecordKey key{};
        std::uint16_t ops_abi_version{INVALID_OPS_ABI_VERSION};
        TypeCapabilities capabilities{TypeCapabilities::None};
    };

    class HGRAPH_EXPORT TypeRecordRegistry
    {
    public:
        static TypeRecordRegistry &instance();

        TypeRecordRegistry(const TypeRecordRegistry &) = delete;
        TypeRecordRegistry &operator=(const TypeRecordRegistry &) = delete;
        TypeRecordRegistry(TypeRecordRegistry &&) = delete;
        TypeRecordRegistry &operator=(TypeRecordRegistry &&) = delete;

        [[nodiscard]] const TypeRecord &intern(const TypeRecordDefinition &definition);
        /** Publish a graph-local value record's ordinary owning record once. */
        void publish_external_value_owner(const TypeRecord &record,
                                          const TypeRecord &owner);
        [[nodiscard]] const TypeRecord *find(const TypeRecordKey &key) const noexcept;
        /** Cold-path cardinality used by registry diagnostics. */
        [[nodiscard]] std::size_t size() const noexcept;
        void reset() noexcept;

    private:
        struct KeyHash
        {
            [[nodiscard]] std::size_t operator()(const TypeRecordKey &key) const noexcept;
        };

        struct Entry
        {
            std::string implementation_label;
            TypeRecord record;

            explicit Entry(const TypeRecordDefinition &definition);
        };

        TypeRecordRegistry() = default;

        mutable std::mutex m_mutex;
        std::unordered_map<TypeRecordKey, std::unique_ptr<Entry>, KeyHash> m_entries;
    };
} // namespace hgraph

#endif // HGRAPH_TYPES_METADATA_TYPE_RECORD_REGISTRY_H
