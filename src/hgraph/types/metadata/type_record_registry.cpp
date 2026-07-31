#include <hgraph/types/metadata/type_record_registry.h>

#include <functional>
#include <stdexcept>
#include <utility>

namespace hgraph
{
    namespace
    {
        inline constexpr std::uint32_t KNOWN_CAPABILITIES = (1u << 10u) - 1u;

        [[nodiscard]] constexpr bool allowed_role(TypeFamily family, TypeRole role) noexcept
        {
            switch (family)
            {
            case TypeFamily::Value:
                return role == TypeRole::Instance;
            case TypeFamily::TimeSeries:
                return role == TypeRole::Data || role == TypeRole::Input || role == TypeRole::Output;
            case TypeFamily::Node:
            case TypeFamily::Graph:
            case TypeFamily::Executor:
            case TypeFamily::Clock:
                return role == TypeRole::Runtime;
            case TypeFamily::Invalid:
                return false;
            }
            return false;
        }

        [[nodiscard]] constexpr bool known_capabilities(TypeCapabilities capabilities) noexcept
        {
            return (static_cast<std::uint32_t>(capabilities) & ~KNOWN_CAPABILITIES) == 0;
        }

        void validate(const TypeRecordDefinition &definition)
        {
            if (definition.key.schema == nullptr || !definition.key.schema->valid())
            {
                throw std::invalid_argument("TypeRecordDefinition requires a valid schema header");
            }
            if (!allowed_role(definition.key.schema->family, definition.key.role))
            {
                throw std::invalid_argument("TypeRecordDefinition role is not valid for its schema family");
            }
            if (definition.ops_abi_version == INVALID_OPS_ABI_VERSION)
            {
                throw std::invalid_argument("TypeRecordDefinition requires a non-zero ops ABI version");
            }
            if (!known_capabilities(definition.capabilities))
            {
                throw std::invalid_argument("TypeRecordDefinition contains unknown capability bits");
            }
            if (definition.key.plan == nullptr || !definition.key.plan->valid())
            {
                throw std::invalid_argument("TypeRecordDefinition requires a valid storage plan");
            }
            if (definition.key.ops == nullptr)
            {
                throw std::invalid_argument("TypeRecordDefinition requires an ops table");
            }
        }

        [[nodiscard]] std::size_t combine_hash(std::size_t seed, std::size_t value) noexcept
        {
            return seed ^ (value + 0x9e3779b9u + (seed << 6u) + (seed >> 2u));
        }
    } // namespace

    bool TypeRecord::valid() const noexcept
    {
        return magic == TYPE_RECORD_MAGIC && abi_version == TYPE_RECORD_ABI_VERSION && reserved0 == 0 &&
               reserved1 == 0 && schema != nullptr && schema->valid() && allowed_role(schema->family, role) &&
               ops_abi_version != INVALID_OPS_ABI_VERSION && known_capabilities(capabilities) && plan != nullptr &&
               plan->valid() && ops != nullptr && (implementation_label == nullptr || implementation_label[0] != '\0');
    }

    TypeRecordRegistry &TypeRecordRegistry::instance()
    {
        // Records are retained by Values and runtime handles that may be
        // destroyed during static teardown. Match the immortal schema/plan
        // registries so those handles never observe a destructed record table.
        static auto *registry = new TypeRecordRegistry();
        return *registry;
    }

    TypeRecordRegistry::Entry::Entry(const TypeRecordDefinition &definition)
        : implementation_label(definition.implementation_label),
          record(definition.key.role, definition.ops_abi_version, definition.capabilities,
                 implementation_label.empty() ? nullptr : implementation_label.c_str(), definition.key.schema,
                 definition.key.plan, definition.key.ops, definition.key.debug)
    {
    }

    std::size_t TypeRecordRegistry::KeyHash::operator()(const TypeRecordKey &key) const noexcept
    {
        std::size_t result = std::hash<const SchemaHeader *>{}(key.schema);
        result = combine_hash(result, std::hash<std::uint8_t>{}(static_cast<std::uint8_t>(key.role)));
        result = combine_hash(result, std::hash<const MemoryUtils::StoragePlan *>{}(key.plan));
        result = combine_hash(result, std::hash<const void *>{}(key.ops));
        return combine_hash(result, std::hash<const DebugDescriptor *>{}(key.debug));
    }

    const TypeRecord &TypeRecordRegistry::intern(const TypeRecordDefinition &definition)
    {
        validate(definition);

        std::lock_guard lock(m_mutex);
        if (const auto found = m_entries.find(definition.key); found != m_entries.end())
        {
            const Entry &entry = *found->second;
            if (entry.record.ops_abi_version != definition.ops_abi_version ||
                entry.record.capabilities != definition.capabilities ||
                std::string_view{entry.implementation_label} != definition.implementation_label)
            {
                throw std::logic_error(
                    "TypeRecordDefinition conflicts with the canonical record for its key: existing '" +
                    entry.implementation_label + "', requested '" +
                    std::string{definition.implementation_label} + "'");
            }
            return entry.record;
        }

        auto entry = std::make_unique<Entry>(definition);
        const TypeRecord *result = &entry->record;
        m_entries.emplace(definition.key, std::move(entry));
        return *result;
    }

    const TypeRecord *TypeRecordRegistry::find(const TypeRecordKey &key) const noexcept
    {
        std::lock_guard lock(m_mutex);
        const auto found = m_entries.find(key);
        return found == m_entries.end() ? nullptr : &found->second->record;
    }

    std::size_t TypeRecordRegistry::size() const noexcept
    {
        std::lock_guard lock(m_mutex);
        return m_entries.size();
    }

    void TypeRecordRegistry::reset() noexcept
    {
        std::lock_guard lock(m_mutex);
        m_entries.clear();
    }
} // namespace hgraph
