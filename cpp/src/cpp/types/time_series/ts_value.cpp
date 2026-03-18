#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/ts_value.h>

namespace hgraph
{
    namespace
    {
        [[nodiscard]] const value::TypeMeta *value_schema_of(const TSValueBuilder &builder) noexcept
        {
            return builder.schema().value_type;
        }
    }  // namespace

    TSValue::TSValue(const TSMeta &schema)
        : m_builder(&TSValueBuilderFactory::checked_builder_for(schema)), m_schema(schema)
    {
        allocate_and_construct();
    }

    TSValue::TSValue(const TSValue &other)
        : m_builder(other.m_builder), m_schema(other.m_schema)
    {
        if (m_builder == nullptr) { return; }

        m_storage = builder().allocate();
        try {
            builder().copy_construct(m_storage, other.storage_memory(), other.builder());
        } catch (...) {
            builder().deallocate(m_storage);
            m_storage = nullptr;
            throw;
        }
    }

    TSValue::TSValue(TSValue &&other) noexcept
        : m_builder(other.m_builder), m_storage(other.m_storage), m_schema(other.m_schema)
    {
        other.m_builder = nullptr;
        other.m_storage = nullptr;
    }

    TSValue &TSValue::operator=(const TSValue &other)
    {
        if (this == &other) { return *this; }

        if (other.m_builder == nullptr) {
            clear_storage();
            m_builder = nullptr;
            return *this;
        }

        if (m_builder == nullptr) {
            m_builder = other.m_builder;
            m_schema = other.m_schema;
            m_storage = builder().allocate();
            try {
                builder().copy_construct(m_storage, other.storage_memory(), other.builder());
            } catch (...) {
                builder().deallocate(m_storage);
                m_storage = nullptr;
                m_builder = nullptr;
                throw;
            }
            return *this;
        }

        if (&builder() != &other.builder()) {
            throw std::invalid_argument("TSValue copy assignment requires matching builder");
        }

        void *replacement = builder().allocate();
        try {
            builder().copy_construct(replacement, other.storage_memory(), other.builder());
        } catch (...) {
            builder().deallocate(replacement);
            throw;
        }

        clear_storage();
        m_builder = other.m_builder;
        m_schema = other.m_schema;
        m_storage = replacement;
        return *this;
    }

    TSValue &TSValue::operator=(TSValue &&other) noexcept
    {
        if (this == &other) { return *this; }

        clear_storage();
        m_builder = other.m_builder;
        m_storage = other.m_storage;
        m_schema = other.m_schema;

        other.m_builder = nullptr;
        other.m_storage = nullptr;
        return *this;
    }

    TSValue::~TSValue()
    {
        clear_storage();
    }

    View TSValue::value() const noexcept
    {
        return m_builder != nullptr ? View{&builder().value_builder().dispatch(), const_cast<void *>(value_memory()), value_schema_of(builder())}
                                    : View::invalid_for(schema().value_type);
    }

    View TSValue::value() noexcept
    {
        return m_builder != nullptr ? View{&builder().value_builder().dispatch(), value_memory(), value_schema_of(builder())}
                                    : View::invalid_for(schema().value_type);
    }

    AtomicView TSValue::atomic_value() const
    {
        return value().as_atomic();
    }

    ListView TSValue::list_value() const
    {
        return value().as_list();
    }

    BundleView TSValue::bundle_value() const
    {
        return value().as_bundle();
    }

    SetView TSValue::set_value() const
    {
        return value().as_set();
    }

    MapView TSValue::dict_value() const
    {
        return value().as_map();
    }

    CyclicBufferView TSValue::window_value() const
    {
        return value().as_cyclic_buffer();
    }

    ListDeltaView TSValue::list_delta_value() const
    {
        return list_value().delta();
    }

    BundleDeltaView TSValue::bundle_delta_value() const
    {
        return bundle_value().delta();
    }

    SetDeltaView TSValue::set_delta_value() const
    {
        return set_value().delta();
    }

    MapDeltaView TSValue::dict_delta_value() const
    {
        return dict_value().delta();
    }

    void TSValue::allocate_and_construct()
    {
        m_storage = builder().allocate();
        try {
            builder().construct(m_storage);
        } catch (...) {
            builder().deallocate(m_storage);
            m_storage = nullptr;
            throw;
        }
    }

    void TSValue::clear_storage() noexcept
    {
        if (m_builder == nullptr || m_storage == nullptr) { return; }
        builder().destruct(m_storage);
        builder().deallocate(m_storage);
        m_storage = nullptr;
    }
}  // namespace hgraph
