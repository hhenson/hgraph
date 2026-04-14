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
    {
        TSValueBuilderFactory::checked_builder_for(schema).construct_value(*this);
    }

    TSValue::TSValue(const TSValue &other)
    {
        if (other.m_builder != nullptr) { other.builder().copy_construct_value(*this, other); }
    }

    TSValue::TSValue(TSValue &&other) noexcept
    {
        if (other.m_builder != nullptr) { other.builder().move_construct_value(*this, other); }
    }

    TSValue &TSValue::operator=(const TSValue &other)
    {
        if (this == &other) { return *this; }

        if (other.m_builder == nullptr) {
            clear_storage();
            reset_binding();
            return *this;
        }

        if (m_builder == nullptr) {
            m_builder = other.m_builder;
            m_storage.set_tag(storage_ownership_tag(StorageOwnership::Owned));
            void *memory = builder().allocate();
            try {
                builder().copy_construct(memory, other.storage_memory(), other.builder());
                attach_storage(memory);
            } catch (...) {
                builder().deallocate(memory);
                reset_binding();
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
        m_storage.set(replacement, storage_ownership_tag(StorageOwnership::Owned));
        return *this;
    }

    TSValue &TSValue::operator=(TSValue &&other) noexcept
    {
        if (this == &other) { return *this; }

        clear_storage();
        m_builder = other.m_builder;
        m_storage = other.m_storage;

        other.reset_binding();
        return *this;
    }

    TSValue::~TSValue()
    {
        clear_storage();
    }

    View TSValue::value() const noexcept
    {
        return m_builder != nullptr
                   ? View{&builder().value_builder().dispatch(), const_cast<void *>(value_memory()), value_schema_of(builder())}
                   : View::invalid_for(nullptr);
    }

    View TSValue::value() noexcept
    {
        return m_builder != nullptr ? View{&builder().value_builder().dispatch(), value_memory(), value_schema_of(builder())}
                                    : View::invalid_for(nullptr);
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

    BufferView TSValue::window_value() const
    {
        return BufferView{value().as_tuple()[1]};
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
        builder().construct_value(*this);
    }

    void TSValue::clear_storage() noexcept
    {
        if (m_builder == nullptr || storage_memory() == nullptr) { return; }
        builder().destruct_value(*this);
    }
}  // namespace hgraph
