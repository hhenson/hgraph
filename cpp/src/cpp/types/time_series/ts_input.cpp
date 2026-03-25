#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/ts_input.h>

#include <stdexcept>

namespace hgraph
{
    TSInput::TSInput(const TSInputBuilder &builder)
        : TSValue(builder.schema(), builder.ts_value_builder(), StorageOwnership::External), m_builder(&builder)
    {
        allocate_and_construct();
    }

    TSInput::TSInput(const TSInput &other)
        : TSValue(other.schema(), other.builder().ts_value_builder(), StorageOwnership::External), m_builder(other.m_builder)
    {
        m_storage = builder().allocate();
        try {
            builder().copy_construct(m_storage, other.storage_memory(), other.builder());
            attach_storage(builder().ts_value_memory(m_storage));
        } catch (...) {
            builder().deallocate(m_storage);
            m_storage = nullptr;
            throw;
        }
    }

    TSInput::TSInput(TSInput &&other) noexcept
        : TSValue(other.schema(), other.builder().ts_value_builder(), StorageOwnership::External),
          m_builder(other.m_builder),
          m_storage(other.m_storage)
    {
        if (m_storage != nullptr) { attach_storage(builder().ts_value_memory(m_storage)); }
        other.m_builder = nullptr;
        other.m_storage = nullptr;
        other.detach_storage();
    }

    TSInput &TSInput::operator=(const TSInput &other)
    {
        if (this == &other) { return *this; }

        void *replacement = other.builder().allocate();
        try {
            other.builder().copy_construct(replacement, other.storage_memory(), other.builder());
        } catch (...) {
            other.builder().deallocate(replacement);
            throw;
        }

        clear_storage();
        m_builder = other.m_builder;
        m_storage = replacement;
        rebind_builder(builder().schema(), builder().ts_value_builder(), StorageOwnership::External);
        attach_storage(builder().ts_value_memory(m_storage));
        return *this;
    }

    TSInput &TSInput::operator=(TSInput &&other) noexcept
    {
        if (this == &other) { return *this; }

        clear_storage();
        m_builder = other.m_builder;
        m_storage = other.m_storage;
        if (m_builder != nullptr) { rebind_builder(builder().schema(), builder().ts_value_builder(), StorageOwnership::External); }
        if (m_storage != nullptr) { attach_storage(builder().ts_value_memory(m_storage)); }

        other.m_builder = nullptr;
        other.m_storage = nullptr;
        other.detach_storage();
        return *this;
    }

    TSInput::~TSInput()
    {
        clear_storage();
    }

    TSInputView TSInput::view() noexcept
    {
        return TSInputView{view_context()};
    }

    View TSInput::active_state() const
    {
        return View{&builder().active_builder().dispatch(),
                    m_storage != nullptr ? const_cast<void *>(active_memory()) : nullptr,
                    &builder().active_schema()};
    }

    View TSInput::active_state()
    {
        return View{&builder().active_builder().dispatch(), m_storage != nullptr ? active_memory() : nullptr, &builder().active_schema()};
    }

    void TSInput::allocate_and_construct()
    {
        m_storage = builder().allocate();
        try {
            builder().construct(m_storage);
            attach_storage(builder().ts_value_memory(m_storage));
        } catch (...) {
            builder().deallocate(m_storage);
            m_storage = nullptr;
            throw;
        }
    }

    void TSInput::clear_storage() noexcept
    {
        if (m_builder == nullptr || m_storage == nullptr) { return; }
        builder().destruct(m_storage);
        builder().deallocate(m_storage);
        m_storage = nullptr;
        detach_storage();
    }
}  // namespace hgraph
