#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/ts_input.h>

#include <stdexcept>

namespace hgraph
{
    TSInput::TSInput(const TSInputBuilder &builder)
    {
        builder.construct_input(*this);
    }

    TSInput::TSInput(const TSInput &other)
    {
        if (other.m_builder != nullptr) { other.builder().copy_construct_input(*this, other); }
    }

    TSInput::TSInput(TSInput &&other) noexcept
    {
        if (other.m_builder != nullptr) { other.builder().move_construct_input(*this, other); }
    }

    TSInput &TSInput::operator=(const TSInput &other)
    {
        if (this == &other) { return *this; }
        TSInput replacement(other);
        return *this = std::move(replacement);
    }

    TSInput &TSInput::operator=(TSInput &&other) noexcept
    {
        if (this == &other) { return *this; }

        clear_storage();
        reset_binding();
        m_builder = nullptr;
        m_builder = other.m_builder;
        m_storage = other.m_storage;
        if (m_builder != nullptr) { rebind_builder(builder().ts_value_builder(), StorageOwnership::External); }
        if (storage_memory() != nullptr) { attach_storage(builder().ts_value_memory(storage_memory())); }

        other.clear_storage_handle();
        other.m_builder = nullptr;
        other.detach_storage();
        other.reset_binding();
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
        if (m_builder == nullptr || storage_memory() == nullptr) { return View::invalid_for(nullptr); }
        return View{&builder().active_builder().dispatch(),
                    storage_memory() != nullptr ? const_cast<void *>(active_memory()) : nullptr,
                    &builder().active_schema()};
    }

    View TSInput::active_state()
    {
        if (m_builder == nullptr || storage_memory() == nullptr) { return View::invalid_for(nullptr); }
        return View{&builder().active_builder().dispatch(), storage_memory() != nullptr ? active_memory() : nullptr, &builder().active_schema()};
    }

    void TSInput::allocate_and_construct()
    {
        builder().construct_input(*this);
    }

    void TSInput::clear_storage() noexcept
    {
        if (m_builder == nullptr || storage_memory() == nullptr) { return; }
        builder().destruct_input(*this);
    }
}  // namespace hgraph
