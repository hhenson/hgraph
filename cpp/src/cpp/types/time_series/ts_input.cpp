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
        : m_active_trie(other.m_active_trie.deep_copy())
    {
        if (other.m_builder != nullptr) { other.builder().copy_construct_input(*this, other); }
    }

    TSInput::TSInput(TSInput &&other) noexcept
        : m_active_trie(std::move(other.m_active_trie))
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
        m_active_trie = std::move(other.m_active_trie);
        m_builder = other.m_builder;
        m_storage = other.m_storage;
        if (m_builder != nullptr) { rebind_builder(builder().ts_value_builder(), StorageOwnership::External); }
        if (storage_memory() != nullptr) { attach_storage(storage_memory()); }

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

    TSInputView TSInput::view(Notifiable *scheduling_notifier, engine_time_t evaluation_time)
    {
        TSViewContext context{view_context()};
        context.active_pos = ActiveTriePosition{&m_active_trie, m_active_trie.root_node()};
        context.scheduling_notifier = scheduling_notifier;
        context.input_view_ops = &detail::default_input_view_ops();
        return TSInputView{context, TSViewContext::none(), evaluation_time};
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
