#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/value/state.h>
#include <hgraph/types/time_series/value/value.h>

#include <cstring>
#include <stdexcept>

namespace hgraph
{
    Value::Value(const value::TypeMeta &schema)
        : m_builder(&ValueBuilderFactory::checked_builder_for(&schema))
    {
        allocate_and_construct();
    }

    Value::Value(const Value &other)
        : m_builder(other.m_builder)
    {
        if (!other.valid()) { return; }

        if (builder().stores_inline_in_value_handle()) {
            builder().copy_construct(storage_memory(), other.storage_memory(), other.builder());
        } else {
            m_storage.heap_memory = builder().allocate();
            try {
                builder().copy_construct(m_storage.heap_memory, other.storage_memory(), other.builder());
            } catch (...) {
                builder().deallocate(m_storage.heap_memory);
                m_storage.heap_memory = nullptr;
                throw;
            }
        }
    }

    Value::Value(Value &&other) noexcept
        : m_builder(other.m_builder)
    {
        if (!other.valid()) { return; }

        if (builder().stores_inline_in_value_handle()) {
            builder().move_construct(storage_memory(), other.storage_memory(), other.builder());
        } else {
            m_storage.heap_memory = other.m_storage.heap_memory;
            other.m_storage.heap_memory = nullptr;
        }
    }

    Value &Value::operator=(const Value &other)
    {
        if (this == &other) { return *this; }
        if (&builder() != &other.builder()) {
            throw std::invalid_argument("Value copy assignment requires matching builder");
        }

        if (!other.valid()) {
            reset();
            return *this;
        }

        if (builder().stores_inline_in_value_handle()) {
            if (valid()) { reset(); }
            builder().copy_construct(storage_memory(), other.storage_memory(), other.builder());
            return *this;
        }

        void *replacement = builder().allocate();
        try {
            builder().copy_construct(replacement, other.storage_memory(), other.builder());
        } catch (...) {
            builder().deallocate(replacement);
            throw;
        }

        reset();
        m_storage.heap_memory = replacement;
        return *this;
    }

    Value &Value::operator=(Value &&other)
    {
        if (this == &other) { return *this; }
        if (&builder() != &other.builder()) {
            throw std::invalid_argument("Value move assignment requires matching builder");
        }

        reset();
        if (!other.valid()) { return *this; }

        if (builder().stores_inline_in_value_handle()) {
            builder().move_construct(storage_memory(), other.storage_memory(), other.builder());
        } else {
            m_storage.heap_memory = other.m_storage.heap_memory;
            other.m_storage.heap_memory = nullptr;
        }
        return *this;
    }

    Value::~Value()
    {
        reset();
    }

    bool Value::valid() const noexcept
    {
        return builder().stores_inline_in_value_handle() || m_storage.heap_memory != nullptr;
    }

    Value::operator bool() const noexcept
    {
        return valid();
    }

    const value::TypeMeta *Value::schema() const noexcept
    {
        return &builder().schema();
    }

    View Value::view() noexcept
    {
        return View{&builder().dispatch(), valid() ? storage_memory() : nullptr, schema()};
    }

    View Value::view() const noexcept
    {
        return View{&builder().dispatch(), valid() ? const_cast<void *>(storage_memory()) : nullptr, schema()};
    }

    AtomicView Value::atomic_view() noexcept
    {
        return view().as_atomic();
    }

    AtomicView Value::atomic_view() const noexcept
    {
        return view().as_atomic();
    }

    TupleView Value::tuple_view() noexcept
    {
        return view().as_tuple();
    }

    TupleView Value::tuple_view() const noexcept
    {
        return view().as_tuple();
    }

    BundleView Value::bundle_view() noexcept
    {
        return view().as_bundle();
    }

    BundleView Value::bundle_view() const noexcept
    {
        return view().as_bundle();
    }

    ListView Value::list_view() noexcept
    {
        return view().as_list();
    }

    ListView Value::list_view() const noexcept
    {
        return view().as_list();
    }

    SetView Value::set_view() noexcept
    {
        return view().as_set();
    }

    SetView Value::set_view() const noexcept
    {
        return view().as_set();
    }

    MapView Value::map_view() noexcept
    {
        return view().as_map();
    }

    MapView Value::map_view() const noexcept
    {
        return view().as_map();
    }

    CyclicBufferView Value::cyclic_buffer_view() noexcept
    {
        return view().as_cyclic_buffer();
    }

    CyclicBufferView Value::cyclic_buffer_view() const noexcept
    {
        return view().as_cyclic_buffer();
    }

    QueueView Value::queue_view() noexcept
    {
        return view().as_queue();
    }

    QueueView Value::queue_view() const noexcept
    {
        return view().as_queue();
    }

    void Value::allocate_and_construct()
    {
        if (builder().stores_inline_in_value_handle()) {
            builder().construct(storage_memory());
            return;
        }

        m_storage.heap_memory = builder().allocate();
        try {
            builder().construct(m_storage.heap_memory);
        } catch (...) {
            builder().deallocate(m_storage.heap_memory);
            m_storage.heap_memory = nullptr;
            throw;
        }
    }

    void Value::reset() noexcept
    {
        if (!valid()) { return; }

        if (builder().requires_destroy()) {
            builder().destroy(storage_memory());
        }
        if (builder().requires_deallocate() && !builder().stores_inline_in_value_handle()) {
            builder().deallocate(m_storage.heap_memory);
            m_storage.heap_memory = nullptr;
        }
    }

    void *Value::storage_memory() noexcept
    {
        return builder().stores_inline_in_value_handle()
                   ? static_cast<void *>(m_storage.inline_storage.data())
                   : m_storage.heap_memory;
    }

    const void *Value::storage_memory() const noexcept
    {
        return builder().stores_inline_in_value_handle()
                   ? static_cast<const void *>(m_storage.inline_storage.data())
                   : m_storage.heap_memory;
    }

    const ValueBuilder &Value::builder() const noexcept
    {
        return *m_builder;
    }

}  // namespace hgraph
