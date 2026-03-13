#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/value/state.h>
#include <hgraph/types/time_series/value/value.h>

#include <cstring>
#include <stdexcept>

namespace hgraph
{
    Value::Value(const value::TypeMeta *schema, MutationTracking tracking)
        : Value(*schema, tracking)
    {
        if (schema == nullptr) { throw std::invalid_argument("Value requires a non-null schema"); }
    }

    Value::Value(const value::TypeMeta &schema, MutationTracking tracking)
        : m_builder(&ValueBuilderFactory::checked_builder_for(&schema, tracking))
    {
        allocate_and_construct();
    }

    Value::Value(const View &view, MutationTracking tracking)
        : Value(*(view.schema() != nullptr ? view.schema() : throw std::invalid_argument("Value(View) requires a schema-bound view")),
                tracking)
    {
        if (!view.has_value()) { return; }
        this->view().copy_from(view);
    }

    Value::Value(const Value &other)
        : m_builder(other.m_builder)
    {
        if (m_builder == nullptr) { return; }
        if (!other.has_value()) { return; }

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
        if (m_builder == nullptr) { return; }
        if (!other.has_value()) { return; }

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
        if (other.m_builder == nullptr) {
            clear_storage();
            m_builder = nullptr;
            return *this;
        }
        if (m_builder == nullptr) {
            m_builder = other.m_builder;
            if (!other.has_value()) { return *this; }
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
            return *this;
        }
        if (&builder() != &other.builder()) {
            throw std::invalid_argument("Value copy assignment requires matching builder");
        }

        if (!other.has_value()) {
            clear_storage();
            return *this;
        }

        if (builder().stores_inline_in_value_handle()) {
            if (has_value()) { clear_storage(); }
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

        clear_storage();
        m_storage.heap_memory = replacement;
        return *this;
    }

    Value &Value::operator=(Value &&other)
    {
        if (this == &other) { return *this; }
        if (other.m_builder == nullptr) {
            clear_storage();
            m_builder = nullptr;
            return *this;
        }
        if (m_builder == nullptr) {
            m_builder = other.m_builder;
            if (!other.has_value()) { return *this; }
            if (builder().stores_inline_in_value_handle()) {
                builder().move_construct(storage_memory(), other.storage_memory(), other.builder());
            } else {
                m_storage.heap_memory = other.m_storage.heap_memory;
                other.m_storage.heap_memory = nullptr;
            }
            return *this;
        }
        if (&builder() != &other.builder()) {
            throw std::invalid_argument("Value move assignment requires matching builder");
        }

        clear_storage();
        if (!other.has_value()) { return *this; }

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
        clear_storage();
    }

    bool Value::has_value() const noexcept
    {
        return m_builder != nullptr && (builder().stores_inline_in_value_handle() || m_storage.heap_memory != nullptr);
    }

    Value::operator bool() const noexcept
    {
        return has_value();
    }

    const value::TypeMeta *Value::schema() const noexcept
    {
        return m_builder != nullptr ? &builder().schema() : nullptr;
    }

    MutationTracking Value::tracking() const noexcept
    {
        return m_builder != nullptr ? builder().tracking() : MutationTracking::Plain;
    }

    View Value::view() noexcept
    {
        return m_builder != nullptr ? View{&builder().dispatch(), has_value() ? storage_memory() : nullptr, schema()}
                                 : View::invalid_for(nullptr);
    }

    View Value::view() const noexcept
    {
        return m_builder != nullptr
                   ? View{&builder().dispatch(), has_value() ? const_cast<void *>(storage_memory()) : nullptr, schema()}
                                 : View::invalid_for(nullptr);
    }

    AtomicView Value::atomic_view()
    {
        return view().as_atomic();
    }

    AtomicView Value::atomic_view() const
    {
        return view().as_atomic();
    }

    TupleView Value::tuple_view()
    {
        return view().as_tuple();
    }

    TupleView Value::tuple_view() const
    {
        return view().as_tuple();
    }

    BundleView Value::bundle_view()
    {
        return view().as_bundle();
    }

    BundleView Value::bundle_view() const
    {
        return view().as_bundle();
    }

    ListView Value::list_view()
    {
        return view().as_list();
    }

    ListView Value::list_view() const
    {
        return view().as_list();
    }

    SetView Value::set_view()
    {
        return view().as_set();
    }

    SetView Value::set_view() const
    {
        return view().as_set();
    }

    MapView Value::map_view()
    {
        return view().as_map();
    }

    MapView Value::map_view() const
    {
        return view().as_map();
    }

    CyclicBufferView Value::cyclic_buffer_view()
    {
        return view().as_cyclic_buffer();
    }

    CyclicBufferView Value::cyclic_buffer_view() const
    {
        return view().as_cyclic_buffer();
    }

    QueueView Value::queue_view()
    {
        return view().as_queue();
    }

    QueueView Value::queue_view() const
    {
        return view().as_queue();
    }

    bool Value::equals(const Value &other) const
    {
        return view().equals(other.view());
    }

    bool Value::equals(const View &other) const
    {
        return view().equals(other);
    }

    size_t Value::hash() const
    {
        return view().hash();
    }

    std::string Value::to_string() const
    {
        return view().to_string();
    }

    nb::object Value::to_python() const
    {
        return view().to_python();
    }

    void Value::from_python(const nb::object &src)
    {
        if (src.is_none()) {
            reset();
            return;
        }
        if (!has_value()) { allocate_and_construct(); }
        view().from_python(src);
    }

    void Value::reset()
    {
        clear_storage();
        allocate_and_construct();
    }

    void Value::allocate_and_construct()
    {
        if (m_builder == nullptr) { throw std::runtime_error("Cannot materialize a schema-less Value"); }
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

    void Value::clear_storage() noexcept
    {
        if (m_builder == nullptr) { return; }
        if (!has_value()) { return; }

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
