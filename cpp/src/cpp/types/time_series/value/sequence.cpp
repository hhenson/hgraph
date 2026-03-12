#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/value/sequence.h>
#include <hgraph/types/time_series/value/state.h>

#include <algorithm>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>

namespace hgraph
{

    namespace detail
    {

        struct CyclicBufferState
        {
            std::byte *elements{nullptr};
            size_t     size{0};
            size_t     head{0};
        };

        struct QueueState
        {
            std::byte *elements{nullptr};
            size_t     size{0};
            size_t     capacity{0};
            size_t     head{0};
        };

        struct SequenceDispatchBase
        {
            explicit SequenceDispatchBase(const value::TypeMeta &schema)
                : m_schema(schema),
                  m_element_builder(ValueBuilderFactory::checked_builder_for(schema.element_type))
            {
                if (schema.element_type == nullptr) {
                    throw std::runtime_error("Sequence schema requires an element schema");
                }
            }

            [[nodiscard]] size_t element_stride() const noexcept
            {
                const size_t alignment = m_element_builder.get().alignment();
                const size_t size = m_element_builder.get().size();
                return ((size + alignment - 1) / alignment) * alignment;
            }

            [[nodiscard]] const value::TypeMeta &element_schema() const noexcept
            {
                return *m_schema.get().element_type;
            }

            [[nodiscard]] const ViewDispatch &element_dispatch() const noexcept
            {
                return m_element_builder.get().dispatch();
            }

            void assign_slot(void *slot, const void *value) const
            {
                element_dispatch().assign(slot, value);
            }

            void reset_slot(void *slot) const
            {
                if (m_element_builder.get().requires_destroy()) {
                    m_element_builder.get().destroy(slot);
                }
                m_element_builder.get().construct(slot);
            }

            void construct_slots(std::byte *base, size_t count) const
            {
                for (size_t i = 0; i < count; ++i) {
                    m_element_builder.get().construct(base + i * element_stride());
                }
            }

            void destroy_slots(std::byte *base, size_t count) const noexcept
            {
                if (!m_element_builder.get().requires_destroy()) { return; }
                for (size_t i = 0; i < count; ++i) {
                    m_element_builder.get().destroy(base + i * element_stride());
                }
            }

            void move_slots_linear(std::byte *dst, std::byte *src, size_t count) const
            {
                for (size_t i = 0; i < count; ++i) {
                    m_element_builder.get().move_construct(dst + i * element_stride(),
                                                           src + i * element_stride(),
                                                           m_element_builder);
                }
            }

            std::reference_wrapper<const value::TypeMeta> m_schema;
            std::reference_wrapper<const ValueBuilder>    m_element_builder;
        };

        struct CyclicBufferDispatch final : CyclicBufferViewDispatch, SequenceDispatchBase
        {
            explicit CyclicBufferDispatch(const value::TypeMeta &schema)
                : SequenceDispatchBase(schema)
            {
            }

            [[nodiscard]] size_t size(const void *data) const noexcept override { return state(data)->size; }
            [[nodiscard]] size_t capacity() const noexcept override { return m_schema.get().fixed_size; }
            [[nodiscard]] const value::TypeMeta &element_schema() const noexcept override { return SequenceDispatchBase::element_schema(); }
            [[nodiscard]] const ViewDispatch &element_dispatch() const noexcept override { return SequenceDispatchBase::element_dispatch(); }

            [[nodiscard]] void *element_data(void *data, size_t index) const override
            {
                const CyclicBufferState *buffer = state(data);
                if (index >= buffer->size) { throw std::out_of_range("CyclicBuffer index out of range"); }
                return buffer->elements + physical_index(*buffer, index) * element_stride();
            }

            [[nodiscard]] const void *element_data(const void *data, size_t index) const override
            {
                const CyclicBufferState *buffer = state(data);
                if (index >= buffer->size) { throw std::out_of_range("CyclicBuffer index out of range"); }
                return buffer->elements + physical_index(*buffer, index) * element_stride();
            }

            void set_at(void *data, size_t index, const void *value) const override
            {
                assign_slot(element_data(data, index), value);
            }

            void push(void *data, const void *value) const override
            {
                CyclicBufferState *buffer = state(data);
                if (capacity() == 0) { throw std::runtime_error("Cannot push to zero-capacity cyclic buffer"); }

                if (buffer->size < capacity()) {
                    const size_t tail = (buffer->head + buffer->size) % capacity();
                    assign_slot(buffer->elements + tail * element_stride(), value);
                    ++buffer->size;
                    return;
                }

                const size_t overwrite = buffer->head;
                reset_slot(buffer->elements + overwrite * element_stride());
                assign_slot(buffer->elements + overwrite * element_stride(), value);
                buffer->head = (buffer->head + 1) % capacity();
            }

            void pop(void *data) const override
            {
                CyclicBufferState *buffer = state(data);
                if (buffer->size == 0) { throw std::out_of_range("pop_front on empty cyclic buffer"); }
                reset_slot(buffer->elements + buffer->head * element_stride());
                buffer->head = capacity() == 0 ? 0 : (buffer->head + 1) % capacity();
                --buffer->size;
                if (buffer->size == 0) { buffer->head = 0; }
            }

            void clear(void *data) const override
            {
                CyclicBufferState *buffer = state(data);
                for (size_t i = 0; i < buffer->size; ++i) {
                    reset_slot(buffer->elements + physical_index(*buffer, i) * element_stride());
                }
                buffer->size = 0;
                buffer->head = 0;
            }

            [[nodiscard]] size_t hash(const void *data) const override
            {
                size_t result = 0;
                for (size_t i = 0; i < size(data); ++i) {
                    const size_t element_hash = element_dispatch().hash(element_data(data, i));
                    result ^= element_hash + 0x9e3779b9 + (result << 6) + (result >> 2);
                }
                return result;
            }

            [[nodiscard]] std::string to_string(const void *data) const override
            {
                std::string result = "CyclicBuffer[";
                for (size_t i = 0; i < size(data); ++i) {
                    if (i > 0) { result += ", "; }
                    result += element_dispatch().to_string(element_data(data, i));
                }
                result += "]";
                return result;
            }

            [[nodiscard]] std::partial_ordering compare(const void *lhs, const void *rhs) const override
            {
                const size_t lhs_size = size(lhs);
                const size_t rhs_size = size(rhs);
                const size_t count = std::min(lhs_size, rhs_size);
                for (size_t i = 0; i < count; ++i) {
                    const std::partial_ordering order = element_dispatch().compare(element_data(lhs, i), element_data(rhs, i));
                    if (std::is_lt(order) || std::is_gt(order) || order == std::partial_ordering::unordered) {
                        return order;
                    }
                }
                if (lhs_size < rhs_size) { return std::partial_ordering::less; }
                if (lhs_size > rhs_size) { return std::partial_ordering::greater; }
                return std::partial_ordering::equivalent;
            }

            [[nodiscard]] nb::object to_python(const void *data, const value::TypeMeta *schema) const override
            {
                static_cast<void>(schema);
                nb::list result;
                for (size_t i = 0; i < size(data); ++i) {
                    result.append(element_dispatch().to_python(element_data(data, i), &element_schema()));
                }
                return result;
            }

            void from_python(void *dst, const nb::object &src, const value::TypeMeta *schema) const override
            {
                static_cast<void>(schema);
                if (!nb::isinstance<nb::list>(src) && !nb::isinstance<nb::tuple>(src)) {
                    throw std::runtime_error("Cyclic buffer value expects a list or tuple");
                }

                clear(dst);
                nb::iterator it = nb::iter(src);
                while (it != nb::iterator::sentinel()) {
                    void *temp = m_element_builder.get().allocate();
                    try {
                        m_element_builder.get().construct(temp);
                        element_dispatch().from_python(temp, nb::borrow<nb::object>(*it), &element_schema());
                        push(dst, temp);
                        if (m_element_builder.get().requires_destroy()) {
                            m_element_builder.get().destroy(temp);
                        }
                        m_element_builder.get().deallocate(temp);
                    } catch (...) {
                        if (temp != nullptr) {
                            if (m_element_builder.get().requires_destroy()) {
                                m_element_builder.get().destroy(temp);
                            }
                            m_element_builder.get().deallocate(temp);
                        }
                        throw;
                    }
                    ++it;
                }
            }

            void assign(void *dst, const void *src) const override
            {
                clear(dst);
                for (size_t i = 0; i < size(src); ++i) {
                    push(dst, element_data(src, i));
                }
            }

            void set_from_cpp(void *dst, const void *src, const value::TypeMeta *src_schema) const override
            {
                static_cast<void>(dst);
                static_cast<void>(src);
                static_cast<void>(src_schema);
                throw std::invalid_argument("Cyclic buffer set_from_cpp is not implemented");
            }

            void move_from_cpp(void *dst, void *src, const value::TypeMeta *src_schema) const override
            {
                static_cast<void>(dst);
                static_cast<void>(src);
                static_cast<void>(src_schema);
                throw std::invalid_argument("Cyclic buffer move_from_cpp is not implemented");
            }

            void construct(void *memory) const
            {
                CyclicBufferState *buffer = state(memory);
                std::construct_at(buffer);
                if (capacity() == 0) { return; }
                buffer->elements = static_cast<std::byte *>(
                    ::operator new(capacity() * element_stride(), std::align_val_t{m_element_builder.get().alignment()}));
                construct_slots(buffer->elements, capacity());
            }

            void destroy(void *memory) const noexcept
            {
                CyclicBufferState *buffer = state(memory);
                if (buffer->elements != nullptr) {
                    destroy_slots(buffer->elements, capacity());
                    ::operator delete(buffer->elements, std::align_val_t{m_element_builder.get().alignment()});
                }
                std::destroy_at(buffer);
            }

            void copy_construct(void *dst, const void *src) const
            {
                construct(dst);
                assign(dst, src);
            }

            void move_construct(void *dst, void *src) const
            {
                std::construct_at(state(dst), std::move(*state(src)));
                state(src)->elements = nullptr;
                state(src)->size = 0;
                state(src)->head = 0;
            }

          private:
            [[nodiscard]] size_t physical_index(const CyclicBufferState &buffer, size_t logical_index) const noexcept
            {
                return capacity() == 0 ? 0 : (buffer.head + logical_index) % capacity();
            }

            [[nodiscard]] static CyclicBufferState *state(void *memory) noexcept
            {
                return std::launder(reinterpret_cast<CyclicBufferState *>(memory));
            }

            [[nodiscard]] static const CyclicBufferState *state(const void *memory) noexcept
            {
                return std::launder(reinterpret_cast<const CyclicBufferState *>(memory));
            }
        };

        struct QueueDispatch final : QueueViewDispatch, SequenceDispatchBase
        {
            explicit QueueDispatch(const value::TypeMeta &schema)
                : SequenceDispatchBase(schema)
            {
            }

            [[nodiscard]] size_t size(const void *data) const noexcept override { return state(data)->size; }
            [[nodiscard]] size_t max_capacity() const noexcept override { return m_schema.get().fixed_size; }
            [[nodiscard]] const value::TypeMeta &element_schema() const noexcept override { return SequenceDispatchBase::element_schema(); }
            [[nodiscard]] const ViewDispatch &element_dispatch() const noexcept override { return SequenceDispatchBase::element_dispatch(); }

            [[nodiscard]] void *element_data(void *data, size_t index) const override
            {
                QueueState *queue = state(data);
                if (index >= queue->size) { throw std::out_of_range("Queue index out of range"); }
                return queue->elements + physical_index(*queue, index) * element_stride();
            }

            [[nodiscard]] const void *element_data(const void *data, size_t index) const override
            {
                const QueueState *queue = state(data);
                if (index >= queue->size) { throw std::out_of_range("Queue index out of range"); }
                return queue->elements + physical_index(*queue, index) * element_stride();
            }

            void push(void *data, const void *value) const override
            {
                QueueState *queue = state(data);
                if (queue->capacity == 0) {
                    reserve(queue, max_capacity() > 0 ? max_capacity() : 4);
                } else if (max_capacity() > 0 && queue->size == max_capacity()) {
                    pop(data);
                } else if (queue->size == queue->capacity) {
                    const size_t grown = max_capacity() > 0 ? max_capacity() : queue->capacity * 2;
                    reserve(queue, std::max<size_t>(grown, queue->capacity + 1));
                }

                const size_t tail = (queue->head + queue->size) % queue->capacity;
                assign_slot(queue->elements + tail * element_stride(), value);
                ++queue->size;
            }

            void pop(void *data) const override
            {
                QueueState *queue = state(data);
                if (queue->size == 0) { throw std::out_of_range("pop_front on empty queue"); }
                reset_slot(queue->elements + queue->head * element_stride());
                queue->head = queue->capacity == 0 ? 0 : (queue->head + 1) % queue->capacity;
                --queue->size;
                if (queue->size == 0) { queue->head = 0; }
            }

            void clear(void *data) const override
            {
                QueueState *queue = state(data);
                for (size_t i = 0; i < queue->size; ++i) {
                    reset_slot(queue->elements + physical_index(*queue, i) * element_stride());
                }
                queue->size = 0;
                queue->head = 0;
            }

            [[nodiscard]] size_t hash(const void *data) const override
            {
                size_t result = 0;
                for (size_t i = 0; i < size(data); ++i) {
                    const size_t element_hash = element_dispatch().hash(element_data(data, i));
                    result ^= element_hash + 0x9e3779b9 + (result << 6) + (result >> 2);
                }
                return result;
            }

            [[nodiscard]] std::string to_string(const void *data) const override
            {
                std::string result = "Queue[";
                for (size_t i = 0; i < size(data); ++i) {
                    if (i > 0) { result += ", "; }
                    result += element_dispatch().to_string(element_data(data, i));
                }
                result += "]";
                return result;
            }

            [[nodiscard]] std::partial_ordering compare(const void *lhs, const void *rhs) const override
            {
                const size_t lhs_size = size(lhs);
                const size_t rhs_size = size(rhs);
                const size_t count = std::min(lhs_size, rhs_size);
                for (size_t i = 0; i < count; ++i) {
                    const std::partial_ordering order = element_dispatch().compare(element_data(lhs, i), element_data(rhs, i));
                    if (std::is_lt(order) || std::is_gt(order) || order == std::partial_ordering::unordered) {
                        return order;
                    }
                }
                if (lhs_size < rhs_size) { return std::partial_ordering::less; }
                if (lhs_size > rhs_size) { return std::partial_ordering::greater; }
                return std::partial_ordering::equivalent;
            }

            [[nodiscard]] nb::object to_python(const void *data, const value::TypeMeta *schema) const override
            {
                static_cast<void>(schema);
                nb::list result;
                for (size_t i = 0; i < size(data); ++i) {
                    result.append(element_dispatch().to_python(element_data(data, i), &element_schema()));
                }
                return result;
            }

            void from_python(void *dst, const nb::object &src, const value::TypeMeta *schema) const override
            {
                static_cast<void>(schema);
                if (!nb::isinstance<nb::list>(src) && !nb::isinstance<nb::tuple>(src)) {
                    throw std::runtime_error("Queue value expects a list or tuple");
                }

                clear(dst);
                nb::iterator it = nb::iter(src);
                while (it != nb::iterator::sentinel()) {
                    void *temp = m_element_builder.get().allocate();
                    try {
                        m_element_builder.get().construct(temp);
                        element_dispatch().from_python(temp, nb::borrow<nb::object>(*it), &element_schema());
                        push(dst, temp);
                        if (m_element_builder.get().requires_destroy()) {
                            m_element_builder.get().destroy(temp);
                        }
                        m_element_builder.get().deallocate(temp);
                    } catch (...) {
                        if (temp != nullptr) {
                            if (m_element_builder.get().requires_destroy()) {
                                m_element_builder.get().destroy(temp);
                            }
                            m_element_builder.get().deallocate(temp);
                        }
                        throw;
                    }
                    ++it;
                }
            }

            void assign(void *dst, const void *src) const override
            {
                clear(dst);
                for (size_t i = 0; i < size(src); ++i) {
                    push(dst, element_data(src, i));
                }
            }

            void set_from_cpp(void *dst, const void *src, const value::TypeMeta *src_schema) const override
            {
                static_cast<void>(dst);
                static_cast<void>(src);
                static_cast<void>(src_schema);
                throw std::invalid_argument("Queue value set_from_cpp is not implemented");
            }

            void move_from_cpp(void *dst, void *src, const value::TypeMeta *src_schema) const override
            {
                static_cast<void>(dst);
                static_cast<void>(src);
                static_cast<void>(src_schema);
                throw std::invalid_argument("Queue value move_from_cpp is not implemented");
            }

            void construct(void *memory) const
            {
                std::construct_at(state(memory));
            }

            void destroy(void *memory) const noexcept
            {
                QueueState *queue = state(memory);
                if (queue->elements != nullptr) {
                    if (queue->capacity > 0) {
                        destroy_slots(queue->elements, queue->capacity);
                    }
                    ::operator delete(queue->elements, std::align_val_t{m_element_builder.get().alignment()});
                }
                std::destroy_at(queue);
            }

            void copy_construct(void *dst, const void *src) const
            {
                construct(dst);
                assign(dst, src);
            }

            void move_construct(void *dst, void *src) const
            {
                std::construct_at(state(dst), std::move(*state(src)));
                state(src)->elements = nullptr;
                state(src)->size = 0;
                state(src)->capacity = 0;
                state(src)->head = 0;
            }

          private:
            [[nodiscard]] size_t physical_index(const QueueState &queue, size_t logical_index) const noexcept
            {
                return queue.capacity == 0 ? 0 : (queue.head + logical_index) % queue.capacity;
            }

            void reserve(QueueState *queue, size_t min_capacity) const
            {
                if (min_capacity <= queue->capacity) { return; }
                if (max_capacity() > 0 && min_capacity > max_capacity()) {
                    min_capacity = max_capacity();
                }
                if (min_capacity <= queue->capacity) { return; }

                std::byte *new_elements = static_cast<std::byte *>(
                    ::operator new(min_capacity * element_stride(), std::align_val_t{m_element_builder.get().alignment()}));
                construct_slots(new_elements, min_capacity);

                for (size_t i = 0; i < queue->size; ++i) {
                    assign_slot(new_elements + i * element_stride(), element_data(queue, i));
                }

                if (queue->elements != nullptr) {
                    destroy_slots(queue->elements, queue->capacity);
                    ::operator delete(queue->elements, std::align_val_t{m_element_builder.get().alignment()});
                }

                queue->elements = new_elements;
                queue->capacity = min_capacity;
                queue->head = 0;
            }

            [[nodiscard]] static QueueState *state(void *memory) noexcept
            {
                return std::launder(reinterpret_cast<QueueState *>(memory));
            }

            [[nodiscard]] static const QueueState *state(const void *memory) noexcept
            {
                return std::launder(reinterpret_cast<const QueueState *>(memory));
            }
        };

        template <typename TDispatch> struct SequenceStateOps final : StateOps
        {
            explicit SequenceStateOps(const TDispatch &dispatch) noexcept
                : m_dispatch(dispatch)
            {
            }

            void expand_builder(ValueBuilder &builder, const value::TypeMeta &schema) const noexcept override
            {
                static_cast<void>(schema);
                if constexpr (std::is_same_v<TDispatch, CyclicBufferDispatch>) {
                    builder.cache_layout(sizeof(CyclicBufferState), alignof(CyclicBufferState));
                } else {
                    builder.cache_layout(sizeof(QueueState), alignof(QueueState));
                }
                builder.cache_lifecycle(true, true, false);
            }

            [[nodiscard]] const ViewDispatch &view_dispatch(const value::TypeMeta &schema) const noexcept override
            {
                static_cast<void>(schema);
                return m_dispatch;
            }

            [[nodiscard]] bool requires_destroy(const value::TypeMeta &schema) const noexcept override
            {
                static_cast<void>(schema);
                return true;
            }

            [[nodiscard]] bool requires_deallocate(const value::TypeMeta &schema) const noexcept override
            {
                static_cast<void>(schema);
                return true;
            }

            [[nodiscard]] bool stores_inline_in_value_handle(const value::TypeMeta &schema) const noexcept override
            {
                static_cast<void>(schema);
                return false;
            }

            void construct(void *memory) const override { m_dispatch.get().construct(memory); }
            void destroy(void *memory) const noexcept override { m_dispatch.get().destroy(memory); }
            void copy_construct(void *dst, const void *src) const override { m_dispatch.get().copy_construct(dst, src); }
            void move_construct(void *dst, void *src) const override { m_dispatch.get().move_construct(dst, src); }

            std::reference_wrapper<const TDispatch> m_dispatch;
        };

        struct CachedBuilderEntry
        {
            std::shared_ptr<const ViewDispatch> dispatch;
            std::shared_ptr<const StateOps>     state_ops;
            std::shared_ptr<const ValueBuilder> builder;
        };

        const ValueBuilder *sequence_builder_for(const value::TypeMeta *schema)
        {
            if (schema == nullptr) { return nullptr; }
            if (schema->kind != value::TypeKind::CyclicBuffer && schema->kind != value::TypeKind::Queue) { return nullptr; }

            static std::mutex cache_mutex;
            static std::unordered_map<const value::TypeMeta *, CachedBuilderEntry> cache;

            std::lock_guard lock(cache_mutex);
            if (auto it = cache.find(schema); it != cache.end()) {
                return it->second.builder.get();
            }

            CachedBuilderEntry entry;
            if (schema->kind == value::TypeKind::CyclicBuffer) {
                auto dispatch = std::make_shared<CyclicBufferDispatch>(*schema);
                auto state_ops = std::make_shared<SequenceStateOps<CyclicBufferDispatch>>(*dispatch);
                auto builder = std::make_shared<ValueBuilder>(*schema, *state_ops);
                entry.dispatch = std::move(dispatch);
                entry.state_ops = std::move(state_ops);
                entry.builder = std::move(builder);
            } else {
                auto dispatch = std::make_shared<QueueDispatch>(*schema);
                auto state_ops = std::make_shared<SequenceStateOps<QueueDispatch>>(*dispatch);
                auto builder = std::make_shared<ValueBuilder>(*schema, *state_ops);
                entry.dispatch = std::move(dispatch);
                entry.state_ops = std::move(state_ops);
                entry.builder = std::move(builder);
            }

            auto [it, inserted] = cache.emplace(schema, std::move(entry));
            static_cast<void>(inserted);
            return it->second.builder.get();
        }

    }  // namespace detail

    BufferView::BufferView(const View &view)
        : View(view)
    {
        if (!view.valid()) { return; }
        if (view.schema() == nullptr ||
            (view.schema()->kind != value::TypeKind::CyclicBuffer && view.schema()->kind != value::TypeKind::Queue)) {
            throw std::runtime_error("BufferView requires a cyclic buffer or queue schema");
        }
    }

    size_t BufferView::size() const
    {
        const auto *dispatch = buffer_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("BufferView::size on invalid view"); }
        return dispatch->size(data());
    }

    bool BufferView::empty() const { return size() == 0; }
    const value::TypeMeta *BufferView::element_schema() const
    {
        const auto *dispatch = buffer_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("BufferView::element_schema on invalid view"); }
        return &dispatch->element_schema();
    }
    View BufferView::element_at(size_t index)
    {
        const auto *dispatch = buffer_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("BufferView::element_at on invalid view"); }
        return View{&dispatch->element_dispatch(), dispatch->element_data(data(), index), &dispatch->element_schema()};
    }
    View BufferView::element_at(size_t index) const
    {
        const auto *dispatch = buffer_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("BufferView::element_at on invalid view"); }
        return View{&dispatch->element_dispatch(), const_cast<void *>(dispatch->element_data(data(), index)), &dispatch->element_schema()};
    }
    View BufferView::front() { return element_at(0); }
    View BufferView::front() const { return element_at(0); }
    View BufferView::back() { return element_at(size() - 1); }
    View BufferView::back() const { return element_at(size() - 1); }
    void BufferView::push(const View &value)
    {
        const auto *dispatch = buffer_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("BufferView::push on invalid view"); }
        if (!value.valid() || value.schema() != &dispatch->element_schema()) {
            throw std::invalid_argument("BufferView::push requires a valid matching-schema value");
        }
        dispatch->push(data(), data_of(value));
    }
    void BufferView::pop()
    {
        const auto *dispatch = buffer_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("BufferView::pop on invalid view"); }
        dispatch->pop(data());
    }
    void BufferView::clear()
    {
        const auto *dispatch = buffer_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("BufferView::clear on invalid view"); }
        dispatch->clear(data());
    }
    const detail::BufferViewDispatch *BufferView::buffer_dispatch() const noexcept
    {
        return valid() ? static_cast<const detail::BufferViewDispatch *>(dispatch()) : nullptr;
    }

    CyclicBufferView::CyclicBufferView(const View &view)
        : BufferView(view)
    {
        if (!view.valid()) { return; }
        if (view.schema() == nullptr || view.schema()->kind != value::TypeKind::CyclicBuffer) {
            throw std::runtime_error("CyclicBufferView requires a cyclic buffer schema");
        }
    }

    size_t CyclicBufferView::capacity() const
    {
        const auto *dispatch = cyclic_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("CyclicBufferView::capacity on invalid view"); }
        return dispatch->capacity();
    }
    bool CyclicBufferView::full() const { return size() == capacity(); }
    View CyclicBufferView::at(size_t index) { return element_at(index); }
    View CyclicBufferView::at(size_t index) const { return element_at(index); }
    View CyclicBufferView::operator[](size_t index) { return at(index); }
    View CyclicBufferView::operator[](size_t index) const { return at(index); }
    void CyclicBufferView::set(size_t index, const View &value)
    {
        const auto *dispatch = cyclic_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("CyclicBufferView::set on invalid view"); }
        if (!value.valid() || value.schema() != &dispatch->element_schema()) {
            throw std::invalid_argument("CyclicBufferView::set requires a valid matching-schema value");
        }
        dispatch->set_at(data(), index, data_of(value));
    }
    const detail::CyclicBufferViewDispatch *CyclicBufferView::cyclic_dispatch() const noexcept
    {
        return valid() ? static_cast<const detail::CyclicBufferViewDispatch *>(dispatch()) : nullptr;
    }

    QueueView::QueueView(const View &view)
        : BufferView(view)
    {
        if (!view.valid()) { return; }
        if (view.schema() == nullptr || view.schema()->kind != value::TypeKind::Queue) {
            throw std::runtime_error("QueueView requires a queue schema");
        }
    }

    size_t QueueView::max_capacity() const
    {
        const auto *dispatch = queue_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("QueueView::max_capacity on invalid view"); }
        return dispatch->max_capacity();
    }
    bool QueueView::has_max_capacity() const noexcept { return valid() && max_capacity() > 0; }
    const detail::QueueViewDispatch *QueueView::queue_dispatch() const noexcept
    {
        return valid() ? static_cast<const detail::QueueViewDispatch *>(dispatch()) : nullptr;
    }

}  // namespace hgraph
