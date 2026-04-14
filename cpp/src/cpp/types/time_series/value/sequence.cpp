#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/value/sequence.h>
#include <hgraph/types/time_series/value/builder.h>
#include <hgraph/types/time_series/value/value.h>

#include <algorithm>
#include <concepts>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>

namespace hgraph
{
    namespace detail
    {

        /**
         * Header for cyclic-buffer storage.
         *
         * Cyclic buffers have schema-fixed capacity, so their element payload is
         * placed directly after this header in the owning value allocation. The
         * `elements` pointer therefore aliases memory inside the root value block
         * rather than a separately allocated child buffer.
         */
        struct CyclicBufferStateBase
        {
            std::byte *elements{nullptr};
            size_t     size{0};
            size_t     head{0};
        };

        struct PlainCyclicBufferState : CyclicBufferStateBase
        {
        };

        struct DeltaCyclicBufferState : CyclicBufferStateBase
        {
            bool       has_removed{false};
            size_t     mutation_depth{0};
        };

        struct QueueStateBase
        {
            /**
             * Element payload storage.
             *
             * Small queues point into the root value allocation. Once growth
             * exceeds the schema-derived inline capacity, this pointer is
             * rebound to a separately allocated growable buffer.
             */
            std::byte *elements{nullptr};
            size_t     size{0};
            size_t     capacity{0};
            size_t     head{0};
        };

        struct PlainQueueState : QueueStateBase
        {
        };

        struct DeltaQueueState : QueueStateBase
        {
            bool       has_removed{false};
            size_t     mutation_depth{0};
        };

        template <MutationTracking TTracking> struct SequenceDispatchBase
        {
            static constexpr MutationTracking tracking_mode = TTracking;
            static constexpr bool tracks_deltas_v = TTracking == MutationTracking::Delta;

            explicit SequenceDispatchBase(const value::TypeMeta &schema)
                : m_schema(schema),
                  m_element_builder(ValueBuilderFactory::checked_builder_for(schema.element_type, MutationTracking::Plain))
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
                if (m_element_builder.get().requires_destruct()) {
                    m_element_builder.get().destruct(slot);
                }
                m_element_builder.get().construct(slot);
            }

            void retain_removed_slot(void *removed_slot, bool &has_removed, const void *value) const
            {
                if constexpr (!tracks_deltas_v) { return; }
                if (has_removed) { reset_slot(removed_slot); }
                assign_slot(removed_slot, value);
                has_removed = true;
            }

            void clear_removed_slot(void *removed_slot, bool &has_removed) const
            {
                if constexpr (!tracks_deltas_v) { return; }
                if (!has_removed) { return; }
                reset_slot(removed_slot);
                has_removed = false;
            }

            void construct_slots(std::byte *base, size_t count) const
            {
                for (size_t i = 0; i < count; ++i) {
                    m_element_builder.get().construct(base + i * element_stride());
                }
            }

            void destruct_slots(std::byte *base, size_t count) const noexcept
            {
                if (!m_element_builder.get().requires_destruct()) { return; }
                for (size_t i = 0; i < count; ++i) {
                    m_element_builder.get().destruct(base + i * element_stride());
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

        template <MutationTracking TTracking>
        struct CyclicBufferDispatch final : CyclicBufferViewDispatch, SequenceDispatchBase<TTracking>
        {
            static constexpr MutationTracking tracking_mode = TTracking;
            static constexpr bool tracks_deltas_v = TTracking == MutationTracking::Delta;
            using BufferState = std::conditional_t<tracks_deltas_v, DeltaCyclicBufferState, PlainCyclicBufferState>;

            [[nodiscard]] MutationTracking tracking() const noexcept override
            {
                return tracking_mode;
            }

            explicit CyclicBufferDispatch(const value::TypeMeta &schema)
                : SequenceDispatchBase<TTracking>(schema)
            {
            }

            [[nodiscard]] size_t size(const void *data) const noexcept override { return state(data)->size; }
            [[nodiscard]] size_t capacity() const noexcept override { return this->m_schema.get().fixed_size; }
            [[nodiscard]] const value::TypeMeta &element_schema() const noexcept override { return SequenceDispatchBase<TTracking>::element_schema(); }
            [[nodiscard]] const ViewDispatch &element_dispatch() const noexcept override { return SequenceDispatchBase<TTracking>::element_dispatch(); }

            /**
             * Return the total root-allocation size required for the cyclic
             * buffer header plus all schema-fixed element slots.
             */
            [[nodiscard]] size_t allocation_size() const noexcept
            {
                return align_up(sizeof(BufferState), this->m_element_builder.get().alignment()) +
                       storage_slots() * this->element_stride();
            }

            /**
             * Return the alignment required by the header/element block.
             */
            [[nodiscard]] size_t allocation_alignment() const noexcept
            {
                return std::max(alignof(BufferState), this->m_element_builder.get().alignment());
            }

            void begin_mutation(void *data) const override
            {
                if constexpr (tracks_deltas_v) {
                    BufferState *buffer = state(data);
                    if (buffer->mutation_depth++ == 0 && buffer->elements != nullptr) {
                        this->clear_removed_slot(removed_slot_memory(data), buffer->has_removed);
                    }
                }
            }

            void end_mutation(void *data) const override
            {
                if constexpr (tracks_deltas_v) {
                    BufferState *buffer = state(data);
                    if (buffer->mutation_depth == 0) {
                        throw std::runtime_error("Cyclic buffer mutation depth underflow");
                    }
                    --buffer->mutation_depth;
                }
            }

            [[nodiscard]] void *element_data(void *data, size_t index) const override
            {
                const auto *buffer = state(data);
                if (index >= buffer->size) { throw std::out_of_range("CyclicBuffer index out of range"); }
                return buffer->elements + physical_index(*buffer, index) * this->element_stride();
            }

            [[nodiscard]] const void *element_data(const void *data, size_t index) const override
            {
                const auto *buffer = state(data);
                if (index >= buffer->size) { throw std::out_of_range("CyclicBuffer index out of range"); }
                return buffer->elements + physical_index(*buffer, index) * this->element_stride();
            }

            [[nodiscard]] bool has_removed(const void *data) const noexcept override
            {
                if constexpr (tracks_deltas_v) {
                    return state(data)->has_removed;
                } else {
                    static_cast<void>(data);
                    return false;
                }
            }

            [[nodiscard]] void *removed_data(void *data) const override
            {
                BufferState *buffer = state(data);
                if constexpr (!tracks_deltas_v) {
                    static_cast<void>(buffer);
                    throw std::out_of_range("CyclicBuffer has no removed value");
                } else if (!buffer->has_removed) {
                    throw std::out_of_range("CyclicBuffer has no removed value");
                }
                return removed_slot_memory(data);
            }

            [[nodiscard]] const void *removed_data(const void *data) const override
            {
                const BufferState *buffer = state(data);
                if constexpr (!tracks_deltas_v) {
                    static_cast<void>(buffer);
                    throw std::out_of_range("CyclicBuffer has no removed value");
                } else if (!buffer->has_removed) {
                    throw std::out_of_range("CyclicBuffer has no removed value");
                }
                return removed_slot_memory(data);
            }

            void set_at(void *data, size_t index, const void *value) const override
            {
                this->assign_slot(element_data(data, index), value);
            }

            void push(void *data, const void *value) const override
            {
                auto *buffer = state(data);
                if (capacity() == 0) { throw std::runtime_error("Cannot push to zero-capacity cyclic buffer"); }

                if (buffer->size < capacity()) {
                    const size_t tail = (buffer->head + buffer->size) % capacity();
                    this->assign_slot(buffer->elements + tail * this->element_stride(), value);
                    ++buffer->size;
                    return;
                }

                const size_t overwrite = buffer->head;
                if constexpr (tracks_deltas_v) {
                    this->retain_removed_slot(
                        removed_slot_memory(data), removed_flag(data), buffer->elements + overwrite * this->element_stride());
                }
                this->reset_slot(buffer->elements + overwrite * this->element_stride());
                this->assign_slot(buffer->elements + overwrite * this->element_stride(), value);
                buffer->head = (buffer->head + 1) % capacity();
            }

            void pop(void *data) const override
            {
                auto *buffer = state(data);
                if (buffer->size == 0) { throw std::out_of_range("pop_front on empty cyclic buffer"); }
                if constexpr (tracks_deltas_v) {
                    this->retain_removed_slot(
                        removed_slot_memory(data), removed_flag(data), buffer->elements + buffer->head * this->element_stride());
                }
                this->reset_slot(buffer->elements + buffer->head * this->element_stride());
                buffer->head = capacity() == 0 ? 0 : (buffer->head + 1) % capacity();
                --buffer->size;
                if (buffer->size == 0) { buffer->head = 0; }
            }

            void clear(void *data) const override
            {
                auto *buffer = state(data);
                for (size_t i = 0; i < buffer->size; ++i) {
                    const size_t physical = physical_index(*buffer, i);
                    if constexpr (tracks_deltas_v) {
                        this->retain_removed_slot(
                            removed_slot_memory(data), removed_flag(data), buffer->elements + physical * this->element_stride());
                    }
                    this->reset_slot(buffer->elements + physical * this->element_stride());
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
                BufferState *buffer = state(dst);
                nb::iterator it = nb::iter(src);
                size_t imported = 0;
                while (it != nb::iterator::sentinel() && imported < capacity()) {
                    const size_t tail = (buffer->head + buffer->size) % capacity();
                    this->element_dispatch().from_python(
                        buffer->elements + tail * this->element_stride(), nb::borrow<nb::object>(*it), &this->element_schema());
                    ++buffer->size;
                    ++it;
                    ++imported;
                }
            }

            void assign(void *dst, const void *src) const override
            {
                clear(dst);
                for (size_t i = 0; i < size(src); ++i) {
                    push(dst, element_data(src, i));
                }
            }

            void copy_from(void *dst, const View &src) const override
            {
                if (this == detail::ViewAccess::dispatch(src)) {
                    assign(dst, detail::ViewAccess::data(src));
                    return;
                }

                clear(dst);
                const auto source = src.as_cyclic_buffer();
                for (size_t i = 0; i < source.size(); ++i) {
                    Value normalized_element{element_schema(), element_dispatch().tracking()};
                    normalized_element.view().copy_from(source.at(i));
                    push(dst, detail::ViewAccess::data(normalized_element.view()));
                }
            }

            void set_from_cpp(void *dst, const void *src, const value::TypeMeta *src_schema) const override
            {
                if (src_schema == &this->m_schema.get()) {
                    assign(dst, src);
                    return;
                }
                throw std::invalid_argument("Cyclic buffer set_from_cpp requires a matching schema");
            }

            void move_from_cpp(void *dst, void *src, const value::TypeMeta *src_schema) const override
            {
                if (src_schema == &this->m_schema.get()) {
                    assign(dst, src);
                    clear(src);
                    return;
                }
                throw std::invalid_argument("Cyclic buffer move_from_cpp requires a matching schema");
            }

            /**
             * Construct the cyclic buffer in caller-supplied root storage.
             *
             * The element array is rebound to the inline payload region that
             * follows the header, so no secondary allocation is required.
             */
            void construct(void *memory) const
            {
                std::construct_at(state(memory));
                if (storage_slots() == 0) { return; }
                state(memory)->elements = elements_memory(memory);
                this->construct_slots(state(memory)->elements, storage_slots());
            }

            /**
             * Destroy the inline payload in place without deallocating any child
             * buffer, because cyclic-buffer elements live inside the root value
             * allocation.
             */
            void destruct(void *memory) const noexcept
            {
                auto *buffer = state(memory);
                if (buffer->elements != nullptr) {
                    this->destruct_slots(buffer->elements, storage_slots());
                }
                std::destroy_at(state(memory));
            }

            void copy_construct(void *dst, const void *src) const
            {
                construct(dst);
                assign(dst, src);
            }

            /**
             * Move-construct the cyclic buffer into a new root allocation.
             *
             * Because the element payload is inline rather than separately
             * allocated, move-construction must rebind the destination element
             * pointer and move each slot into the new inline region.
             */
            void move_construct(void *dst, void *src) const
            {
                auto *dst_buffer = state(dst);
                auto *src_buffer = state(src);
                std::construct_at(dst_buffer);
                dst_buffer->elements = elements_memory(dst);
                dst_buffer->size = src_buffer->size;
                dst_buffer->head = src_buffer->head;
                if constexpr (tracks_deltas_v) {
                    dst_buffer->has_removed = src_buffer->has_removed;
                    dst_buffer->mutation_depth = 0;
                }

                for (size_t i = 0; i < storage_slots(); ++i) {
                    this->m_element_builder.get().move_construct(dst_buffer->elements + i * this->element_stride(),
                                                                 src_buffer->elements + i * this->element_stride(),
                                                                 this->m_element_builder);
                }

                src_buffer->size = 0;
                src_buffer->head = 0;
                if constexpr (tracks_deltas_v) {
                    src_buffer->has_removed = false;
                    src_buffer->mutation_depth = 0;
                }
            }

          private:
            [[nodiscard]] static size_t align_up(size_t value, size_t alignment) noexcept
            {
                return alignment <= 1 ? value : ((value + alignment - 1) / alignment) * alignment;
            }

            [[nodiscard]] size_t storage_slots() const noexcept
            {
                return tracks_deltas_v ? capacity() + 1 : capacity();
            }

            /**
             * Return the start of the inline element region inside the root
             * value allocation.
             */
            [[nodiscard]] std::byte *elements_memory(void *memory) const noexcept
            {
                return static_cast<std::byte *>(memory) +
                       align_up(sizeof(BufferState), this->m_element_builder.get().alignment());
            }

            [[nodiscard]] size_t removed_slot_index() const noexcept
            {
                return capacity();
            }

            [[nodiscard]] void *removed_slot_memory(void *data) const noexcept
            {
                return state(data)->elements + removed_slot_index() * this->element_stride();
            }

            [[nodiscard]] const void *removed_slot_memory(const void *data) const noexcept
            {
                return state(data)->elements + removed_slot_index() * this->element_stride();
            }

            template <typename TState>
            [[nodiscard]] size_t physical_index(const TState &buffer, size_t logical_index) const noexcept
            {
                return capacity() == 0 ? 0 : (buffer.head + logical_index) % capacity();
            }

            [[nodiscard]] BufferState *state(void *memory) const noexcept
            {
                return std::launder(reinterpret_cast<BufferState *>(memory));
            }

            [[nodiscard]] const BufferState *state(const void *memory) const noexcept
            {
                return std::launder(reinterpret_cast<const BufferState *>(memory));
            }

            [[nodiscard]] bool &removed_flag(void *memory) const noexcept
            {
                return state(memory)->has_removed;
            }
        };

        template <MutationTracking TTracking>
        struct QueueDispatch final : QueueViewDispatch, SequenceDispatchBase<TTracking>
        {
            static constexpr MutationTracking tracking_mode = TTracking;
            static constexpr bool tracks_deltas_v = TTracking == MutationTracking::Delta;
            using QueueState = std::conditional_t<tracks_deltas_v, DeltaQueueState, PlainQueueState>;

            [[nodiscard]] MutationTracking tracking() const noexcept override
            {
                return tracking_mode;
            }

            explicit QueueDispatch(const value::TypeMeta &schema)
                : SequenceDispatchBase<TTracking>(schema)
            {
            }

            /**
             * Return the number of logical queue elements retained inline in the
             * root value allocation before the queue grows into heap storage.
             */
            [[nodiscard]] size_t inline_capacity() const noexcept
            {
                return SmallBufferPolicy::capacity_for([this](size_t elements) noexcept {
                    return storage_slots(elements) * this->element_stride() <= SmallBufferPolicy::target_bytes;
                });
            }

            /**
             * Return the total root-allocation size required for the queue
             * header plus its schema-derived inline small buffer.
             */
            [[nodiscard]] size_t allocation_size() const noexcept
            {
                return inline_capacity() == 0 ? sizeof(QueueState)
                                              : header_size() + storage_slots(inline_capacity()) * this->element_stride();
            }

            /**
             * Return the alignment required by the header and inline payload.
             */
            [[nodiscard]] size_t allocation_alignment() const noexcept
            {
                return std::max(alignof(QueueState), this->m_element_builder.get().alignment());
            }

            [[nodiscard]] size_t size(const void *data) const noexcept override { return state(data)->size; }
            [[nodiscard]] size_t max_capacity() const noexcept override { return this->m_schema.get().fixed_size; }
            [[nodiscard]] const value::TypeMeta &element_schema() const noexcept override { return SequenceDispatchBase<TTracking>::element_schema(); }
            [[nodiscard]] const ViewDispatch &element_dispatch() const noexcept override { return SequenceDispatchBase<TTracking>::element_dispatch(); }

            void begin_mutation(void *data) const override
            {
                if constexpr (tracks_deltas_v) {
                    QueueState *queue = state(data);
                    if (queue->mutation_depth++ == 0 && queue->elements != nullptr && queue->capacity > 0) {
                        this->clear_removed_slot(removed_slot_memory(data), queue->has_removed);
                    }
                }
            }

            void end_mutation(void *data) const override
            {
                if constexpr (tracks_deltas_v) {
                    QueueState *queue = state(data);
                    if (queue->mutation_depth == 0) {
                        throw std::runtime_error("Queue mutation depth underflow");
                    }
                    --queue->mutation_depth;
                }
            }

            [[nodiscard]] void *element_data(void *data, size_t index) const override
            {
                auto *queue = state(data);
                if (index >= queue->size) { throw std::out_of_range("Queue index out of range"); }
                return queue->elements + physical_index(*queue, index) * this->element_stride();
            }

            [[nodiscard]] const void *element_data(const void *data, size_t index) const override
            {
                const auto *queue = state(data);
                if (index >= queue->size) { throw std::out_of_range("Queue index out of range"); }
                return queue->elements + physical_index(*queue, index) * this->element_stride();
            }

            [[nodiscard]] bool has_removed(const void *data) const noexcept override
            {
                if constexpr (tracks_deltas_v) {
                    return state(data)->has_removed;
                } else {
                    static_cast<void>(data);
                    return false;
                }
            }

            [[nodiscard]] void *removed_data(void *data) const override
            {
                QueueState *queue = state(data);
                if constexpr (!tracks_deltas_v) {
                    static_cast<void>(queue);
                    throw std::out_of_range("Queue has no removed value");
                } else if (!queue->has_removed || queue->elements == nullptr || queue->capacity == 0) {
                    throw std::out_of_range("Queue has no removed value");
                }
                return removed_slot_memory(data);
            }

            [[nodiscard]] const void *removed_data(const void *data) const override
            {
                const QueueState *queue = state(data);
                if constexpr (!tracks_deltas_v) {
                    static_cast<void>(queue);
                    throw std::out_of_range("Queue has no removed value");
                } else if (!queue->has_removed || queue->elements == nullptr || queue->capacity == 0) {
                    throw std::out_of_range("Queue has no removed value");
                }
                return removed_slot_memory(data);
            }

            void push(void *data, const void *value) const override
            {
                auto *queue = state(data);
                if (queue->capacity == 0) {
                    reserve_exact(data, max_capacity() > 0 ? max_capacity() : 4);
                } else if (max_capacity() > 0 && queue->size == max_capacity()) {
                    pop(data);
                } else if (queue->size == queue->capacity) {
                    const size_t grown = max_capacity() > 0 ? max_capacity() : queue->capacity * 2;
                    reserve_exact(data, std::max<size_t>(grown, queue->capacity + 1));
                }

                const size_t tail = (queue->head + queue->size) % queue->capacity;
                this->assign_slot(queue->elements + tail * this->element_stride(), value);
                ++queue->size;
            }

            void pop(void *data) const override
            {
                auto *queue = state(data);
                if (queue->size == 0) { throw std::out_of_range("pop_front on empty queue"); }
                if constexpr (tracks_deltas_v) {
                    this->retain_removed_slot(
                        removed_slot_memory(data), removed_flag(data), queue->elements + queue->head * this->element_stride());
                }
                this->reset_slot(queue->elements + queue->head * this->element_stride());
                queue->head = queue->capacity == 0 ? 0 : (queue->head + 1) % queue->capacity;
                --queue->size;
                if (queue->size == 0) { queue->head = 0; }
            }

            void clear(void *data) const override
            {
                auto *queue = state(data);
                for (size_t i = 0; i < queue->size; ++i) {
                    const size_t physical = physical_index(*queue, i);
                    if constexpr (tracks_deltas_v) {
                        this->retain_removed_slot(
                            removed_slot_memory(data), removed_flag(data), queue->elements + physical * this->element_stride());
                    }
                    this->reset_slot(queue->elements + physical * this->element_stride());
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
                QueueState *queue = state(dst);
                const size_t sequence_size = nb::len(nb::cast<nb::sequence>(src));
                const size_t import_limit = max_capacity();
                const size_t target_size = import_limit == 0 ? sequence_size : std::min(import_limit, sequence_size);
                if (target_size > 0) {
                    reserve_exact(dst, max_capacity() > 0 ? std::min(max_capacity(), target_size) : target_size);
                }

                nb::iterator it = nb::iter(src);
                size_t       imported = 0;
                while (it != nb::iterator::sentinel() && (import_limit == 0 || imported < import_limit)) {
                    const size_t tail = (queue->head + queue->size) % queue->capacity;
                    this->element_dispatch().from_python(
                        queue->elements + tail * this->element_stride(), nb::borrow<nb::object>(*it), &this->element_schema());
                    ++queue->size;
                    ++it;
                    ++imported;
                }
            }

            void assign(void *dst, const void *src) const override
            {
                clear(dst);
                for (size_t i = 0; i < size(src); ++i) {
                    push(dst, element_data(src, i));
                }
            }

            void copy_from(void *dst, const View &src) const override
            {
                if (this == detail::ViewAccess::dispatch(src)) {
                    assign(dst, detail::ViewAccess::data(src));
                    return;
                }

                clear(dst);
                const auto *source_dispatch = static_cast<const QueueViewDispatch *>(detail::ViewAccess::dispatch(src));
                const void *source_data = detail::ViewAccess::data(src);
                for (size_t i = 0; i < source_dispatch->size(source_data); ++i) {
                    View source_element{
                        &source_dispatch->element_dispatch(),
                        const_cast<void *>(source_dispatch->element_data(source_data, i)),
                        &source_dispatch->element_schema()};
                    Value normalized_element{element_schema(), element_dispatch().tracking()};
                    normalized_element.view().copy_from(source_element);
                    push(dst, detail::ViewAccess::data(normalized_element.view()));
                }
            }

            void set_from_cpp(void *dst, const void *src, const value::TypeMeta *src_schema) const override
            {
                if (src_schema == &this->m_schema.get()) {
                    assign(dst, src);
                    return;
                }
                throw std::invalid_argument("Queue value set_from_cpp requires a matching schema");
            }

            void move_from_cpp(void *dst, void *src, const value::TypeMeta *src_schema) const override
            {
                if (src_schema == &this->m_schema.get()) {
                    assign(dst, src);
                    clear(src);
                    return;
                }
                throw std::invalid_argument("Queue value move_from_cpp requires a matching schema");
            }

            void reserve(void *data, size_t capacity) const override
            {
                if (max_capacity() > 0 && capacity > max_capacity()) {
                    throw std::invalid_argument("Queue reserve exceeds the configured maximum capacity");
                }
                reserve_exact(data, capacity);
            }

            void construct(void *memory) const
            {
                std::construct_at(state(memory));
                if (const size_t capacity = inline_capacity(); capacity > 0) {
                    QueueState *queue = state(memory);
                    queue->elements = inline_elements_memory(memory);
                    queue->capacity = capacity;
                    queue->size = 0;
                    queue->head = 0;
                    this->construct_slots(queue->elements, storage_slots(capacity));
                }
            }

            void destruct(void *memory) const noexcept
            {
                auto *queue = state(memory);
                if (queue->elements != nullptr) {
                    if (queue->capacity > 0) {
                        this->destruct_slots(queue->elements, storage_slots(queue->capacity));
                    }
                    if (!uses_inline_storage(memory)) {
                        ::operator delete(queue->elements, std::align_val_t{this->m_element_builder.get().alignment()});
                    }
                }
                std::destroy_at(state(memory));
            }

            void copy_construct(void *dst, const void *src) const
            {
                construct(dst);
                assign(dst, src);
            }

            void move_construct(void *dst, void *src) const
            {
                if (uses_inline_storage(src)) {
                    construct(dst);
                    assign(dst, src);
                    clear(src);
                    return;
                }

                std::construct_at(state(dst), std::move(*state(src)));
                state(src)->elements = nullptr;
                state(src)->size = 0;
                state(src)->capacity = 0;
                state(src)->head = 0;
                if constexpr (tracks_deltas_v) {
                    state(src)->has_removed = false;
                    state(src)->mutation_depth = 0;
                }
            }

          private:
            template <typename TState>
            [[nodiscard]] size_t physical_index(const TState &queue, size_t logical_index) const noexcept
            {
                return queue.capacity == 0 ? 0 : (queue.head + logical_index) % queue.capacity;
            }

            void reserve_exact(void *data, size_t min_capacity) const
            {
                QueueStateBase *queue = state(data);
                if (min_capacity <= queue->capacity) { return; }
                if (max_capacity() > 0 && min_capacity > max_capacity()) {
                    min_capacity = max_capacity();
                }
                if (min_capacity <= queue->capacity) { return; }

                std::byte *new_elements = static_cast<std::byte *>(
                    ::operator new(storage_slots(min_capacity) * this->element_stride(),
                                   std::align_val_t{this->m_element_builder.get().alignment()}));
                size_t constructed = 0;
                try {
                    for (; constructed < queue->size; ++constructed) {
                        this->m_element_builder.get().move_construct(new_elements + constructed * this->element_stride(),
                                                                     const_cast<void *>(element_data(queue, constructed)),
                                                                     this->m_element_builder);
                    }
                    for (; constructed < min_capacity; ++constructed) {
                        this->m_element_builder.get().construct(new_elements + constructed * this->element_stride());
                    }
                    if constexpr (tracks_deltas_v) {
                        if (queue->elements != nullptr && removed_flag(reinterpret_cast<void *>(queue)) && queue->capacity > 0) {
                            this->m_element_builder.get().move_construct(
                                new_elements + min_capacity * this->element_stride(),
                                const_cast<void *>(removed_slot_memory(reinterpret_cast<void *>(queue))),
                                this->m_element_builder);
                        } else {
                            this->m_element_builder.get().construct(new_elements + min_capacity * this->element_stride());
                        }
                        ++constructed;
                    }
                } catch (...) {
                    this->destruct_slots(new_elements, constructed);
                    ::operator delete(new_elements, std::align_val_t{this->m_element_builder.get().alignment()});
                    throw;
                }

                if (queue->elements != nullptr) {
                    this->destruct_slots(queue->elements, storage_slots(queue->capacity));
                    if (!uses_inline_storage(data)) {
                        ::operator delete(queue->elements, std::align_val_t{this->m_element_builder.get().alignment()});
                    }
                }

                queue->elements = new_elements;
                queue->capacity = min_capacity;
                queue->head = 0;
            }

            [[nodiscard]] size_t storage_slots(size_t logical_capacity) const noexcept
            {
                if (logical_capacity == 0) { return 1; }
                return tracks_deltas_v ? logical_capacity + 1 : logical_capacity;
            }

            [[nodiscard]] size_t header_size() const noexcept
            {
                const size_t header = tracks_deltas_v ? sizeof(DeltaQueueState) : sizeof(PlainQueueState);
                const size_t alignment = this->m_element_builder.get().alignment();
                return alignment <= 1 ? header : ((header + alignment - 1) / alignment) * alignment;
            }

            [[nodiscard]] std::byte *inline_elements_memory(void *memory) const noexcept
            {
                return static_cast<std::byte *>(memory) + header_size();
            }

            [[nodiscard]] bool uses_inline_storage(const void *memory) const noexcept
            {
                const QueueState *queue = state(memory);
                return queue->elements != nullptr && queue->elements == static_cast<const std::byte *>(memory) + header_size();
            }

            [[nodiscard]] void *removed_slot_memory(void *memory) const noexcept
            {
                return state(memory)->elements + state(memory)->capacity * this->element_stride();
            }

            [[nodiscard]] const void *removed_slot_memory(const void *memory) const noexcept
            {
                return state(memory)->elements + state(memory)->capacity * this->element_stride();
            }

            [[nodiscard]] QueueState *state(void *memory) const noexcept
            {
                return std::launder(reinterpret_cast<QueueState *>(memory));
            }

            [[nodiscard]] const QueueState *state(const void *memory) const noexcept
            {
                return std::launder(reinterpret_cast<const QueueState *>(memory));
            }

            [[nodiscard]] bool &removed_flag(void *memory) const noexcept
            {
                return state(memory)->has_removed;
            }

        };

        template <typename TDispatch> struct SequenceStateOps final : ValueBuilderOps
        {
            explicit SequenceStateOps(const TDispatch &dispatch) noexcept
                : m_dispatch(dispatch)
            {
            }

            [[nodiscard]] BuilderLayout layout(const value::TypeMeta &schema) const noexcept override
            {
                static_cast<void>(schema);
                return BuilderLayout{m_dispatch.get().allocation_size(), m_dispatch.get().allocation_alignment()};
            }

            [[nodiscard]] const ViewDispatch &view_dispatch(const value::TypeMeta &schema) const noexcept override
            {
                static_cast<void>(schema);
                return m_dispatch.get();
            }

            [[nodiscard]] bool requires_destruct(const value::TypeMeta &schema) const noexcept override
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
            void destruct(void *memory) const noexcept override { m_dispatch.get().destruct(memory); }
            void copy_construct(void *dst, const void *src) const override { m_dispatch.get().copy_construct(dst, src); }
            void move_construct(void *dst, void *src) const override { m_dispatch.get().move_construct(dst, src); }

            std::reference_wrapper<const TDispatch> m_dispatch;
        };

        struct CachedBuilderEntry
        {
            std::shared_ptr<const ViewDispatch> dispatch;
            std::shared_ptr<const ValueBuilderOps>     state_ops;
            std::shared_ptr<const ValueBuilder> builder;
        };

        struct SequenceBuilderKey
        {
            const value::TypeMeta *schema{nullptr};
            MutationTracking       tracking{MutationTracking::Delta};

            [[nodiscard]] bool operator==(const SequenceBuilderKey &other) const noexcept
            {
                return schema == other.schema && tracking == other.tracking;
            }
        };

        struct SequenceBuilderKeyHash
        {
            [[nodiscard]] size_t operator()(const SequenceBuilderKey &key) const noexcept
            {
                return std::hash<const value::TypeMeta *>{}(key.schema) ^
                       (static_cast<size_t>(key.tracking) << 1U);
            }
        };

        const ValueBuilder *sequence_builder_for(const value::TypeMeta *schema, MutationTracking tracking)
        {
            if (schema == nullptr) { return nullptr; }
            if (schema->kind != value::TypeKind::CyclicBuffer && schema->kind != value::TypeKind::Queue) { return nullptr; }

            static std::recursive_mutex cache_mutex;
            static std::unordered_map<SequenceBuilderKey, CachedBuilderEntry, SequenceBuilderKeyHash> cache;

            std::lock_guard lock(cache_mutex);
            const SequenceBuilderKey key{schema, tracking};
            if (auto it = cache.find(key); it != cache.end()) {
                return it->second.builder.get();
            }

            CachedBuilderEntry entry;
            if (schema->kind == value::TypeKind::CyclicBuffer) {
                if (tracking == MutationTracking::Delta) {
                    auto dispatch = std::make_shared<CyclicBufferDispatch<MutationTracking::Delta>>(*schema);
                    auto state_ops =
                        std::make_shared<SequenceStateOps<CyclicBufferDispatch<MutationTracking::Delta>>>(*dispatch);
                    auto builder = std::make_shared<ValueBuilder>(*schema, tracking, *state_ops);
                    entry.dispatch = std::move(dispatch);
                    entry.state_ops = std::move(state_ops);
                    entry.builder = std::move(builder);
                } else {
                    auto dispatch = std::make_shared<CyclicBufferDispatch<MutationTracking::Plain>>(*schema);
                    auto state_ops =
                        std::make_shared<SequenceStateOps<CyclicBufferDispatch<MutationTracking::Plain>>>(*dispatch);
                    auto builder = std::make_shared<ValueBuilder>(*schema, tracking, *state_ops);
                    entry.dispatch = std::move(dispatch);
                    entry.state_ops = std::move(state_ops);
                    entry.builder = std::move(builder);
                }
            } else {
                if (tracking == MutationTracking::Delta) {
                    auto dispatch = std::make_shared<QueueDispatch<MutationTracking::Delta>>(*schema);
                    auto state_ops =
                        std::make_shared<SequenceStateOps<QueueDispatch<MutationTracking::Delta>>>(*dispatch);
                    auto builder = std::make_shared<ValueBuilder>(*schema, tracking, *state_ops);
                    entry.dispatch = std::move(dispatch);
                    entry.state_ops = std::move(state_ops);
                    entry.builder = std::move(builder);
                } else {
                    auto dispatch = std::make_shared<QueueDispatch<MutationTracking::Plain>>(*schema);
                    auto state_ops =
                        std::make_shared<SequenceStateOps<QueueDispatch<MutationTracking::Plain>>>(*dispatch);
                    auto builder = std::make_shared<ValueBuilder>(*schema, tracking, *state_ops);
                    entry.dispatch = std::move(dispatch);
                    entry.state_ops = std::move(state_ops);
                    entry.builder = std::move(builder);
                }
            }

            auto [it, inserted] = cache.emplace(key, std::move(entry));
            static_cast<void>(inserted);
            return it->second.builder.get();
        }

    }  // namespace detail

    BufferView::BufferView(const View &view)
        : View(view)
    {
        if (!view.has_value()) { return; }
        if (view.schema() == nullptr ||
            (view.schema()->kind != value::TypeKind::CyclicBuffer && view.schema()->kind != value::TypeKind::Queue)) {
            throw std::runtime_error("BufferView requires a cyclic buffer or queue schema");
        }
    }

    BufferMutationView BufferView::begin_mutation()
    {
        return BufferMutationView{*this};
    }

    void BufferView::begin_mutation_scope()
    {
        const auto *dispatch = buffer_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("BufferView::begin_mutation on invalid view"); }
        dispatch->begin_mutation(data());
    }

    void BufferView::end_mutation_scope() noexcept
    {
        const auto *dispatch = buffer_dispatch();
        if (dispatch == nullptr) { return; }
        dispatch->end_mutation(data());
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
    bool BufferView::has_removed() const
    {
        const auto *dispatch = buffer_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("BufferView::has_removed on invalid view"); }
        return dispatch->has_removed(data());
    }
    View BufferView::removed()
    {
        const auto *dispatch = buffer_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("BufferView::removed on invalid view"); }
        return View{&dispatch->element_dispatch(), dispatch->removed_data(data()), &dispatch->element_schema()};
    }
    View BufferView::removed() const
    {
        const auto *dispatch = buffer_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("BufferView::removed on invalid view"); }
        return View{&dispatch->element_dispatch(), const_cast<void *>(dispatch->removed_data(data())), &dispatch->element_schema()};
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

    BufferMutationView::BufferMutationView(BufferView &view)
        : BufferView(view)
    {
        begin_mutation_scope();
    }

    BufferMutationView::BufferMutationView(BufferMutationView &&other) noexcept
        : BufferView(other)
    {
        m_owns_scope = other.m_owns_scope;
        other.m_owns_scope = false;
    }

    BufferMutationView::~BufferMutationView()
    {
        if (!m_owns_scope) { return; }
        try {
            end_mutation_scope();
        } catch (...) {
        }
    }

    void BufferMutationView::push(const View &value)
    {
        const auto *dispatch = buffer_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("BufferMutationView::push on invalid view"); }
        if (!value.has_value() || value.schema() != &dispatch->element_schema()) {
            throw std::runtime_error("BufferMutationView::push requires a valid matching-schema value");
        }
        if (value.tracking() == dispatch->element_dispatch().tracking()) {
            dispatch->push(data(), data_of(value));
            return;
        }
        Value normalized{value, dispatch->element_dispatch().tracking()};
        dispatch->push(data(), data_of(normalized.view()));
    }
    void BufferMutationView::pop()
    {
        const auto *dispatch = buffer_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("BufferMutationView::pop on invalid view"); }
        dispatch->pop(data());
    }
    void BufferMutationView::clear()
    {
        const auto *dispatch = buffer_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("BufferMutationView::clear on invalid view"); }
        dispatch->clear(data());
    }
    const detail::BufferViewDispatch *BufferView::buffer_dispatch() const noexcept
    {
        return has_value() ? static_cast<const detail::BufferViewDispatch *>(dispatch()) : nullptr;
    }

    CyclicBufferView::CyclicBufferView(const View &view)
        : BufferView(view)
    {
        if (!view.has_value()) { return; }
        if (view.schema() == nullptr || view.schema()->kind != value::TypeKind::CyclicBuffer) {
            throw std::runtime_error("CyclicBufferView requires a cyclic buffer schema");
        }
    }

    CyclicBufferMutationView CyclicBufferView::begin_mutation()
    {
        return CyclicBufferMutationView{*this};
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

    CyclicBufferMutationView::CyclicBufferMutationView(CyclicBufferView &view)
        : CyclicBufferView(view)
    {
        begin_mutation_scope();
    }

    CyclicBufferMutationView::CyclicBufferMutationView(CyclicBufferMutationView &&other) noexcept
        : CyclicBufferView(other)
    {
        m_owns_scope = other.m_owns_scope;
        other.m_owns_scope = false;
    }

    CyclicBufferMutationView::~CyclicBufferMutationView()
    {
        if (!m_owns_scope) { return; }
        try {
            end_mutation_scope();
        } catch (...) {
        }
    }

    void CyclicBufferMutationView::push(const View &value)
    {
        const auto *dispatch = buffer_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("CyclicBufferMutationView::push on invalid view"); }
        if (!value.has_value() || value.schema() != &dispatch->element_schema()) {
            throw std::runtime_error("CyclicBufferMutationView::push requires a valid matching-schema value");
        }
        if (value.tracking() == dispatch->element_dispatch().tracking()) {
            dispatch->push(data(), data_of(value));
            return;
        }
        Value normalized{value, dispatch->element_dispatch().tracking()};
        dispatch->push(data(), data_of(normalized.view()));
    }

    void CyclicBufferMutationView::pop()
    {
        const auto *dispatch = buffer_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("CyclicBufferMutationView::pop on invalid view"); }
        dispatch->pop(data());
    }

    void CyclicBufferMutationView::clear()
    {
        const auto *dispatch = buffer_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("CyclicBufferMutationView::clear on invalid view"); }
        dispatch->clear(data());
    }

    void CyclicBufferMutationView::set(size_t index, const View &value)
    {
        const auto *dispatch = cyclic_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("CyclicBufferMutationView::set on invalid view"); }
        if (!value.has_value() || value.schema() != &dispatch->element_schema()) {
            throw std::runtime_error("CyclicBufferMutationView::set requires a valid matching-schema value");
        }
        if (value.tracking() == dispatch->element_dispatch().tracking()) {
            dispatch->set_at(data(), index, data_of(value));
            return;
        }
        Value normalized{value, dispatch->element_dispatch().tracking()};
        dispatch->set_at(data(), index, data_of(normalized.view()));
    }
    const detail::CyclicBufferViewDispatch *CyclicBufferView::cyclic_dispatch() const noexcept
    {
        return has_value() ? static_cast<const detail::CyclicBufferViewDispatch *>(dispatch()) : nullptr;
    }

    QueueView::QueueView(const View &view)
        : BufferView(view)
    {
        if (!view.has_value()) { return; }
        if (view.schema() == nullptr || view.schema()->kind != value::TypeKind::Queue) {
            throw std::runtime_error("QueueView requires a queue schema");
        }
    }

    QueueMutationView QueueView::begin_mutation()
    {
        return QueueMutationView{*this};
    }

    size_t QueueView::max_capacity() const
    {
        const auto *dispatch = queue_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("QueueView::max_capacity on invalid view"); }
        return dispatch->max_capacity();
    }
    bool QueueView::has_max_capacity() const noexcept { return has_value() && max_capacity() > 0; }

    QueueMutationView::QueueMutationView(QueueView &view)
        : QueueView(view)
    {
        begin_mutation_scope();
    }

    QueueMutationView::QueueMutationView(QueueMutationView &&other) noexcept
        : QueueView(other)
    {
        m_owns_scope = other.m_owns_scope;
        other.m_owns_scope = false;
    }

    QueueMutationView::~QueueMutationView()
    {
        if (!m_owns_scope) { return; }
        try {
            end_mutation_scope();
        } catch (...) {
        }
    }

    void QueueMutationView::push(const View &value)
    {
        const auto *dispatch = buffer_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("QueueMutationView::push on invalid view"); }
        if (!value.has_value() || value.schema() != &dispatch->element_schema()) {
            throw std::runtime_error("QueueMutationView::push requires a valid matching-schema value");
        }
        if (value.tracking() == dispatch->element_dispatch().tracking()) {
            dispatch->push(data(), data_of(value));
            return;
        }
        Value normalized{value, dispatch->element_dispatch().tracking()};
        dispatch->push(data(), data_of(normalized.view()));
    }

    void QueueMutationView::pop()
    {
        const auto *dispatch = buffer_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("QueueMutationView::pop on invalid view"); }
        dispatch->pop(data());
    }

    void QueueMutationView::clear()
    {
        const auto *dispatch = buffer_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("QueueMutationView::clear on invalid view"); }
        dispatch->clear(data());
    }

    void QueueMutationView::reserve(size_t capacity)
    {
        const auto *dispatch = queue_dispatch();
        if (dispatch == nullptr) { throw std::runtime_error("QueueMutationView::reserve on invalid view"); }
        dispatch->reserve(data(), capacity);
    }

    const detail::QueueViewDispatch *QueueView::queue_dispatch() const noexcept
    {
        return has_value() ? static_cast<const detail::QueueViewDispatch *>(dispatch()) : nullptr;
    }
}  // namespace hgraph
