#include <hgraph/types/metadata/ts_data_plan_factory_detail.h>

#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/value/specialized_views.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/util/scope.h>

#include <fmt/format.h>

#include <algorithm>
#include <compare>
#include <cstddef>
#include <iterator>
#include <memory>
#include <mutex>
#include <new>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>

namespace hgraph::ts_data_plan_factory_detail
{
    namespace
    {
        [[nodiscard]] std::size_t combine_hash(std::size_t seed, std::size_t value) noexcept
        {
            seed ^= value + 0x9e3779b97f4a7c15ULL + (seed << 6U) + (seed >> 2U);
            return seed;
        }

        [[nodiscard]] std::size_t align_up(std::size_t value, std::size_t alignment) noexcept
        {
            return alignment <= 1 ? value : ((value + alignment - 1) / alignment) * alignment;
        }

        class TSWindowStorageCore
        {
          public:
            TSWindowStorageCore(const ValueTypeRef &time_binding, const ValueTypeRef &element_binding)
                : time_binding_(time_binding),
                  element_binding_(element_binding)
            {}

            TSWindowStorageCore(const TSWindowStorageCore &other)
                : time_binding_(other.time_binding_),
                  element_binding_(other.element_binding_),
                  evicted_(other.evicted_),
                  evicted_time_(other.evicted_time_)
            {
                copy_live_from(other);
            }

            TSWindowStorageCore &operator=(const TSWindowStorageCore &other)
            {
                if (this != &other)
                {
                    clear();
                    deallocate();
                    time_binding_    = other.time_binding_;
                    element_binding_ = other.element_binding_;
                    evicted_         = other.evicted_;
                    evicted_time_    = other.evicted_time_;
                    copy_live_from(other);
                }
                return *this;
            }

            TSWindowStorageCore(TSWindowStorageCore &&other) noexcept
                : time_binding_(other.time_binding_),
                  element_binding_(other.element_binding_),
                  time_bytes_(std::exchange(other.time_bytes_, nullptr)),
                  value_bytes_(std::exchange(other.value_bytes_, nullptr)),
                  capacity_(std::exchange(other.capacity_, 0)),
                  size_(std::exchange(other.size_, 0)),
                  head_(std::exchange(other.head_, 0)),
                  evicted_(std::move(other.evicted_)),
                  evicted_time_(std::exchange(other.evicted_time_, MIN_DT))
            {
                other.time_binding_    = nullptr;
                other.element_binding_ = nullptr;
            }

            TSWindowStorageCore &operator=(TSWindowStorageCore &&other) noexcept
            {
                if (this != &other)
                {
                    clear();
                    deallocate();
                    time_binding_          = other.time_binding_;
                    element_binding_       = other.element_binding_;
                    time_bytes_            = std::exchange(other.time_bytes_, nullptr);
                    value_bytes_           = std::exchange(other.value_bytes_, nullptr);
                    capacity_              = std::exchange(other.capacity_, 0);
                    size_                  = std::exchange(other.size_, 0);
                    head_                  = std::exchange(other.head_, 0);
                    evicted_               = std::move(other.evicted_);
                    evicted_time_          = std::exchange(other.evicted_time_, MIN_DT);
                    other.time_binding_    = nullptr;
                    other.element_binding_ = nullptr;
                }
                return *this;
            }

            ~TSWindowStorageCore()
            {
                clear();
                deallocate();
            }

            [[nodiscard]] std::size_t size() const noexcept { return size_; }
            [[nodiscard]] bool empty() const noexcept { return size_ == 0; }
            [[nodiscard]] ValueTypeRef time_binding() const { return time_binding_; }
            [[nodiscard]] ValueTypeRef element_binding() const { return element_binding_; }

            /** The element most recently evicted by a push (hgraph's
                removed_value) and the evaluation time it fell out. */
            [[nodiscard]] const Value &evicted_element() const noexcept { return evicted_; }
            [[nodiscard]] DateTime evicted_time() const noexcept { return evicted_time_; }
            [[nodiscard]] DateTime cleared_time() const noexcept
            {
                return evicted_.has_value() ? MIN_DT : evicted_time_;
            }

            /** Clear the logical window while retaining its allocation. */
            void clear_values(DateTime modified_time) noexcept
            {
                clear();
                evicted_.reset();
                evicted_time_ = modified_time;
            }

            [[nodiscard]] const void *element_at(std::size_t index) const
            {
                if (index >= size_) { throw std::out_of_range("TSW window storage index out of range"); }
                return value_slot(physical_index(index));
            }

            [[nodiscard]] void *element_at(std::size_t index)
            {
                if (index >= size_) { throw std::out_of_range("TSW window storage index out of range"); }
                return value_slot(physical_index(index));
            }

            [[nodiscard]] const void *time_element_at(std::size_t index) const
            {
                if (index >= size_) { throw std::out_of_range("TSW window storage time index out of range"); }
                return time_slot(physical_index(index));
            }

            [[nodiscard]] DateTime time_at(std::size_t index) const
            {
                if (index >= size_) { throw std::out_of_range("TSW window storage time index out of range"); }
                return time_at_physical(physical_index(index));
            }

            [[nodiscard]] const void *last_element() const noexcept
            {
                if (size_ == 0) { return nullptr; }
                return value_slot((head_ + size_ - 1) % capacity_);
            }

          protected:
            [[nodiscard]] std::size_t storage_capacity() const noexcept { return capacity_; }

            void append(const ValueView &source, DateTime modified_time)
            {
                validate_source(source);
                if (capacity_ == 0) { throw std::logic_error("TSW storage has no capacity"); }
                if (size_ >= capacity_)
                {
                    throw std::logic_error("TSW storage append requires available capacity");
                }

                const auto physical = (head_ + size_) % capacity_;
                copy_construct_slot(physical, source.data(), modified_time);
                ++size_;
            }

            void append_move(const ValueView &source, DateTime modified_time)
            {
                validate_source(source);
                if (capacity_ == 0) { throw std::logic_error("TSW storage has no capacity"); }
                if (size_ >= capacity_)
                {
                    throw std::logic_error("TSW storage append requires available capacity");
                }

                const auto physical = (head_ + size_) % capacity_;
                move_construct_slot(physical, const_cast<void *>(source.data()), modified_time);
                ++size_;
            }

            void overwrite_oldest(const ValueView &source, DateTime modified_time)
            {
                validate_source(source);
                if (capacity_ == 0) { throw std::logic_error("TSW storage has no capacity"); }

                const auto physical = head_;
                record_evicted(value_slot(physical), modified_time);
                copy_assign_value_slot(physical, source.data());
                copy_assign_time_slot(physical, modified_time);
                head_ = (head_ + 1) % capacity_;
            }

            /** Stash the element a push is about to drop (hgraph's removed_value). */
            void record_evicted(const void *element_memory, DateTime modified_time)
            {
                evicted_      = Value{ValueView{element_binding_, const_cast<void *>(element_memory)}};
                evicted_time_ = modified_time;
            }

            [[nodiscard]] static IndexedValueView checked_source_values(const ValueView &source)
            {
                if (!source.has_value()) { throw std::invalid_argument("TSW copy requires a live source value"); }
                if (source.schema() == nullptr || source.schema()->value_kind() != ValueTypeKind::List)
                {
                    throw std::invalid_argument("TSW copy requires a list-shaped source value");
                }
                return source.as_indexed_view();
            }

            [[nodiscard]] const MemoryUtils::StoragePlan &time_plan() const
            {
                return time_binding_.checked_plan();
            }

            [[nodiscard]] const MemoryUtils::StoragePlan &element_plan() const
            {
                return element_binding_.checked_plan();
            }

            [[nodiscard]] std::size_t time_stride() const noexcept
            {
                const auto *plan = time_binding_ ? time_binding_.plan() : nullptr;
                return plan != nullptr ? align_up(plan->layout.size, plan->layout.alignment) : 0;
            }

            [[nodiscard]] std::size_t value_stride() const noexcept
            {
                const auto *plan = element_binding_ ? element_binding_.plan() : nullptr;
                return plan != nullptr ? align_up(plan->layout.size, plan->layout.alignment) : 0;
            }

            [[nodiscard]] void *time_slot(std::size_t physical) noexcept
            {
                return time_bytes_ + physical * time_stride();
            }

            [[nodiscard]] const void *time_slot(std::size_t physical) const noexcept
            {
                return time_bytes_ + physical * time_stride();
            }

            [[nodiscard]] void *value_slot(std::size_t physical) noexcept
            {
                return value_bytes_ + physical * value_stride();
            }

            [[nodiscard]] const void *value_slot(std::size_t physical) const noexcept
            {
                return value_bytes_ + physical * value_stride();
            }

            [[nodiscard]] DateTime time_at_physical(std::size_t physical) const noexcept
            {
                return *MemoryUtils::cast<DateTime>(time_slot(physical));
            }

            [[nodiscard]] std::size_t physical_index(std::size_t logical) const noexcept
            {
                return capacity_ == 0 ? 0 : (head_ + logical) % capacity_;
            }

            void validate_source(const ValueView &source) const
            {
                if (!source.has_value()) { throw std::invalid_argument("TSW push requires a live source value"); }
                if (source.schema() != element_binding().schema())
                {
                    throw std::invalid_argument("TSW push requires the window element schema");
                }
                if (!source.binding() || source.binding().plan() != element_binding().plan())
                {
                    throw std::invalid_argument("TSW push requires a source with the element storage plan");
                }
            }

            void copy_construct_slot(std::size_t physical, const void *source, DateTime modified_time)
            {
                copy_construct_slot(physical, source, &modified_time);
            }

            void move_construct_slot(std::size_t physical, void *source, DateTime modified_time)
            {
                element_binding().move_construct_at(value_slot(physical), source);
                auto rollback_value = make_scope_exit([&]() noexcept {
                    element_binding().destroy_at(value_slot(physical));
                });
                time_binding().copy_construct_at(time_slot(physical), &modified_time);
                rollback_value.release();
            }

            void copy_construct_slot(std::size_t physical, const void *source, const void *time_source)
            {
                element_binding().copy_construct_at(value_slot(physical), source);
                auto rollback_value = make_scope_exit([&]() noexcept {
                    element_binding().destroy_at(value_slot(physical));
                });
                time_binding().copy_construct_at(time_slot(physical), time_source);
                rollback_value.release();
            }

            void copy_assign_value_slot(std::size_t physical, const void *source)
            {
                const auto &plan = element_plan();
                if (!plan.can_copy_assign())
                {
                    throw std::logic_error("TSW replacement requires copy-assignable element storage");
                }
                plan.copy_assign(value_slot(physical), source);
            }

            void copy_assign_time_slot(std::size_t physical, DateTime modified_time)
            {
                const auto &plan = time_plan();
                if (!plan.can_copy_assign())
                {
                    throw std::logic_error("TSW replacement requires copy-assignable time storage");
                }
                plan.copy_assign(time_slot(physical), &modified_time);
            }

            void destroy_slot(std::size_t physical) noexcept
            {
                element_binding().destroy_at(value_slot(physical));
                time_binding().destroy_at(time_slot(physical));
            }

            void clear() noexcept
            {
                if (time_binding_ == nullptr || element_binding_ == nullptr ||
                    time_bytes_ == nullptr || value_bytes_ == nullptr)
                {
                    size_ = 0;
                    head_ = 0;
                    return;
                }
                for (std::size_t index = 0; index < size_; ++index)
                {
                    destroy_slot(physical_index(index));
                }
                size_ = 0;
                head_ = 0;
            }

            void prune_before(DateTime cutoff) noexcept
            {
                while (size_ > 0 && time_at_physical(head_) < cutoff)
                {
                    destroy_slot(head_);
                    head_ = capacity_ == 0 ? 0 : (head_ + 1) % capacity_;
                    --size_;
                }
                if (size_ == 0) { head_ = 0; }
            }

            void ensure_capacity(std::size_t required)
            {
                if (required <= capacity_) { return; }
                const auto grown = capacity_ == 0 ? std::max<std::size_t>(required, 4)
                                                  : std::max(required, capacity_ * 2);
                reserve_exact(grown);
            }

            void reserve_exact(std::size_t new_capacity)
            {
                if (new_capacity <= capacity_) { return; }

                const auto &value_plan = element_plan();
                const auto &time_plan_ = time_plan();
                const auto  new_value_stride = align_up(value_plan.layout.size, value_plan.layout.alignment);
                const auto  new_time_stride  = align_up(time_plan_.layout.size, time_plan_.layout.alignment);
                auto       *new_value_bytes  = static_cast<std::byte *>(
                    ::operator new(new_value_stride * new_capacity, std::align_val_t{value_plan.layout.alignment}));
                std::byte  *new_time_bytes   = nullptr;
                std::size_t constructed_values = 0;
                std::size_t constructed_times  = 0;

                auto rollback = make_scope_exit([&]() noexcept {
                    for (std::size_t index = constructed_times; index > 0; --index)
                    {
                        time_plan_.destroy(new_time_bytes + (index - 1) * new_time_stride);
                    }
                    for (std::size_t index = constructed_values; index > 0; --index)
                    {
                        value_plan.destroy(new_value_bytes + (index - 1) * new_value_stride);
                    }
                    if (new_time_bytes != nullptr)
                    {
                        ::operator delete(new_time_bytes, std::align_val_t{time_plan_.layout.alignment});
                    }
                    ::operator delete(new_value_bytes, std::align_val_t{value_plan.layout.alignment});
                });

                new_time_bytes = static_cast<std::byte *>(
                    ::operator new(new_time_stride * new_capacity, std::align_val_t{time_plan_.layout.alignment}));

                while (constructed_values < size_)
                {
                    const auto index = constructed_values;
                    value_plan.copy_construct(new_value_bytes + index * new_value_stride, element_at(index));
                    ++constructed_values;
                    time_plan_.copy_construct(new_time_bytes + index * new_time_stride, time_element_at(index));
                    ++constructed_times;
                }

                clear();
                deallocate();

                value_bytes_ = new_value_bytes;
                time_bytes_  = new_time_bytes;
                capacity_    = new_capacity;
                size_        = constructed_values;
                head_        = 0;
                rollback.release();
            }

          private:
            void copy_live_from(const TSWindowStorageCore &other)
            {
                if (time_binding_ == nullptr || element_binding_ == nullptr) { return; }
                reserve_exact(other.capacity_);
                for (std::size_t index = 0; index < other.size_; ++index)
                {
                    copy_construct_slot(index, other.element_at(index), other.time_element_at(index));
                }
                size_ = other.size_;
                head_ = 0;
            }

            void deallocate() noexcept
            {
                if (value_bytes_ != nullptr && element_binding_ != nullptr)
                {
                    ::operator delete(value_bytes_, std::align_val_t{element_plan().layout.alignment});
                }
                if (time_bytes_ != nullptr && time_binding_ != nullptr)
                {
                    ::operator delete(time_bytes_, std::align_val_t{time_plan().layout.alignment});
                }
                value_bytes_ = nullptr;
                time_bytes_  = nullptr;
                capacity_    = 0;
            }

            ValueTypeRef time_binding_{nullptr};
            ValueTypeRef element_binding_{nullptr};
            std::byte              *time_bytes_{nullptr};
            std::byte              *value_bytes_{nullptr};
            std::size_t             capacity_{0};
            std::size_t             size_{0};
            std::size_t             head_{0};
            Value                   evicted_{};
            DateTime                evicted_time_{MIN_DT};
        };

        class SizeTSWindowStorage final : public TSWindowStorageCore
        {
          public:
            SizeTSWindowStorage(const ValueTypeRef &time_binding,
                                const ValueTypeRef &element_binding,
                                std::size_t period)
                : TSWindowStorageCore(time_binding, element_binding),
                  period_(period)
            {
                if (period_ == 0) { throw std::invalid_argument("TSW fixed window requires a non-zero period"); }
                reserve_exact(period_);
            }

            [[nodiscard]] std::size_t capacity() const noexcept { return period_; }

            void push(const ValueView &source, DateTime modified_time)
            {
                if (size() < period_) { append(source, modified_time); }
                else { overwrite_oldest(source, modified_time); }
            }

            void copy_from_value(const ValueView &source, DateTime modified_time)
            {
                const IndexedValueView source_values = checked_source_values(source);
                if (source_values.size() > period_)
                {
                    throw std::length_error("TSW fixed window source exceeds the configured period");
                }

                clear();
                for (std::size_t index = 0; index < source_values.size(); ++index)
                {
                    push(source_values.at(index), modified_time);
                }
            }

            void move_from_value(ValueView source, DateTime modified_time)
            {
                const IndexedValueView source_values = checked_source_values(source);
                if (source_values.size() > period_)
                {
                    throw std::length_error("TSW fixed window source exceeds the configured period");
                }

                clear();
                for (std::size_t index = 0; index < source_values.size(); ++index)
                {
                    append_move(source_values.at(index), modified_time);
                }
            }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
            void copy_from_python(nb::handle source, DateTime modified_time)
            {
                nb::object object = nb::borrow<nb::object>(source);
                if (!nb::isinstance<nb::list>(object) && !nb::isinstance<nb::tuple>(object))
                {
                    throw std::invalid_argument("TSW value expects a Python list or tuple");
                }
                if (static_cast<std::size_t>(nb::len(object)) > period_)
                {
                    throw std::length_error("TSW fixed window source exceeds the configured period");
                }

                clear();
                nb::iterator it = nb::iter(object);
                while (it != nb::iterator::sentinel())
                {
                    if ((*it).is_none()) { throw std::invalid_argument("TSW value does not allow None elements"); }
                    Value element{element_binding()};
                    element_binding().ops_ref().from_python(element_binding(),
                                                                const_cast<void *>(element.view().data()),
                                                                *it);
                    push(element.view(), modified_time);
                    ++it;
                }
            }
#endif

          private:
            std::size_t period_{0};
        };

        class TimeTSWindowStorage final : public TSWindowStorageCore
        {
          public:
            TimeTSWindowStorage(const ValueTypeRef &time_binding,
                                const ValueTypeRef &element_binding,
                                TimeDelta time_range)
                : TSWindowStorageCore(time_binding, element_binding),
                  time_range_(time_range)
            {}

            void push(const ValueView &source, DateTime modified_time)
            {
                const DateTime cutoff = modified_time - time_range_;
                // Stash the LAST element the span drop evicts (removed_value).
                std::size_t dropped = 0;
                while (dropped < size() && time_at(dropped) < cutoff) { ++dropped; }
                if (dropped > 0) { record_evicted(element_at(dropped - 1), modified_time); }
                prune_before(cutoff);
                ensure_capacity(size() + 1);
                append(source, modified_time);
            }

            void copy_from_value(const ValueView &source, DateTime modified_time)
            {
                const IndexedValueView source_values = checked_source_values(source);

                clear();
                for (std::size_t index = 0; index < source_values.size(); ++index)
                {
                    push(source_values.at(index), modified_time);
                }
            }

            void move_from_value(ValueView source, DateTime modified_time)
            {
                const IndexedValueView source_values = checked_source_values(source);

                clear();
                ensure_capacity(source_values.size());
                for (std::size_t index = 0; index < source_values.size(); ++index)
                {
                    append_move(source_values.at(index), modified_time);
                }
            }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
            void copy_from_python(nb::handle source, DateTime modified_time)
            {
                nb::object object = nb::borrow<nb::object>(source);
                if (!nb::isinstance<nb::list>(object) && !nb::isinstance<nb::tuple>(object))
                {
                    throw std::invalid_argument("TSW value expects a Python list or tuple");
                }

                clear();
                nb::iterator it = nb::iter(object);
                while (it != nb::iterator::sentinel())
                {
                    if ((*it).is_none()) { throw std::invalid_argument("TSW value does not allow None elements"); }
                    Value element{element_binding()};
                    element_binding().ops_ref().from_python(element_binding(),
                                                                const_cast<void *>(element.view().data()),
                                                                *it);
                    push(element.view(), modified_time);
                    ++it;
                }
            }
#endif

          private:
            TimeDelta time_range_{};
        };

        struct SizeWindowStoragePlanContext
        {
            ValueTypeRef time_binding{nullptr};
            ValueTypeRef element_binding{nullptr};
            std::size_t             period{0};
        };

        struct TimeWindowStoragePlanContext
        {
            ValueTypeRef time_binding{nullptr};
            ValueTypeRef element_binding{nullptr};
            TimeDelta     time_range{};
        };

        using WindowStoragePlanContext = std::variant<SizeWindowStoragePlanContext, TimeWindowStoragePlanContext>;

        template <typename Context>
        [[nodiscard]] const Context &storage_context(const void *context)
        {
            if (context == nullptr) { throw std::logic_error("TSW storage requires lifecycle context"); }
            return *static_cast<const Context *>(context);
        }

        void size_window_storage_construct(void *dst, const void *context)
        {
            const auto &state = storage_context<SizeWindowStoragePlanContext>(context);
            if (state.time_binding == nullptr)
            {
                throw std::logic_error("TSW storage time binding is not resolved");
            }
            if (state.element_binding == nullptr)
            {
                throw std::logic_error("TSW storage element binding is not resolved");
            }
            std::construct_at(static_cast<SizeTSWindowStorage *>(dst), state.time_binding, state.element_binding,
                              state.period);
        }

        void time_window_storage_construct(void *dst, const void *context)
        {
            const auto &state = storage_context<TimeWindowStoragePlanContext>(context);
            if (state.time_binding == nullptr)
            {
                throw std::logic_error("TSW storage time binding is not resolved");
            }
            if (state.element_binding == nullptr)
            {
                throw std::logic_error("TSW storage element binding is not resolved");
            }
            std::construct_at(static_cast<TimeTSWindowStorage *>(dst), state.time_binding, state.element_binding,
                              state.time_range);
        }

        template <typename Storage>
        void window_storage_destroy(void *memory, const void *) noexcept
        {
            std::destroy_at(static_cast<Storage *>(memory));
        }

        template <typename Storage>
        void window_storage_copy_construct(void *dst, const void *src, const void *)
        {
            std::construct_at(static_cast<Storage *>(dst), *static_cast<const Storage *>(src));
        }

        template <typename Storage>
        void window_storage_move_construct(void *dst, void *src, const void *)
        {
            std::construct_at(static_cast<Storage *>(dst), std::move(*static_cast<Storage *>(src)));
        }

        template <typename Storage>
        void window_storage_copy_assign(void *dst, const void *src, const void *)
        {
            *static_cast<Storage *>(dst) = *static_cast<const Storage *>(src);
        }

        template <typename Storage>
        void window_storage_move_assign(void *dst, void *src, const void *)
        {
            *static_cast<Storage *>(dst) = std::move(*static_cast<Storage *>(src));
        }

        struct WindowPlanEntry
        {
            WindowStoragePlanContext             context{};
            std::unique_ptr<MemoryUtils::StoragePlan> storage_plan{};
            const MemoryUtils::StoragePlan      *root_plan{nullptr};
        };

        [[nodiscard]] std::unordered_map<const TSValueTypeMetaData *, std::unique_ptr<WindowPlanEntry>> &
        window_plan_entries() noexcept
        {
            static std::unordered_map<const TSValueTypeMetaData *, std::unique_ptr<WindowPlanEntry>> entries;
            return entries;
        }

        [[nodiscard]] std::mutex &window_plan_mutex() noexcept
        {
            static std::mutex mutex;
            return mutex;
        }

        [[nodiscard]] ValueTypeRef window_time_binding()
        {
            auto       &registry  = TypeRegistry::instance();
            const auto *time_meta = registry.register_scalar<DateTime>("datetime");
            const auto binding   = ValuePlanFactory::instance().type_for(time_meta);
            if (!binding)
            {
                throw std::logic_error("TSDataPlanFactory: TSW time binding is not resolved");
            }
            return binding;
        }

        template <typename Storage>
        [[nodiscard]] const Storage &storage(const void *memory)
        {
            return *MemoryUtils::cast<Storage>(memory);
        }

        template <typename Storage>
        [[nodiscard]] Storage &storage(void *memory)
        {
            return *MemoryUtils::cast<Storage>(memory);
        }

        struct TSWContextCommon
        {
            const TSValueTypeMetaData      *schema{nullptr};
            const MemoryUtils::StoragePlan *value_plan{nullptr};
            TSWDataLayout                  *layout{nullptr};
            TSWDataOps                      ops{};
            IndexedValueOps                 value_ops{};

            virtual ~TSWContextCommon() = default;

            void bind_value_surface()
            {
                if (schema->value_schema == nullptr)
                {
                    throw std::logic_error("TSDataPlanFactory: TSW value schema is not resolved");
                }
                layout->value_binding = intern_value_type(*schema->value_schema, *value_plan, value_ops);
            }
        };

        template <typename Storage>
        struct TSWContextBase : TSWContextCommon
        {
            void initialise_common(const TSValueTypeMetaData &schema_,
                                   const MemoryUtils::StoragePlan &value_plan_,
                                   const ValueTypeRef &time_binding,
                                   const ValueTypeRef &element_binding,
                                   TSWDataLayout &layout_,
                                   std::size_t value_offset,
                                   std::size_t tracking_offset)
            {
                schema     = &schema_;
                value_plan = &value_plan_;
                layout     = &layout_;

                layout->element_binding = element_binding;
                layout->time_binding    = time_binding;
                layout->value_offset    = value_offset;
                layout->tracking_offset = tracking_offset;
                layout->delta_binding   = element_binding;

                configure_ts_ops();
                configure_value_ops();
            }

          protected:
            void configure_ts_ops()
            {
                ops = TSWDataOps{};
                TSDataOps &base_ops = ops;
                base_ops = TSDataOps{
                    .context                   = this,
                    .kind                      = TSTypeKind::TSW,
                    .allows_mutation           = true,
                    .layout_impl               = &window_layout,
                    .tracking_impl             = &window_tracking,
                    .mutable_tracking_impl     = &window_mutable_tracking,
                    .has_current_value_impl    = &window_has_current_value,
                    .value_memory_impl         = &window_value_memory,
                    .mutable_value_memory_impl = &window_mutable_value_memory,
                    .delta_memory_impl         = &window_delta_memory,
                    .mutable_delta_memory_impl = &window_mutable_delta_memory,
                    .copy_value_from_impl      = &window_copy_value_from,
                    .move_value_from_impl      = &window_move_value_from,
                    .empty_delta_impl          = &ts_data_detail::empty_delta_atomic,
                    .capture_delta_impl        = &ts_data_detail::capture_delta_tsw,
                    .delta_has_effect_impl     = &ts_data_detail::delta_has_effect_atomic,
                    .apply_delta_impl          = &ts_data_detail::apply_delta_tsw,
#if HGRAPH_ENABLE_PYTHON_USER_NODES
                    .from_python_impl          = &window_from_python,
                    .to_python_impl            = &window_to_python,
                    .delta_to_python_impl      = &window_delta_to_python,
#endif
                };
                ops.size_impl        = &window_size;
                ops.element_at_impl  = &window_element_at;
                ops.time_at_impl     = &window_time_at;
                ops.time_element_at_impl = &window_time_element_at;
                ops.capacity_impl    = nullptr;
                ops.full_impl        = nullptr;
                ops.push_impl        = &window_push;
                ops.clear_impl       = &window_clear;
                ops.cleared_time_impl = &window_cleared_time;
                ops.evicted_time_impl    = &window_evicted_time;
                ops.evicted_element_impl = &window_evicted_element;
            }

            [[nodiscard]] static DateTime window_evicted_time(const void *context, const void *memory) noexcept
            {
                return storage<Storage>(window_value_memory(context, memory)).evicted_time();
            }

            [[nodiscard]] static DateTime window_cleared_time(const void *context, const void *memory) noexcept
            {
                return storage<Storage>(window_value_memory(context, memory)).cleared_time();
            }

            [[nodiscard]] static const void *window_evicted_element(const void *context, const void *memory) noexcept
            {
                const auto &window = storage<Storage>(window_value_memory(context, memory));
                return window.evicted_element().has_value() ? window.evicted_element().view().data() : nullptr;
            }

            void configure_value_ops()
            {
                value_ops = IndexedValueOps{
                    {ValueOpsKind::Indexed, this, false, &window_value_hash, &window_value_equals,
                     &window_value_compare,
                     &window_value_to_string
#if HGRAPH_ENABLE_PYTHON_USER_NODES
                     ,
                     &window_value_to_python
#endif
                    },
                    &window_value_size,
                    &window_value_element_at,
                    &window_value_element_binding,
                    &window_value_make_range,
                    nullptr,
                };
                value_ops.owning_type_impl      = &window_value_owning_binding;
                value_ops.copy_construct_view_impl = &window_value_copy_construct_view;
                value_ops.copy_assign_view_impl    = &window_value_copy_assign_view;
            }

            [[nodiscard]] static const TSWContextBase *ctx(const void *context) noexcept
            {
                return static_cast<const TSWContextBase *>(context);
            }

            [[nodiscard]] static const TSDataLayout *window_layout(const void *context) noexcept
            {
                return ctx(context)->layout;
            }

            [[nodiscard]] static ValueTypeRef
            window_value_owning_binding(const void *, ValueTypeRef view_binding)
            {
                const auto binding = ValuePlanFactory::instance().type_for(view_binding.schema());
                if (binding == nullptr)
                {
                    throw std::logic_error("TSW value surface has no canonical owning binding");
                }
                return binding;
            }

            [[nodiscard]] static const void *advance(const void *memory, std::size_t offset) noexcept
            {
                return static_cast<const std::byte *>(memory) + offset;
            }

            [[nodiscard]] static void *advance(void *memory, std::size_t offset) noexcept
            {
                return static_cast<std::byte *>(memory) + offset;
            }

            [[nodiscard]] static const TSDataTracking *window_tracking(const void *context,
                                                                       const void *memory) noexcept
            {
                return MemoryUtils::cast<TSDataTracking>(advance(memory, ctx(context)->layout->tracking_offset));
            }

            [[nodiscard]] static TSDataTracking *window_mutable_tracking(const void *context, void *memory) noexcept
            {
                return MemoryUtils::cast<TSDataTracking>(advance(memory, ctx(context)->layout->tracking_offset));
            }

            [[nodiscard]] static bool window_has_current_value(const void *context, const void *memory) noexcept
            {
                return window_tracking(context, memory)->last_modified_time != MIN_DT;
            }

            [[nodiscard]] static const void *window_value_memory(const void *context, const void *memory) noexcept
            {
                return advance(memory, ctx(context)->layout->value_offset);
            }

            [[nodiscard]] static void *window_mutable_value_memory(const void *context, void *memory) noexcept
            {
                return advance(memory, ctx(context)->layout->value_offset);
            }

            [[nodiscard]] static const void *window_delta_memory(const void *context, const void *memory) noexcept
            {
                return storage<Storage>(window_value_memory(context, memory)).last_element();
            }

            [[nodiscard]] static void *window_mutable_delta_memory(const void *context, void *memory) noexcept
            {
                return const_cast<void *>(storage<Storage>(window_mutable_value_memory(context, memory)).last_element());
            }

            [[nodiscard]] static std::size_t window_size(const void *context, const void *memory)
            {
                return storage<Storage>(window_value_memory(context, memory)).size();
            }

            [[nodiscard]] static const void *window_element_at(const void *context, const void *memory,
                                                               std::size_t index)
            {
                return storage<Storage>(window_value_memory(context, memory)).element_at(index);
            }

            [[nodiscard]] static DateTime window_time_at(const void *context, const void *memory,
                                                              std::size_t index)
            {
                return storage<Storage>(window_value_memory(context, memory)).time_at(index);
            }

            [[nodiscard]] static const void *window_time_element_at(const void *context, const void *memory,
                                                                    std::size_t index)
            {
                return storage<Storage>(window_value_memory(context, memory)).time_element_at(index);
            }

            static void window_push(const void *context, void *memory, const ValueView &source,
                                    DateTime modified_time)
            {
                storage<Storage>(window_mutable_value_memory(context, memory)).push(source, modified_time);
            }

            static void window_clear(const void *context, void *memory, DateTime modified_time)
            {
                storage<Storage>(window_mutable_value_memory(context, memory)).clear_values(modified_time);
            }

            [[nodiscard]] static bool window_copy_value_from(const void *context, void *memory,
                                                             const ValueView &source,
                                                             DateTime modified_time)
            {
                const bool newly_modified =
                    window_tracking(context, memory)->last_modified_time != modified_time;
                storage<Storage>(window_mutable_value_memory(context, memory)).copy_from_value(source, modified_time);
                return newly_modified;
            }

            [[nodiscard]] static bool window_move_value_from(const void *context, void *memory,
                                                             ValueView source,
                                                             DateTime modified_time)
            {
                if (memory == nullptr)
                {
                    throw std::logic_error("TSW move requires live storage");
                }
                if (!source.has_value())
                {
                    throw std::invalid_argument("TSW move requires a live source value");
                }
                if (!source.writable_payload())
                {
                    throw std::invalid_argument("TSW move requires writable source storage");
                }
                if (modified_time == MIN_DT)
                {
                    throw std::invalid_argument("TSW move requires a concrete evaluation time");
                }

                const auto *state = ctx(context);
                const auto *source_schema = source.schema();
                if (source_schema == nullptr || source_schema->value_kind() != ValueTypeKind::List ||
                    source_schema->element_type != state->schema->value_type)
                {
                    throw std::invalid_argument("TSW move requires a list source with the window element schema");
                }

                const bool newly_modified =
                    window_tracking(context, memory)->last_modified_time != modified_time;
                storage<Storage>(window_mutable_value_memory(context, memory))
                    .move_from_value(std::move(source), modified_time);
                return newly_modified;
            }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
            [[nodiscard]] static bool is_python_sequence(nb::handle source)
            {
                nb::object object = nb::borrow<nb::object>(source);
                return nb::isinstance<nb::list>(object) || nb::isinstance<nb::tuple>(object);
            }

            [[nodiscard]] static nb::object window_to_python(const void *context, const void *memory)
            {
                return ctx(context)->layout->value_binding.ops_ref().to_python(window_value_memory(context, memory));
            }

            [[nodiscard]] static nb::object window_delta_to_python(const void *context,
                                                                   const void *memory,
                                                                   DateTime evaluation_time)
            {
                if (window_tracking(context, memory)->last_modified_time != evaluation_time) { return nb::none(); }
                const auto *delta = window_delta_memory(context, memory);
                if (delta == nullptr) { return nb::none(); }
                return ctx(context)->layout->delta_binding.ops_ref().to_python(delta);
            }

            [[nodiscard]] static bool window_from_python(const void *context,
                                                         void       *memory,
                                                         nb::handle  source,
                                                         DateTime modified_time)
            {
                if (memory == nullptr) { throw std::logic_error("TSW from_python requires live storage"); }
                if (source.is_none()) { throw std::invalid_argument("TSW from_python requires a non-None source"); }
                if (modified_time == MIN_DT)
                {
                    throw std::invalid_argument("TSW from_python requires a concrete evaluation time");
                }

                const bool newly_modified =
                    window_tracking(context, memory)->last_modified_time != modified_time;
                if (is_python_sequence(source))
                {
                    storage<Storage>(window_mutable_value_memory(context, memory))
                        .copy_from_python(source, modified_time);
                    return newly_modified;
                }

                if (!newly_modified)
                {
                    throw std::logic_error("TSW from_python allows only one window tick per evaluation time");
                }

                const auto *state = ctx(context);
                Value       element{state->layout->element_binding};
                state->layout->element_binding.ops_ref().from_python(
                    state->layout->element_binding,
                    const_cast<void *>(element.view().data()),
                    source);
                storage<Storage>(window_mutable_value_memory(context, memory)).push(element.view(), modified_time);
                return true;
            }
#endif

            [[nodiscard]] static std::size_t window_value_size(const void *context, const void *memory) noexcept
            {
                (void)context;
                return storage<Storage>(memory).size();
            }

            [[nodiscard]] static const void *window_value_element_at(const void *, const void *memory,
                                                                     std::size_t index)
            {
                return storage<Storage>(memory).element_at(index);
            }

            [[nodiscard]] static ValueTypeRef window_value_element_binding(const void *context,
                                                                                      const void *,
                                                                                      std::size_t) noexcept
            {
                return ctx(context)->layout->element_binding;
            }

            [[nodiscard]] static ValueView window_value_projector(const void *context, const void *memory,
                                                                  std::size_t index)
            {
                return ValueView{ctx(context)->layout->element_binding, storage<Storage>(memory).element_at(index)};
            }

            [[nodiscard]] static Range<ValueView> window_value_make_range(const void *context, const void *memory)
            {
                return Range<ValueView>{
                    .context   = context,
                    .memory    = memory,
                    .limit     = storage<Storage>(memory).size(),
                    .predicate = nullptr,
                    .projector = &window_value_projector,
                };
            }

            static void window_value_copy_construct_view(const void *context,
                                                         const ValueTypeRef &binding,
                                                         void *dst,
                                                         const void *memory)
            {
                if (binding.schema() == nullptr || binding.schema()->value_kind() != ValueTypeKind::List)
                {
                    throw std::logic_error("TSW value copy requires a canonical list binding");
                }
                if (binding.schema()->fixed_size == 0)
                {
                    auto storage = build_dynamic_list_storage(context, binding, memory);
                    std::construct_at(static_cast<ListStorage *>(dst), std::move(storage));
                    return;
                }

                const auto &plan = binding.checked_plan();
                plan.default_construct(dst);
                auto rollback = make_scope_exit([&]() noexcept { plan.destroy(dst); });
                window_value_copy_assign_view(context, binding, dst, memory);
                rollback.release();
            }

            static void window_value_copy_assign_view(const void *context,
                                                      const ValueTypeRef &binding,
                                                      void *dst,
                                                      const void *memory)
            {
                if (binding.schema() == nullptr || binding.schema()->value_kind() != ValueTypeKind::List)
                {
                    throw std::logic_error("TSW value copy requires a canonical list binding");
                }
                if (binding.schema()->fixed_size == 0)
                {
                    *static_cast<ListStorage *>(dst) = build_dynamic_list_storage(context, binding, memory);
                    return;
                }

                assign_fixed_list_storage(context, binding, dst, memory);
            }

            [[nodiscard]] static ValueTypeRef
            window_value_element_owning_binding(const TSWContextBase *state, const ValueTypeRef &binding)
            {
                const auto element_binding = ValuePlanFactory::instance().type_for(binding.schema()->element_type);
                if (element_binding == nullptr || element_binding != state->layout->element_binding)
                {
                    throw std::logic_error("TSW value copy element binding is not resolved");
                }
                return element_binding;
            }

            [[nodiscard]] static ListStorage build_dynamic_list_storage(const void *context,
                                                                        const ValueTypeRef &binding,
                                                                        const void *memory)
            {
                const auto *state = ctx(context);
                const auto element_binding = window_value_element_owning_binding(state, binding);
                ListBuilder builder{element_binding};
                for (const auto element : window_value_make_range(context, memory))
                {
                    builder.push_back_copy(element.data());
                }
                return builder.build_storage();
            }

            static void assign_fixed_list_storage(const void *context,
                                                  const ValueTypeRef &binding,
                                                  void *dst,
                                                  const void *memory)
            {
                const auto *state = ctx(context);
                const auto element_binding = window_value_element_owning_binding(state, binding);
                const auto &plan = binding.checked_plan();
                if (!plan.is_array() || plan.array_count() != binding.schema()->fixed_size)
                {
                    throw std::logic_error("TSW fixed value copy requires a matching array plan");
                }

                const auto &element_plan = plan.array_element_plan();
                const auto count = window_value_size(context, memory);
                if (count > binding.schema()->fixed_size)
                {
                    throw std::logic_error("TSW fixed value copy source exceeds declared fixed size");
                }

                auto *bytes = static_cast<std::byte *>(dst);
                Value default_element{element_binding};
                for (std::size_t index = 0; index < binding.schema()->fixed_size; ++index)
                {
                    const void *source = index < count ? window_value_element_at(context, memory, index)
                                                       : default_element.view().data();
                    element_plan.copy_assign(bytes + plan.element_offset(index), source);
                }
            }

            [[nodiscard]] static std::size_t window_value_hash(const void *context, const void *memory)
            {
                const auto *state = ctx(context);
                const auto &ops   = state->layout->element_binding.ops_ref();
                std::size_t seed  = 0;
                for (std::size_t index = 0; index < storage<Storage>(memory).size(); ++index)
                {
                    seed = combine_hash(seed, ops.hash(storage<Storage>(memory).element_at(index)));
                }
                return seed;
            }

            [[nodiscard]] static bool window_value_equals(const void *context, const void *lhs,
                                                          const void *rhs) noexcept
            {
                if (lhs == nullptr || rhs == nullptr) { return lhs == rhs; }

                return fallback_on_exception(false, [&]() {
                    const auto *state = ctx(context);
                    const auto &a     = storage<Storage>(lhs);
                    const auto &b     = storage<Storage>(rhs);
                    if (a.size() != b.size()) { return false; }
                    const auto &ops = state->layout->element_binding.ops_ref();
                    for (std::size_t index = 0; index < a.size(); ++index)
                    {
                        if (!ops.equals(a.element_at(index), b.element_at(index))) { return false; }
                    }
                    return true;
                });
            }

            [[nodiscard]] static std::partial_ordering window_value_compare(const void *context,
                                                                            const void *lhs,
                                                                            const void *rhs) noexcept
            {
                if (const auto order = value_ops_detail::null_order(lhs, rhs)) { return *order; }

                return fallback_on_exception(std::partial_ordering::unordered, [&]() {
                    const auto *state = ctx(context);
                    const auto &a     = storage<Storage>(lhs);
                    const auto &b     = storage<Storage>(rhs);
                    const auto &ops   = state->layout->element_binding.ops_ref();
                    const auto  count = std::min(a.size(), b.size());
                    for (std::size_t index = 0; index < count; ++index)
                    {
                        const auto order = ops.compare(a.element_at(index), b.element_at(index));
                        if (order != 0) { return order; }
                    }
                    if (a.size() < b.size()) { return std::partial_ordering::less; }
                    if (a.size() > b.size()) { return std::partial_ordering::greater; }
                    return std::partial_ordering::equivalent;
                });
            }

            [[nodiscard]] static std::string window_value_to_string(const void *context, const void *memory)
            {
                if (memory == nullptr) { return {}; }
                const auto *state = ctx(context);
                const auto &ops   = state->layout->element_binding.ops_ref();
                fmt::memory_buffer out;
                fmt::format_to(std::back_inserter(out), "[");
                for (std::size_t index = 0; index < storage<Storage>(memory).size(); ++index)
                {
                    if (index > 0) { fmt::format_to(std::back_inserter(out), ", "); }
                    fmt::format_to(std::back_inserter(out), "{}",
                                   ops.to_string(storage<Storage>(memory).element_at(index)));
                }
                fmt::format_to(std::back_inserter(out), "]");
                return fmt::to_string(out);
            }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
            [[nodiscard]] static nb::object window_value_to_python(const void *context, const void *memory)
            {
                if (memory == nullptr) { throw std::runtime_error("TSW value to_python requires live storage"); }
                const auto *state = ctx(context);
                const auto &ops   = state->layout->element_binding.ops_ref();
                const auto binding = state->layout->element_binding;
                const auto &window = storage<Storage>(memory);
                if (ops.can_to_python_buffer(binding))
                {
                    return ops.to_python_buffer(binding,
                                                ValueArraySource{
                                                    .owner      = memory,
                                                    .size       = window.size(),
                                                    .element_at = &window_buffer_element_at,
                                                });
                }

                nb::list result;
                for (std::size_t index = 0; index < window.size(); ++index)
                {
                    result.append(ops.to_python(window.element_at(index)));
                }
                return result;
            }

            [[nodiscard]] static const void *window_buffer_element_at(const void *owner, std::size_t index)
            {
                return storage<Storage>(owner).element_at(index);
            }
#endif
        };

        struct SizeTSWContext final : TSWContextBase<SizeTSWindowStorage>
        {
            SizeTSWDataLayout size_layout{};

            SizeTSWContext(const TSValueTypeMetaData &schema,
                           const MemoryUtils::StoragePlan &value_plan,
                           const ValueTypeRef &time_binding,
                           const ValueTypeRef &element_binding,
                           std::size_t value_offset,
                           std::size_t tracking_offset)
            {
                initialise_common(schema, value_plan, time_binding, element_binding, size_layout, value_offset,
                                  tracking_offset);
                size_layout.period     = schema.period();
                size_layout.min_period = schema.min_period();
                ops.capacity_impl      = &size_capacity;
                ops.full_impl          = &size_full;
                ops.all_valid_impl     = &size_all_valid;
                // A tick window is VALID only once it holds min_period
                // elements (hgraph: consumers below the minimum see an
                // invalid input, not a short window).
                ops.has_current_value_impl = &size_has_current_value;
            }

          private:
            [[nodiscard]] static const SizeTSWDataLayout &layout_for(const void *context) noexcept
            {
                return static_cast<const SizeTSWDataLayout &>(*ctx(context)->layout);
            }

            [[nodiscard]] static std::size_t size_capacity(const void *context, const void *) noexcept
            {
                return layout_for(context).period;
            }

            [[nodiscard]] static bool size_full(const void *context, const void *memory)
            {
                const auto &layout = layout_for(context);
                return layout.period != 0 && window_size(context, memory) == layout.period;
            }

            [[nodiscard]] static bool size_all_valid(const void *context, const void *memory)
            {
                return window_size(context, memory) >= layout_for(context).min_period;
            }

            [[nodiscard]] static bool size_has_current_value(const void *context, const void *memory) noexcept
            {
                static_cast<void>(context);
                return window_tracking(context, memory)->last_modified_time != MIN_DT;
            }
        };

        struct TimeTSWContext final : TSWContextBase<TimeTSWindowStorage>
        {
            TimeTSWDataLayout time_layout{};

            TimeTSWContext(const TSValueTypeMetaData &schema,
                           const MemoryUtils::StoragePlan &value_plan,
                           const ValueTypeRef &time_binding,
                           const ValueTypeRef &element_binding,
                           std::size_t value_offset,
                           std::size_t tracking_offset)
            {
                initialise_common(schema, value_plan, time_binding, element_binding, time_layout, value_offset,
                                  tracking_offset);
                time_layout.time_range     = schema.time_range();
                time_layout.min_time_range = schema.min_time_range();
                ops.capacity_impl          = &time_capacity;
                ops.full_impl              = &time_full;
                ops.all_valid_impl         = &time_all_valid;
                // A window is valid from its first value. ``all_valid`` is the
                // separate minimum-range readiness predicate.
                ops.has_current_value_impl = &time_has_current_value;
            }

          private:
            [[nodiscard]] static const TimeTSWDataLayout &layout_for(const void *context) noexcept
            {
                return static_cast<const TimeTSWDataLayout &>(*ctx(context)->layout);
            }

            [[nodiscard]] static std::size_t time_capacity(const void *, const void *) noexcept
            {
                return 0;
            }

            [[nodiscard]] static bool time_full(const void *, const void *) noexcept
            {
                return false;
            }

            [[nodiscard]] static bool time_all_valid(const void *context, const void *memory)
            {
                const auto &window = storage<TimeTSWindowStorage>(window_value_memory(context, memory));
                if (window.empty()) { return false; }

                const auto &layout = layout_for(context);
                if (layout.min_time_range <= TimeDelta{0}) { return true; }
                return window.time_at(window.size() - 1) - window.time_at(0) >= layout.min_time_range;
            }

            [[nodiscard]] static bool time_has_current_value(const void *context, const void *memory) noexcept
            {
                static_cast<void>(context);
                return window_tracking(context, memory)->last_modified_time != MIN_DT;
            }
        };

        struct TSWContextKey
        {
            const TSValueTypeMetaData      *schema{nullptr};
            const MemoryUtils::StoragePlan *plan{nullptr};
            std::size_t                     value_offset{0};
            std::size_t                     tracking_offset{0};
            TypeRole                        role{TypeRole::Invalid};
            bool                            embedded{false};

            [[nodiscard]] bool operator==(const TSWContextKey &) const noexcept = default;
        };

        struct TSWContextKeyHash
        {
            [[nodiscard]] std::size_t operator()(const TSWContextKey &key) const noexcept
            {
                auto seed = combine_hash(std::hash<const TSValueTypeMetaData *>{}(key.schema),
                                         std::hash<const MemoryUtils::StoragePlan *>{}(key.plan));
                seed = combine_hash(seed, key.value_offset);
                seed = combine_hash(seed, key.tracking_offset);
                seed = combine_hash(seed, static_cast<std::size_t>(key.role));
                seed = combine_hash(seed, key.embedded);
                return seed;
            }
        };

        [[nodiscard]] std::unordered_map<TSWContextKey, std::unique_ptr<TSWContextCommon>, TSWContextKeyHash> &
        window_contexts() noexcept
        {
            static std::unordered_map<TSWContextKey, std::unique_ptr<TSWContextCommon>, TSWContextKeyHash> contexts;
            return contexts;
        }

        [[nodiscard]] std::mutex &window_context_mutex() noexcept
        {
            static std::mutex mutex;
            return mutex;
        }
    } // namespace

    [[nodiscard]] bool is_window_ts_data(const TSValueTypeMetaData &schema) noexcept
    {
        return schema.kind == TSTypeKind::TSW && schema.value_type != nullptr && schema.value_schema != nullptr &&
               schema.delta_value_schema != nullptr;
    }

    [[nodiscard]] const MemoryUtils::StoragePlan *synthesise_window_plan(const TSValueTypeMetaData &schema)
    {
        if (!is_window_ts_data(schema))
        {
            throw std::logic_error("TSDataPlanFactory: TSW storage requires a TSW schema");
        }
        const auto element_binding = ValuePlanFactory::instance().type_for(schema.value_type);
        if (!element_binding)
        {
            throw std::logic_error("TSDataPlanFactory: TSW element binding is not resolved");
        }
        const auto time_binding = window_time_binding();

        std::lock_guard<std::mutex> lock(window_plan_mutex());
        auto                       &entries = window_plan_entries();
        if (const auto it = entries.find(&schema); it != entries.end()) { return it->second->root_plan; }

        auto entry = std::make_unique<WindowPlanEntry>();
        if (schema.is_duration_based())
        {
            entry->context = TimeWindowStoragePlanContext{
                .time_binding    = time_binding,
                .element_binding = element_binding,
                .time_range      = schema.time_range(),
            };
            entry->storage_plan = std::make_unique<MemoryUtils::StoragePlan>(MemoryUtils::StoragePlan{
                .layout                       = MemoryUtils::layout_for<TimeTSWindowStorage>(),
                .lifecycle                    = {.construct      = &time_window_storage_construct,
                                                 .destroy        = &window_storage_destroy<TimeTSWindowStorage>,
                                                 .copy_construct = &window_storage_copy_construct<TimeTSWindowStorage>,
                                                 .move_construct = &window_storage_move_construct<TimeTSWindowStorage>,
                                                 .copy_assign    = &window_storage_copy_assign<TimeTSWindowStorage>,
                                                 .move_assign    = &window_storage_move_assign<TimeTSWindowStorage>},
                .lifecycle_context            = &std::get<TimeWindowStoragePlanContext>(entry->context),
                .composite_kind_tag           = MemoryUtils::CompositeKind::None,
                .trivially_destructible       = false,
                .trivially_copyable           = false,
                .trivially_move_constructible = false,
            });
        }
        else
        {
            entry->context = SizeWindowStoragePlanContext{
                .time_binding    = time_binding,
                .element_binding = element_binding,
                .period          = schema.period(),
            };
            entry->storage_plan = std::make_unique<MemoryUtils::StoragePlan>(MemoryUtils::StoragePlan{
                .layout                       = MemoryUtils::layout_for<SizeTSWindowStorage>(),
                .lifecycle                    = {.construct      = &size_window_storage_construct,
                                                 .destroy        = &window_storage_destroy<SizeTSWindowStorage>,
                                                 .copy_construct = &window_storage_copy_construct<SizeTSWindowStorage>,
                                                 .move_construct = &window_storage_move_construct<SizeTSWindowStorage>,
                                                 .copy_assign    = &window_storage_copy_assign<SizeTSWindowStorage>,
                                                 .move_assign    = &window_storage_move_assign<SizeTSWindowStorage>},
                .lifecycle_context            = &std::get<SizeWindowStoragePlanContext>(entry->context),
                .composite_kind_tag           = MemoryUtils::CompositeKind::None,
                .trivially_destructible       = false,
                .trivially_copyable           = false,
                .trivially_move_constructible = false,
            });
        }

        auto builder = MemoryUtils::named_tuple();
        builder.reserve(2);
        builder.add_field("window", *entry->storage_plan);
        builder.add_field("tracking", MemoryUtils::plan_for<TSDataTracking>());
        entry->root_plan = &builder.build();

        const auto *result = entry->root_plan;
        entries.emplace(&schema, std::move(entry));
        return result;
    }

    [[nodiscard]] const TSDataOps &window_ts_data_ops(const TSValueTypeMetaData      &schema,
                                                      const MemoryUtils::StoragePlan &plan,
                                                      std::size_t value_offset,
                                                      std::size_t tracking_offset,
                                                      TypeRole role,
                                                      bool embedded)
    {
        const auto element_binding = ValuePlanFactory::instance().type_for(schema.value_type);
        if (!element_binding)
        {
            throw std::logic_error("TSDataPlanFactory: TSW element binding is not resolved");
        }
        const auto time_binding = window_time_binding();

        std::lock_guard<std::mutex> lock(window_context_mutex());
        auto                       &contexts = window_contexts();
        const TSWContextKey         key{&schema, &plan, value_offset, tracking_offset, role, embedded};
        if (const auto it = contexts.find(key); it != contexts.end()) { return it->second->ops; }

        const auto *window_component = plan.find_component("window");
        if (window_component == nullptr || window_component->plan == nullptr)
        {
            throw std::logic_error("TSDataPlanFactory: TSW TSData plan is missing window storage");
        }

        std::unique_ptr<TSWContextCommon> context;
        if (schema.is_duration_based())
        {
            context = std::make_unique<TimeTSWContext>(schema, *window_component->plan, time_binding, element_binding,
                                                       value_offset, tracking_offset);
        }
        else
        {
            context = std::make_unique<SizeTSWContext>(schema, *window_component->plan, time_binding, element_binding,
                                                       value_offset, tracking_offset);
        }
        auto *result = context.get();
        contexts.emplace(key, std::move(context));
        result->bind_value_surface();
        return result->ops;
    }

    void clear_window_ts_data_contexts() noexcept
    {
        {
            std::lock_guard<std::mutex> lock(window_context_mutex());
            window_contexts().clear();
        }
        {
            std::lock_guard<std::mutex> lock(window_plan_mutex());
            window_plan_entries().clear();
        }
    }
} // namespace hgraph::ts_data_plan_factory_detail
