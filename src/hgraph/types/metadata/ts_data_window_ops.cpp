#include <hgraph/types/metadata/ts_data_plan_factory_detail.h>

#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/time_series/ts_data/impl/current_state_ops.h>
#include <hgraph/types/time_series/ts_data/impl/checkpoint.h>
#include <hgraph/types/value/specialized_views.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/util/scope.h>

#include <hgraph/types/python_ops.h>

#include "detail/ts_data_seams.h"
#include "detail/window_change.h"
#include "../time_series/ts_data/ownership.h"
#include "../../runtime/window_removals.h"
#include <hgraph/util/tagged_ptr.h>

#include <fmt/format.h>

#include <algorithm>
#include <compare>
#include <cstddef>
#include <cstring>
#include <limits>
#include <iterator>
#include <memory>
#include <mutex>
#include <span>

#include <hgraph/types/utils/counted_mutex.h>
#include <new>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

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

        struct RemovedWindowSample
        {
            Value value{};
            DateTime time{MIN_DT};
        };

        // Registrations are 16-byte aligned. Sharing this word with the four
        // tick flags keeps idle and standalone windows at their original size.
        class WindowTickState
        {
          public:
            enum Effect : unsigned { Added = 1, Cleared = 2, Replaced = 4, Applied = 8 };
            explicit WindowTickState(std::uintptr_t flags = 0) noexcept : state_(nullptr, flags) {}
            WindowTickState(const WindowTickState &other) noexcept : WindowTickState(other.state_.tag()) {}
            WindowTickState &operator=(const WindowTickState &other) noexcept
            {
                if (this != &other) { cancel(); state_.set_tag(other.state_.tag()); }
                return *this;
            }
            WindowTickState(WindowTickState &&other) noexcept : state_(std::exchange(other.state_, {})) {}
            WindowTickState &operator=(WindowTickState &&other) noexcept
            {
                if (this != &other) { cancel(); state_ = std::exchange(other.state_, {}); }
                return *this;
            }
            ~WindowTickState() { cancel(); }

            bool has(Effect flag) const noexcept { return (state_.tag() & flag) != 0; }
            void mark(Effect flag) noexcept { state_.set_tag(state_.tag() | flag); }
            WindowRemovalRegistration *registration() const noexcept { return state_.as<WindowRemovalRegistration>(); }
            void registration(WindowRemovalRegistration *entry) noexcept { state_.set_ptr(entry); }

          private:
            void cancel() noexcept
            {
                if (auto *entry = registration()) { cancel_window_removals(entry); registration(nullptr); }
            }
            erased_tagged_ptr<16, 4> state_{};
        };

        // The first removed sample stays inline: an ordinary fixed-count roll
        // needs no removal-list allocation. Larger clears and duration drops spill.
        struct WindowTick
        {
            DateTime time{MIN_DT};
            WindowTickState state{};
            using Batch = std::vector<RemovedWindowSample>;
            struct RingRemoval
            {
                std::size_t slot{};
                std::shared_ptr<const WindowRemovalClock> clock{};
            };
            // This variant owns the object lifetime at the old head, while the
            // ring continues to own the backing allocation. Moving the ring
            // transfers that lifetime; a detached copy materializes the sample.
            struct OwnedRingRemoval
            {
                struct DestroyInPlace
                {
                    ValueTypeRef binding;
                    void operator()(void *value) const noexcept { binding.destroy_at(value); }
                };
                std::unique_ptr<void, DestroyInPlace> slot;
                DateTime time;
                OwnedRingRemoval(ValueTypeRef binding, void *value, DateTime sample_time) noexcept
                    : slot(value, DestroyInPlace{binding}), time(sample_time) {}
                ValueView view() const { return {slot.get_deleter().binding, slot.get()}; }
            };
            // A retired tick remembers only its timestamp. Even a writable cursor
            // kept past release cannot turn a prior clear+arrival into a partial delta.
            std::variant<RemovedWindowSample, Batch, RingRemoval, OwnedRingRemoval, std::monostate> removals{};
            bool retired() const noexcept
            {
                const auto *ring = std::get_if<RingRemoval>(&removals);
                return ring ? ring->clock && ring->clock->closed(time) : std::holds_alternative<std::monostate>(removals);
            }

            void retire() noexcept
            {
                state = WindowTickState{};
                removals.emplace<std::monostate>();
            }

            std::size_t size() const noexcept
            {
                if (const auto *single = std::get_if<RemovedWindowSample>(&removals))
                    return single->value.has_value() ? 1 : 0;
                const auto *batch = std::get_if<Batch>(&removals);
                if (batch) { return batch->size(); }
                if (std::holds_alternative<OwnedRingRemoval>(removals)) { return 1; }
                return std::holds_alternative<RingRemoval>(removals) && !retired() ? 1 : 0;
            }

            const RemovedWindowSample &at(std::size_t index) const
            {
                if (const auto *single = std::get_if<RemovedWindowSample>(&removals))
                {
                    if (index == 0 && single->value.has_value()) { return *single; }
                    throw std::out_of_range("TSW removed sample index out of range");
                }
                return std::get<Batch>(removals).at(index);
            }

            template <typename Read>
            void append(std::size_t count, Read read)
            {
                if (count == 0 || retired()) { return; }
                if (auto *single = std::get_if<RemovedWindowSample>(&removals))
                {
                    const bool retain_first = single->value.has_value();
                    if (!retain_first && count == 1)
                    {
                        *single = read(0);
                        return;
                    }
                    Batch batch;
                    batch.reserve(count + retain_first);
                    if (retain_first) { batch.emplace_back(); }
                    for (std::size_t i = 0; i < count; ++i) { batch.push_back(read(i)); }
                    // All throwing copies are complete before moving the inline sample.
                    if (retain_first) { batch.front() = std::move(*single); }
                    removals = std::move(batch);
                    return;
                }
                auto &batch = std::get<Batch>(removals);
                const auto previous = batch.size();
                batch.reserve(previous + count);
                auto rollback = make_scope_exit([&] { batch.resize(previous); });
                for (std::size_t i = 0; i < count; ++i) { batch.push_back(read(i)); }
                rollback.release();
            }

            DynamicStorageMetrics dynamic_storage_metrics() const noexcept
            {
                if (const auto *single = std::get_if<RemovedWindowSample>(&removals))
                    return single->value.dynamic_storage_metrics();
                if (const auto *owned = std::get_if<OwnedRingRemoval>(&removals))
                    return owned->view().dynamic_storage_metrics();
                // The ring slot itself already lies in the reserved ring.
                if (!std::holds_alternative<Batch>(removals)) { return {}; }
                const auto &batch = std::get<Batch>(removals);
                DynamicStorageMetrics result{.live_bytes = batch.size() * sizeof(RemovedWindowSample),
                                             .reserved_bytes = batch.capacity() * sizeof(RemovedWindowSample)};
                for (const auto &sample : batch) { result += sample.value.dynamic_storage_metrics(); }
                return result;
            }
        };

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
                  tick_(other.detached_tick())
            {
                copy_live_from(other);
            }

            TSWindowStorageCore &operator=(const TSWindowStorageCore &other)
            {
                if (this != &other)
                {
                    invalidate_delta_projection();
                    tick_ = {};
                    clear();
                    deallocate();
                    time_binding_    = other.time_binding_;
                    element_binding_ = other.element_binding_;
                    tick_            = other.detached_tick();
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
                  tick_(std::exchange(other.tick_, {}))
            {
                if (auto *entry = tick_.state.registration()) { relocate_window_removals(entry, this); }
                other.invalidate_delta_projection();
                other.time_binding_    = nullptr;
                other.element_binding_ = nullptr;
            }

            TSWindowStorageCore &operator=(TSWindowStorageCore &&other) noexcept
            {
                if (this != &other)
                {
                    invalidate_delta_projection();
                    tick_ = {};
                    clear();
                    deallocate();
                    time_binding_          = other.time_binding_;
                    element_binding_       = other.element_binding_;
                    time_bytes_            = std::exchange(other.time_bytes_, nullptr);
                    value_bytes_           = std::exchange(other.value_bytes_, nullptr);
                    capacity_              = std::exchange(other.capacity_, 0);
                    size_                  = std::exchange(other.size_, 0);
                    head_                  = std::exchange(other.head_, 0);
                    tick_                  = std::exchange(other.tick_, {});
                    if (auto *entry = tick_.state.registration()) { relocate_window_removals(entry, this); }
                    other.invalidate_delta_projection();
                    other.time_binding_    = nullptr;
                    other.element_binding_ = nullptr;
                }
                return *this;
            }

            ~TSWindowStorageCore()
            {
                tick_ = {};
                clear();
                deallocate();
            }

            // Keyed, indexed and range projections borrow the same child delta.
            // Avoid constructing that owning bundle unless a consumer reads it.
            template <typename Build> ValueView delta_projection(bool snapshot, Build build) const
            {
                if (!delta_projection_) { delta_projection_ = std::make_unique<WindowProjections>(); }
                auto &projection = snapshot ? delta_projection_->snapshot : delta_projection_->change;
                if (!projection.has_value()) { projection = build(); }
                return projection.view();
            }

            void invalidate_delta_projection() noexcept { delta_projection_.reset(); }

            [[nodiscard]] std::size_t size() const noexcept { return size_; }
            [[nodiscard]] bool empty() const noexcept { return size_ == 0; }
            [[nodiscard]] ValueTypeRef time_binding() const { return time_binding_; }
            [[nodiscard]] ValueTypeRef element_binding() const { return element_binding_; }

            // Value is an escaping owner and may select a different representation.
            // A staged arrival must use the exact binding of the receiver's ring.
            using ElementStorage = MemoryUtils::ErasedOwner<MemoryUtils::InlineStoragePolicy<>, TypeRecord>;
            [[nodiscard]] ElementStorage stage_element(const ValueView &source) const
            {
                if (!source.has_value() || source.schema() != element_binding_.schema())
                    throw std::invalid_argument("TSW arrival requires the window element type");
                if (source.binding() == element_binding_)
                    return ElementStorage::owning_copy(*element_binding_.record(), source.data());
                ElementStorage retained{*element_binding_.record()};
                element_binding_.ops_ref().copy_assign_from(element_binding_, retained.data(),
                                                            source.binding(), source.data());
                return retained;
            }

            /** Exact raw-buffer ownership plus recursively owned payloads. */
            [[nodiscard]] DynamicStorageMetrics dynamic_storage_metrics() const noexcept
            {
                const std::size_t slot_bytes = value_stride() + time_stride();
                const bool live_departure = std::holds_alternative<WindowTick::OwnedRingRemoval>(tick_.removals) ||
                    (std::holds_alternative<WindowTick::RingRemoval>(tick_.removals) && !tick_.retired());
                DynamicStorageMetrics result{
                    .live_bytes     = (size_ + live_departure) * slot_bytes,
                    .reserved_bytes = capacity_ * slot_bytes,
                };
                const auto &ops = element_binding_.ops_ref();
                for (std::size_t index = 0; index < size_; ++index)
                {
                    result += ops.dynamic_storage_metrics(element_at(index));
                }
                result += tick_.dynamic_storage_metrics();
                if (delta_projection_)
                {
                    result += {sizeof(WindowProjections), sizeof(WindowProjections)};
                    result += delta_projection_->change.dynamic_storage_metrics();
                    result += delta_projection_->snapshot.dynamic_storage_metrics();
                }
                return result;
            }

            [[nodiscard]] const WindowTick &tick() const noexcept { return tick_; }

            void track_removals(const TSDataTracking &tracking)
            {
                if (auto *ring = std::get_if<WindowTick::RingRemoval>(&tick_.removals))
                {
                    auto rollback = make_scope_exit([&] noexcept { tick_.retire(); });
                    observe_window_removal(tracking.parent, ring->clock);
                    rollback.release();
                    return;
                }
                track_owned_removals(tracking);
            }

            void track_owned_removals(const TSDataTracking &tracking) const
            {
                if (!tick_.state.registration() && tick_.size() != 0)
                {
                    auto *entry = track_window_removals(tracking.parent, tick_.time, const_cast<TSWindowStorageCore *>(this), [](void *owner, WindowRemovalRegistration *entry) noexcept {
                        auto &window = *static_cast<TSWindowStorageCore *>(owner);
                        window.tick_.state.registration(entry);
                        if (entry) { return; }
                        // A removal-bearing change is indivisible: dropping the exits
                        // also retires the old arrival/clear and its borrowed projection.
                        window.tick_.retire();
                        window.invalidate_delta_projection();
                    });
                    tick_.state.registration(entry);
                }
            }

            [[nodiscard]] DateTime replaced_time() const noexcept { return tick_.state.has(WindowTickState::Replaced) ? tick_.time : MIN_DT; }

            [[nodiscard]] ValueView removal_value(std::size_t index) const
            {
                if (const auto *ring = std::get_if<WindowTick::RingRemoval>(&tick_.removals))
                    return {element_binding_, value_slot(ring->slot)};
                if (const auto *owned = std::get_if<WindowTick::OwnedRingRemoval>(&tick_.removals))
                    return owned->view();
                return tick_.at(index).value.view();
            }

            [[nodiscard]] DateTime removal_time(std::size_t index) const
            {
                if (const auto *ring = std::get_if<WindowTick::RingRemoval>(&tick_.removals))
                    return time_at_physical(ring->slot);
                if (const auto *owned = std::get_if<WindowTick::OwnedRingRemoval>(&tick_.removals))
                    return owned->time;
                return tick_.at(index).time;
            }

            [[nodiscard]] WindowTick detached_tick() const
            {
                WindowTick copy{.time = tick_.time, .state = tick_.state};
                if (tick_.retired()) { copy.retire(); }
                else if (const auto *one = std::get_if<RemovedWindowSample>(&tick_.removals)) { copy.removals = *one; }
                else if (const auto *batch = std::get_if<WindowTick::Batch>(&tick_.removals)) { copy.removals = *batch; }
                else { copy.removals = RemovedWindowSample{Value{removal_value(0)}, removal_time(0)}; }
                return copy;
            }

            void materialize_ring_removal()
            {
                if (!std::holds_alternative<WindowTick::RingRemoval>(tick_.removals) &&
                    !std::holds_alternative<WindowTick::OwnedRingRemoval>(tick_.removals)) { return; }
                if (tick_.retired()) { tick_.retire(); }
                else { tick_.removals = RemovedWindowSample{Value{removal_value(0)}, removal_time(0)}; }
            }

            [[nodiscard]] ValueView evicted_element() const
            {
                return !tick_.state.has(WindowTickState::Cleared) && tick_.size() ? removal_value(tick_.size() - 1) : ValueView{};
            }
            [[nodiscard]] DateTime evicted_time() const noexcept { return !tick_.state.has(WindowTickState::Cleared) && tick_.size() ? tick_.time : MIN_DT; }
            [[nodiscard]] DateTime cleared_time() const noexcept { return tick_.state.has(WindowTickState::Cleared) ? tick_.time : MIN_DT; }

            void begin_replacement(DateTime modified_time)
            {
                invalidate_delta_projection();
                clear();
                if (tick_.time == modified_time && tick_.retired()) { return; }
                tick_ = WindowTick{.time = modified_time, .state = WindowTickState{WindowTickState::Replaced}};
            }

            void mark_replacement(DateTime modified_time)
            {
                invalidate_delta_projection();
                if (tick_.time == modified_time && tick_.retired()) { return; }
                tick_ = WindowTick{.time = modified_time, .state = WindowTickState{WindowTickState::Replaced}};
                if (!empty()) { tick_.state.mark(WindowTickState::Added); }
            }

            void clear_values(DateTime modified_time)
            {
                record_removals(size_, modified_time);
                if (!tick_.retired()) { tick_.state.mark(WindowTickState::Cleared); }
                clear();
            }

            /** Cold load a validated chronological image in one allocation.
             * This bypasses push: expiry and transient removals belong to the
             * next evaluation, never to checkpoint import. Work is O(live).
             */
            void restore_checkpoint(const IndexedValueView &values, std::span<const DateTime> times)
            {
                discard_history();
                reserve_exact(values.size());
                for (std::size_t index = 0; index < values.size(); ++index)
                {
                    append(values.at(index), times[index]);
                }
            }

            void discard_history() noexcept
            {
                invalidate_delta_projection();
                clear();
                tick_ = {};
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

            void note_arrival(DateTime modified_time)
            {
                begin_tick(modified_time);
                if (!tick_.retired()) { tick_.state.mark(WindowTickState::Added); }
            }

            void roll_owned(const ValueView &source, DateTime modified_time)
            {
                validate_source(source);
                const auto spare = head_ == 0 ? capacity_ - 1 : head_ - 1;
                if (std::holds_alternative<WindowTick::OwnedRingRemoval>(tick_.removals))
                {
                    // Standalone writes can begin before the old observation is
                    // released. The caller may even be reinserting that departure.
                    auto retained = stage_element(source);
                    overwrite_oldest({element_binding_, retained.data()}, modified_time);
                    note_arrival(modified_time);
                    return;
                }
                // Construct first: a throwing payload copy leaves the previous
                // event and every live ring object intact.
                copy_construct_slot(spare, source.data(), modified_time);
                invalidate_delta_projection();
                tick_.state = WindowTickState{WindowTickState::Added};
                tick_.time = modified_time;
                tick_.removals.emplace<WindowTick::OwnedRingRemoval>(
                    element_binding_, value_slot(head_), time_at_physical(head_));
                head_ = head_ + 1 == capacity_ ? 0 : head_ + 1;
            }

            void roll_trivial(const ValueView &source, DateTime modified_time)
            {
                validate_source(source);
                const auto spare = head_ == 0 ? capacity_ - 1 : head_ - 1;
                // An arrival may refer to any current sample or to the previous
                // departure. Copy before invalidating the borrowed projection.
                std::memmove(value_slot(spare), source.data(), element_plan().layout.size);
                std::memcpy(time_slot(spare), &modified_time, sizeof(modified_time));
                invalidate_delta_projection();
                tick_.state = WindowTickState{WindowTickState::Added};
                tick_.time = modified_time;
                auto *ring = std::get_if<WindowTick::RingRemoval>(&tick_.removals);
                if (!ring) { ring = &tick_.removals.emplace<WindowTick::RingRemoval>(); }
                ring->slot = head_;
                head_ = head_ + 1 == capacity_ ? 0 : head_ + 1;
            }

            void overwrite_oldest(const ValueView &source, DateTime modified_time)
            {
                validate_source(source);
                if (capacity_ == 0) { throw std::logic_error("TSW storage has no capacity"); }

                const auto physical = head_;
                record_removals(1, modified_time);
                if (size_ < capacity_)
                {
                    // Replay of a nontrivial value can reserve one spare construction slot.
                    append(source, modified_time);
                    remove_oldest(1);
                    return;
                }
                copy_assign_value_slot(physical, source.data());
                copy_assign_time_slot(physical, modified_time);
                head_ = (head_ + 1) % capacity_;
            }

            void begin_tick(DateTime modified_time)
            {
                invalidate_delta_projection();
                if (tick_.time != modified_time) { tick_ = WindowTick{.time = modified_time}; }
            }

            void record_removals(std::size_t count, DateTime modified_time)
            {
                begin_tick(modified_time);
                materialize_ring_removal();
                tick_.append(count, [this](std::size_t index) {
                    return RemovedWindowSample{Value{ValueView{element_binding_, element_at(index)}}, time_at(index)};
                });
            }

            void evict_oldest(std::size_t count, DateTime modified_time)
            {
                record_removals(count, modified_time);
                remove_oldest(count);
            }

          public:
            void require_unapplied_tick(DateTime now) const
            {
                if (tick_.time == now && tick_.state.has(WindowTickState::Applied))
                    throw std::logic_error("TSW delta can only be applied once per evaluation time");
            }

            // A transported tick carries the source's removal timestamps. Using the
            // receiving clock to recalculate expiry would change delayed replay.
            void apply_delta(bool cleared, const ValueView &added, const IndexedValueView &removed,
                             DateTime modified_time, std::size_t fixed_period)
            {
                require_unapplied_tick(modified_time);

                // A canonical change is relative to the beginning of its cycle.
                // The root can already have removed a prefix of that same history.
                // Take the union of the two prefixes, keeping the source's original
                // times wherever its authoritative removal supplies them.
                const auto expired = tick_.time == modified_time ? tick_.size() : 0;
                const auto overlap = std::min(expired, removed.size());
                const auto drop = cleared ? size_ : removed.size() - overlap;
                if (drop > size_ || (fixed_period && size_ - drop + added.has_value() > fixed_period))
                    throw std::invalid_argument("TSW delta does not match the receiver's retained sample count");

                WindowTick next{.time = modified_time, .state = WindowTickState{WindowTickState::Applied}};
                if (added.has_value()) { next.state.mark(WindowTickState::Added); }
                if (cleared) { next.state.mark(WindowTickState::Cleared); }
                next.append(removed.size(), [&](std::size_t index) {
                    const auto sample = removed.at(index).as_bundle();
                    const auto value = sample.at(0);
                    if (value.schema() != element_binding_.schema() || !value.has_value())
                        throw std::invalid_argument("TSW removal requires a live value of its element type");
                    return RemovedWindowSample{Value{element_binding_, value}, sample.at(1).checked_as<DateTime>()};
                });
                next.append(expired - overlap, [&](std::size_t index) {
                    return RemovedWindowSample{Value{removal_value(overlap + index)}, removal_time(overlap + index)};
                });
                if (added.has_value())
                {
                    auto retained = stage_element(added);
                    const ValueView arrival{element_binding_, retained.data()};
                    // The next append may use the previous departure's position.
                    materialize_ring_removal();
                    if (size_ == capacity_ && drop && element_plan().trivially_copyable)
                    {
                        // The ordinary fixed scalar roll needs no extra ring position.
                        remove_oldest(drop);
                        append(arrival, modified_time);
                    }
                    else
                    {
                        // Finish construction before destroying history if a payload may throw.
                        if (fixed_period && size_ == capacity_) { reserve_exact(size_ + 1); }
                        else { ensure_capacity(size_ + 1); }
                        append(arrival, modified_time);
                        remove_oldest(drop);
                    }
                }
                else { remove_oldest(drop); }
                invalidate_delta_projection();
                if (tick_.time != modified_time || !tick_.retired()) { tick_ = std::move(next); }
            }

          protected:
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

            void remove_oldest(std::size_t count) noexcept
            {
                for (std::size_t i = 0; i < count; ++i)
                {
                    destroy_slot(head_);
                    head_ = (head_ + 1) % capacity_;
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
                if (new_capacity > std::numeric_limits<std::size_t>::max() / std::max(new_value_stride, new_time_stride))
                    throw std::length_error("TSW ring capacity exceeds the available address space");
                materialize_ring_removal();
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
            mutable WindowTick      tick_{};
            struct WindowProjections { Value change; Value snapshot; };
            mutable std::unique_ptr<WindowProjections> delta_projection_;
        };

        struct SizeWindowStoragePlanContext
        {
            ValueTypeRef time_binding{nullptr};
            ValueTypeRef element_binding{nullptr};
            std::size_t period{0};
            enum Rollover { Copy, TrivialRing, OwningRing } rollover{Copy};
        };

        class SizeTSWindowStorage final : public TSWindowStorageCore
        {
          public:
            explicit SizeTSWindowStorage(const SizeWindowStoragePlanContext &config)
                : TSWindowStorageCore(config.time_binding, config.element_binding), config_(&config)
            {
                if (config.period == 0) { throw std::invalid_argument("TSW fixed window requires a non-zero period"); }
                if (config.rollover != SizeWindowStoragePlanContext::Copy && config.period == std::numeric_limits<std::size_t>::max())
                    throw std::length_error("TSW ring capacity exceeds the available address space");
                reserve_exact(config.period + static_cast<std::size_t>(config.rollover != SizeWindowStoragePlanContext::Copy));
            }

            [[nodiscard]] std::size_t capacity() const noexcept { return config_->period; }

            void push(const ValueView &source, DateTime modified_time)
            {
                if (size() == capacity() && config_->rollover != SizeWindowStoragePlanContext::Copy &&
                    tick().time != modified_time)
                {
                    if (config_->rollover == SizeWindowStoragePlanContext::TrivialRing) { roll_trivial(source, modified_time); }
                    else { roll_owned(source, modified_time); }
                    return;
                }
                if (size() < capacity()) { append(source, modified_time); }
                else { overwrite_oldest(source, modified_time); }
                note_arrival(modified_time);
            }

            void copy_from_value(const ValueView &source, DateTime modified_time)
            {
                const IndexedValueView source_values = checked_source_values(source);
                if (source_values.size() > capacity())
                {
                    throw std::length_error("TSW fixed window source exceeds the configured period");
                }

                begin_replacement(modified_time);
                for (std::size_t index = 0; index < source_values.size(); ++index)
                {
                    push(source_values.at(index), modified_time);
                }
            }

            void move_from_value(ValueView source, DateTime modified_time)
            {
                const IndexedValueView source_values = checked_source_values(source);
                if (source_values.size() > capacity())
                {
                    throw std::length_error("TSW fixed window source exceeds the configured period");
                }

                begin_replacement(modified_time);
                for (std::size_t index = 0; index < source_values.size(); ++index)
                {
                    append_move(source_values.at(index), modified_time);
                    note_arrival(modified_time);
                }
            }


          private:
            const SizeWindowStoragePlanContext *config_;
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

            bool expire(DateTime modified_time)
            {
                const DateTime cutoff = modified_time - time_range_;
                std::size_t dropped = 0;
                while (dropped < size() && time_at(dropped) < cutoff) { ++dropped; }
                if (dropped == 0)
                {
                    // Discard a retired tick's transient samples on a later explicit
                    // mutation, even if the new evaluation has no expiry to publish.
                    if (modified_time > tick().time) { begin_tick(modified_time); }
                    return false;
                }
                evict_oldest(dropped, modified_time);
                return true;
            }

            void push(const ValueView &source, DateTime modified_time)
            {
                validate_source(source);
                expire(modified_time);
                ensure_capacity(size() + 1);
                append(source, modified_time);
                note_arrival(modified_time);
            }

            void copy_from_value(const ValueView &source, DateTime modified_time)
            {
                const IndexedValueView source_values = checked_source_values(source);

                begin_replacement(modified_time);
                for (std::size_t index = 0; index < source_values.size(); ++index)
                {
                    push(source_values.at(index), modified_time);
                }
            }

            void move_from_value(ValueView source, DateTime modified_time)
            {
                const IndexedValueView source_values = checked_source_values(source);

                begin_replacement(modified_time);
                ensure_capacity(source_values.size());
                for (std::size_t index = 0; index < source_values.size(); ++index)
                {
                    append_move(source_values.at(index), modified_time);
                    note_arrival(modified_time);
                }
            }


          private:
            TimeDelta time_range_{};
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
            std::construct_at(static_cast<SizeTSWindowStorage *>(dst), state);
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

        struct WindowPlanKey
        {
            const TSValueTypeMetaData *schema{nullptr};
            const TypeRecord          *element_binding{nullptr};

            [[nodiscard]] bool operator==(const WindowPlanKey &) const noexcept = default;
        };

        struct WindowPlanKeyHash
        {
            [[nodiscard]] std::size_t operator()(const WindowPlanKey &key) const noexcept
            {
                return combine_hash(std::hash<const TSValueTypeMetaData *>{}(key.schema),
                                    std::hash<const TypeRecord *>{}(key.element_binding));
            }
        };

        [[nodiscard]] std::unordered_map<WindowPlanKey, std::unique_ptr<WindowPlanEntry>,
                                         WindowPlanKeyHash> &
        window_plan_entries() noexcept
        {
            static std::unordered_map<WindowPlanKey, std::unique_ptr<WindowPlanEntry>,
                                      WindowPlanKeyHash> entries;
            return entries;
        }

        [[nodiscard]] TypeSystemMutex &window_plan_mutex() noexcept
        {
            static TypeSystemMutex mutex;
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
            /** Which storage this context drives (the seams dispatch on it). */
            bool                            time_window{false};

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
                schema      = &schema_;
                value_plan  = &value_plan_;
                layout      = &layout_;
                time_window = std::is_same_v<Storage, TimeTSWindowStorage>;

                layout->element_binding = element_binding;
                layout->time_binding    = time_binding;
                layout->value_offset    = value_offset;
                layout->tracking_offset = tracking_offset;
                layout->canonical_delta_binding = ts_data_detail::canonical_delta_binding_for(schema_);
                layout->delta_binding = layout->canonical_delta_binding;

                configure_ts_ops();
                configure_value_ops();
            }

          public:  // the RFC 0035 seams below reach these; nothing else outside the TU can
            void configure_ts_ops()
            {
                static const detail::TSDataOwnershipOps ownership{
                    .discard_child_history = [](const void *context, void *memory) noexcept {
                        storage<Storage>(window_mutable_value_memory(context, memory)).discard_history();
                    },
                };
                ops = TSWDataOps{};
                TSDataOps &base_ops = ops;
                base_ops = TSDataOps{
                    .context                   = this,
                    .kind                      = TSTypeKind::TSW,
                    .allows_mutation           = true,
                    .ownership_ops             = &ownership,
                    .current_state_ops =
                        &ts_current_state_detail::current_state_ops_for(TSTypeKind::TSW),
                    .checkpoint_ops             = &window_checkpoint_ops(),
                    .layout_impl               = &window_layout,
                    .tracking_impl             = &window_tracking,
                    .mutable_tracking_impl     = &window_mutable_tracking,
                    .has_current_value_impl    = &window_has_current_value,
                    .dynamic_storage_metrics_impl = &window_dynamic_storage_metrics,
                    .delta_view_impl           = &window_arrival_view,
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
                    .python_family             = PythonTSDataFamily::window,
                    .from_python_impl          = &python_ops_detail::forwarder<&PythonOps::TSData::window_from_python>::call,
                    .to_python_impl            = &python_ops_detail::forwarder<&PythonOps::TSData::window_to_python>::call,
                    .delta_to_python_impl      = &python_ops_detail::forwarder<&PythonOps::TSData::window_delta_to_python>::call,
                };
                ops.change_view_impl = &window_change_view;
                ops.size_impl        = &window_size;
                ops.element_at_impl  = &window_element_at;
                ops.time_at_impl     = &window_time_at;
                ops.time_element_at_impl = &window_time_element_at;
                ops.capacity_impl    = nullptr;
                ops.full_impl        = nullptr;
                ops.push_impl        = &window_push;
                ops.expire_impl      = &window_expire;
                ops.apply_window_delta_impl = &window_apply_delta;
                ops.removed_values_impl = &window_removed<ValueView>;
                ops.removed_value_times_impl = &window_removed<DateTime>;
                ops.clear_impl       = &window_clear;
                ops.replace_samples_impl = &window_replace_samples;
                ops.cleared_time_impl = &window_cleared_time;
                ops.replaced_time_impl = &window_replaced_time;
                ops.evicted_time_impl    = &window_evicted_time;
                ops.evicted_element_impl = &window_evicted_element;
            }

            [[nodiscard]] static DateTime window_evicted_time(const void *context, const void *memory) noexcept
            {
                return storage<Storage>(window_value_memory(context, memory)).evicted_time();
            }

            [[nodiscard]] static DateTime window_replaced_time(const void *context, const void *memory) noexcept
            {
                return storage<Storage>(window_value_memory(context, memory)).replaced_time();
            }

            [[nodiscard]] static DateTime window_cleared_time(const void *context, const void *memory) noexcept
            {
                return storage<Storage>(window_value_memory(context, memory)).cleared_time();
            }

            [[nodiscard]] static ValueView window_evicted_element(const void *context, const void *memory) noexcept
            {
                const auto &window = storage<Storage>(window_value_memory(context, memory));
                return window.evicted_element();
            }

            void configure_value_ops()
            {
                value_ops = IndexedValueOps{
                    {ValueOpsKind::Indexed, this, false, &window_value_hash, &window_value_equals,
                     &window_value_compare,
                     &window_value_to_string,
                     &python_ops_detail::forwarder<&PythonOps::TSData::window_value_to_python>::call
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
                value_ops.dynamic_storage_metrics_impl = &window_dynamic_storage_metrics;
            }

            [[nodiscard]] static const TSWContextBase *ctx(const void *context) noexcept
            {
                return static_cast<const TSWContextBase *>(context);
            }

            [[nodiscard]] static const TSCheckpointOps &window_checkpoint_ops() noexcept
            {
                static const TSCheckpointOps checkpoint{
                    [](const TSDataView &, const TSCheckpointContext *) { return true; },
                    &window_checkpoint_capture,
                    &window_checkpoint_validate,
                    &window_checkpoint_restore,
                };
                return checkpoint;
            }

            [[nodiscard]] static TSCheckpointImage window_checkpoint_capture(const TSDataView &view, const TSCheckpointContext *)
            {
                const auto *self = ctx(view.ops().context);
                const auto &window = storage<Storage>(window_value_memory(self, view.data()));
                TSCheckpointImage image;
                image.schema = view.schema();
                image.last_modified_time = view.last_modified_time();
                image.window_times.reserve(window.size());
                // The public count-window value schema has a fixed extent and
                // pads incomplete values. Checkpoint only live samples instead.
                ListBuilder values{self->layout->element_binding};
                for (std::size_t index = 0; index < window.size(); ++index)
                {
                    values.push_back_copy(window.element_at(index));
                    image.window_times.push_back(window.time_at(index));
                }
                image.payload = values.build();
                return image;
            }

            static void window_checkpoint_validate(const TSDataView &view, const TSCheckpointImage &image, const TSCheckpointContext *)
            {
                ts_checkpoint_detail::validate_header(view, image);
                if (!image.children.empty() || !image.keys.empty() || !image.slots.empty() ||
                    !image.free_slots.empty() || !image.published.empty() || image.slot_capacity != 0 ||
                    image.key_set_last_modified_time != MIN_DT)
                    throw std::invalid_argument("window checkpoint contains structural metadata");
                const auto *self = ctx(view.ops().context);
                if (window_size(self, view.data()) != 0)
                    throw std::invalid_argument("window checkpoint restore requires empty endpoint storage");
                const auto *schema = image.payload.schema();
                if (!image.payload.has_value() || schema == nullptr ||
                    schema->value_kind() != ValueTypeKind::List || schema->is_fixed_size() ||
                    schema->element_type != self->layout->element_binding.schema())
                    throw std::invalid_argument("window checkpoint requires a live dynamic list of its element type");
                const auto values = image.payload.view().as_indexed_view();
                if (values.size() != image.window_times.size())
                    throw std::invalid_argument("window checkpoint sample values and timestamps disagree");
                if constexpr (std::is_same_v<Storage, SizeTSWindowStorage>)
                    if (values.size() > view.schema()->period())
                        throw std::invalid_argument("window checkpoint exceeds its count period");
                DateTime previous = MIN_DT;
                for (std::size_t index = 0; index < values.size(); ++index)
                {
                    const auto value = values.at(index);
                    if (!value.has_value() || value.schema() != self->layout->element_binding.schema() ||
                        value.binding().plan() != self->layout->element_binding.plan())
                        throw std::invalid_argument("window checkpoint sample is invalid or incompatible");
                    const auto time = image.window_times[index];
                    if (time == MIN_DT || time < previous ||
                        (image.last_modified_time != MIN_DT && time > image.last_modified_time))
                        throw std::invalid_argument("window checkpoint sample timestamps are not chronological");
                    previous = time;
                }
                if constexpr (std::is_same_v<Storage, TimeTSWindowStorage>)
                    if (values.size() > 1 &&
                        static_cast<std::uint64_t>(image.window_times.back().time_since_epoch().count()) -
                            static_cast<std::uint64_t>(image.window_times.front().time_since_epoch().count()) >
                        static_cast<std::uint64_t>(view.schema()->time_range().count()))
                        throw std::invalid_argument("window checkpoint samples exceed its duration range");
            }

            static void window_checkpoint_restore(const TSDataView &view, const TSCheckpointImage &image, const TSCheckpointContext *)
            {
                const auto *self = ctx(view.ops().context);
                auto &window = storage<Storage>(window_mutable_value_memory(self, view.mutable_data()));
                window.restore_checkpoint(image.payload.view().as_indexed_view(), image.window_times);
                window_mutable_tracking(self, view.mutable_data())->last_modified_time = image.last_modified_time;
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

            [[nodiscard]] static DynamicStorageMetrics window_dynamic_storage_metrics(
                const void *context, const void *memory) noexcept
            {
                if (context == nullptr || memory == nullptr) { return {}; }
                return storage<Storage>(window_value_memory(context, memory)).dynamic_storage_metrics();
            }

            [[nodiscard]] static const void *window_value_memory(const void *context, const void *memory) noexcept
            {
                return advance(memory, ctx(context)->layout->value_offset);
            }

            [[nodiscard]] static void *window_mutable_value_memory(const void *context, void *memory) noexcept
            {
                return advance(memory, ctx(context)->layout->value_offset);
            }

            [[nodiscard]] static ValueView window_arrival_view(const void *context, const void *memory, DateTime now)
            {
                const auto &window = storage<Storage>(window_value_memory(context, memory));
                const bool arrived = now != MIN_DT && window.tick().state.has(WindowTickState::Added) && window.tick().time == now && !window.tick().retired() &&
                                     window_tracking(context, memory)->last_modified_time == now;
                return {ctx(context)->layout->element_binding, arrived ? window.last_element() : nullptr};
            }

            [[nodiscard]] static ValueView window_change_view(const void *context, const void *memory,
                                                              DateTime now, bool snapshot)
            {
                const auto &window = storage<Storage>(window_value_memory(context, memory));
                const auto binding = ctx(context)->layout->canonical_delta_binding;
                const auto last = window_tracking(context, memory)->last_modified_time;
                if (last == MIN_DT || (!snapshot && (last != now || window.tick().time != now || window.tick().retired())))
                    return {binding, nullptr};
                const bool replacement = snapshot || window.tick().state.has(WindowTickState::Replaced);
                // A cached canonical bundle owns allocations even when the raw
                // departure uses a spare ring slot. Enroll it only on this read.
                if (std::holds_alternative<WindowTick::RingRemoval>(window.tick().removals) && !window.tick().retired())
                    window.track_owned_removals(*window_tracking(context, memory));
                return window.delta_projection(replacement, [&] {
                    const auto values = replacement
                        ? Range<ValueView>{.context = context, .memory = memory, .limit = window.size(), .projector = &window_element_at}
                        : window_removed<ValueView>(context, memory, now);
                    const auto times = replacement
                        ? Range<DateTime>{.context = context, .memory = memory, .limit = window.size(), .projector = &window_time_at}
                        : window_removed<DateTime>(context, memory, now);
                    return ts_data_detail::build_window_change(
                        ctx(context)->layout->canonical_delta_binding, !replacement && window.tick().state.has(WindowTickState::Cleared),
                        replacement ? ValueView{} : window_arrival_view(context, memory, now), replacement, values, times);
                });
            }

            [[nodiscard]] static const void *window_delta_memory(const void *context, const void *memory)
            {
                return window_change_view(context, memory, window_tracking(context, memory)->last_modified_time, false).data();
            }

            [[nodiscard]] static void *window_mutable_delta_memory(const void *context, void *memory)
            {
                return const_cast<void *>(window_delta_memory(context, memory));
            }

            [[nodiscard]] static std::size_t window_size(const void *context, const void *memory)
            {
                return storage<Storage>(window_value_memory(context, memory)).size();
            }

            [[nodiscard]] static ValueView window_element_at(const void *context, const void *memory,
                                                             std::size_t index)
            {
                const auto *self = ctx(context);
                return ValueView{self->layout->element_binding,
                                 storage<Storage>(window_value_memory(context, memory)).element_at(index)};
            }

            [[nodiscard]] static DateTime window_time_at(const void *context, const void *memory,
                                                              std::size_t index)
            {
                return storage<Storage>(window_value_memory(context, memory)).time_at(index);
            }

            [[nodiscard]] static ValueView window_time_element_at(const void *context, const void *memory,
                                                                  std::size_t index)
            {
                const auto *self = ctx(context);
                return ValueView{self->layout->time_binding,
                                 storage<Storage>(window_value_memory(context, memory)).time_element_at(index)};
            }

            static void window_push(const void *context, void *memory, const ValueView &source,
                                    DateTime modified_time)
            {
                auto &window = storage<Storage>(window_mutable_value_memory(context, memory));
                if (source.has_value() && source.schema() == window.element_binding().schema() &&
                    source.binding().plan() != window.element_binding().plan())
                {
                    // Arrivals may use another owning representation of the
                    // exact payload schema (for example a mutable HGL list).
                    // Stage before eviction so a failed conversion is atomic.
                    auto retained = window.stage_element(source);
                    window.push(ValueView{window.element_binding(), retained.data()}, modified_time);
                }
                else { window.push(source, modified_time); }
                window.track_removals(*window_tracking(context, memory));
            }

            static bool window_expire(const void *context, void *memory, DateTime modified_time)
            {
                if constexpr (std::is_same_v<Storage, TimeTSWindowStorage>)
                {
                    auto &window = storage<Storage>(window_mutable_value_memory(context, memory));
                    if (!window.expire(modified_time)) { return false; }
                    window.track_removals(*window_tracking(context, memory));
                    return true;
                }
                return false;
            }

            static void window_apply_delta(const void *context, void *memory, bool cleared, const ValueView &added,
                                           const IndexedValueView &removed, DateTime modified_time)
            {
                auto &window = storage<Storage>(window_mutable_value_memory(context, memory));
                const auto period = ctx(context)->schema->is_duration_based() ? 0 : ctx(context)->schema->period();
                window.apply_delta(cleared, added, removed, modified_time, period);
                window.track_removals(*window_tracking(context, memory));
            }

            template <typename T>
            static Range<T> window_removed(const void *context, const void *memory, DateTime time)
            {
                const auto &window = storage<Storage>(window_value_memory(context, memory));
                return {.memory = &window, .limit = time == window.tick().time ? window.tick().size() : 0,
                        .projector = [](const void *, const void *data, std::size_t index) -> T {
                            const auto &owner = *static_cast<const Storage *>(data);
                            if constexpr (std::is_same_v<T, DateTime>) { return owner.removal_time(index); }
                            else { return owner.removal_value(index); }
                        }};
            }

            static void window_clear(const void *context, void *memory, DateTime modified_time)
            {
                auto &window = storage<Storage>(window_mutable_value_memory(context, memory));
                window.clear_values(modified_time);
                window.track_removals(*window_tracking(context, memory));
            }

            static void window_replace_samples(const void *context, void *memory,
                                                const ValueView &source,
                                                std::span<const DateTime> times,
                                                DateTime modified_time)
            {
                const auto *self = ctx(context);
                if (!source.has_value() || source.schema()->value_kind() != ValueTypeKind::List ||
                    source.schema()->element_type != self->schema->value_type)
                    throw std::invalid_argument("TSW sample replacement requires a list of its element type");
                const auto values = source.as_indexed_view();
                if (values.size() != times.size())
                    throw std::invalid_argument("TSW sample values and times disagree");
                if constexpr (std::is_same_v<Storage, SizeTSWindowStorage>)
                    if (values.size() > self->schema->period())
                        throw std::invalid_argument("TSW sample replacement exceeds its period");
                DateTime previous = MIN_DT;
                for (std::size_t index = 0; index < times.size(); ++index)
                {
                    if (!values.at(index).has_value() || times[index] == MIN_DT || times[index] < previous ||
                        times[index] > modified_time)
                        throw std::invalid_argument("TSW sample replacement has invalid chronological samples");
                    previous = times[index];
                }
                if constexpr (std::is_same_v<Storage, TimeTSWindowStorage>)
                    if (times.size() > 1 &&
                        static_cast<std::uint64_t>(times.back().time_since_epoch().count()) -
                            static_cast<std::uint64_t>(times.front().time_since_epoch().count()) >
                        static_cast<std::uint64_t>(self->schema->time_range().count()))
                        throw std::invalid_argument("TSW sample replacement exceeds its time range");
                auto &window = storage<Storage>(window_mutable_value_memory(context, memory));
                window.require_unapplied_tick(modified_time);
                window.restore_checkpoint(values, times);
                window.mark_replacement(modified_time);
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
                if (!binding.schema()->is_fixed_size())
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
                if (!binding.schema()->is_fixed_size())
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
                // A window is valid from its first value. all_valid is the
                // separate minimum-period readiness predicate.
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
                const auto &window = ts_data_plan_factory_detail::storage<TimeTSWindowStorage>(window_value_memory(context, memory));
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

        using WindowContextOwner =
            MemoryUtils::ErasedOwner<MemoryUtils::HeapOnlyStoragePolicy>;

        struct WindowContextEntry
        {
            WindowContextOwner owner;
            const TSWDataOps  *result;

            WindowContextEntry(WindowContextOwner owner_, const TSWDataOps &result_)
                : owner(std::move(owner_)), result(&result_)
            {}

            WindowContextEntry(const WindowContextEntry &) = delete;
            WindowContextEntry &operator=(const WindowContextEntry &) = delete;
            WindowContextEntry(WindowContextEntry &&) noexcept = default;
            WindowContextEntry &operator=(WindowContextEntry &&) noexcept = default;
        };

        template <typename Context, typename... Args>
        [[nodiscard]] WindowContextEntry make_window_context_entry(Args &&...args)
        {
            const auto &context_plan = MemoryUtils::plan_for<Context>();
            auto owner = WindowContextOwner::owning_constructed(
                context_plan, [&](void *memory) {
                    std::construct_at(MemoryUtils::cast<Context>(memory),
                                      std::forward<Args>(args)...);
                });
            auto *context = owner.template as<Context>();
            context->bind_value_surface();
            return WindowContextEntry{std::move(owner), context->ops};
        }

        struct TSWContextKey
        {
            const TSValueTypeMetaData      *schema{nullptr};
            const MemoryUtils::StoragePlan *plan{nullptr};
            std::size_t                     value_offset{0};
            std::size_t                     tracking_offset{0};
            const TypeRecord *element_binding{nullptr};
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
                seed = combine_hash(seed, std::hash<const TypeRecord *>{}(key.element_binding));
                seed = combine_hash(seed, static_cast<std::size_t>(key.role));
                seed = combine_hash(seed, key.embedded);
                return seed;
            }
        };

        [[nodiscard]] std::unordered_map<TSWContextKey, WindowContextEntry, TSWContextKeyHash> &
        window_contexts() noexcept
        {
            static std::unordered_map<TSWContextKey, WindowContextEntry, TSWContextKeyHash> contexts;
            return contexts;
        }

        [[nodiscard]] TypeSystemMutex &window_context_mutex() noexcept
        {
            static TypeSystemMutex mutex;
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
        return synthesise_window_plan(
            schema, ValuePlanFactory::instance().type_for(schema.value_type));
    }

    [[nodiscard]] const MemoryUtils::StoragePlan *synthesise_window_plan(
        const TSValueTypeMetaData &schema,
        ValueTypeRef element_binding)
    {
        if (!is_window_ts_data(schema))
        {
            throw std::logic_error("TSDataPlanFactory: TSW storage requires a TSW schema");
        }
        if (!element_binding || element_binding.schema() != schema.value_type)
        {
            throw std::logic_error("TSDataPlanFactory: TSW element binding is not resolved");
        }
        const auto time_binding = window_time_binding();
        const WindowPlanKey key{&schema, element_binding.record()};

        std::lock_guard lock(window_plan_mutex());
        auto                       &entries = window_plan_entries();
        if (const auto it = entries.find(key); it != entries.end()) { return it->second->root_plan; }

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
            const auto &plan = element_binding.checked_plan();
            const auto &ops = element_binding.ops_ref();
            // Only a self-owning native scalar can stay in a spare ring slot.
            // A custom value projection may require a different owning copy.
            auto rollover = SizeWindowStoragePlanContext::Copy;
            if (element_binding.schema()->value_kind() == ValueTypeKind::Atomic &&
                !ops.owning_type_impl && !ops.copy_construct_view_impl && plan.can_copy_construct() && plan.can_copy_assign())
            {
                rollover = plan.trivially_copyable && plan.trivially_destructible
                    ? SizeWindowStoragePlanContext::TrivialRing : SizeWindowStoragePlanContext::OwningRing;
            }
            entry->context = SizeWindowStoragePlanContext{
                .time_binding    = time_binding,
                .element_binding = element_binding,
                .period          = schema.period(),
                .rollover        = rollover,
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
        entries.emplace(key, std::move(entry));
        return result;
    }

    [[nodiscard]] const TSDataOps &window_ts_data_ops(const TSValueTypeMetaData      &schema,
                                                      const MemoryUtils::StoragePlan &plan,
                                                      std::size_t value_offset,
                                                      std::size_t tracking_offset,
                                                      TypeRole role,
                                                      bool embedded)
    {
        return window_ts_data_ops(
            schema, plan, value_offset, tracking_offset,
            ValuePlanFactory::instance().type_for(schema.value_type), role, embedded);
    }

    [[nodiscard]] const TSDataOps &window_ts_data_ops(const TSValueTypeMetaData      &schema,
                                                      const MemoryUtils::StoragePlan &plan,
                                                      std::size_t value_offset,
                                                      std::size_t tracking_offset,
                                                      ValueTypeRef element_binding,
                                                      TypeRole role,
                                                      bool embedded)
    {
        if (!element_binding || element_binding.schema() != schema.value_type)
        {
            throw std::logic_error("TSDataPlanFactory: TSW element binding is not resolved");
        }
        const auto time_binding = window_time_binding();

        std::lock_guard lock(window_context_mutex());
        auto                       &contexts = window_contexts();
        const TSWContextKey key{
            &schema, &plan, value_offset, tracking_offset, element_binding.record(), role, embedded};
        if (const auto it = contexts.find(key); it != contexts.end()) { return *it->second.result; }

        const auto *window_component = plan.find_component("window");
        if (window_component == nullptr || window_component->plan == nullptr)
        {
            throw std::logic_error("TSDataPlanFactory: TSW TSData plan is missing window storage");
        }

        WindowContextEntry context = schema.is_duration_based()
                                         ? make_window_context_entry<TimeTSWContext>(
                                               schema, *window_component->plan, time_binding,
                                               element_binding, value_offset, tracking_offset)
                                         : make_window_context_entry<SizeTSWContext>(
                                               schema, *window_component->plan, time_binding,
                                               element_binding, value_offset, tracking_offset);
        const auto *result = context.result;
        contexts.emplace(key, std::move(context));
        return *result;
    }

    void clear_window_ts_data_contexts() noexcept
    {
        {
            std::lock_guard lock(window_context_mutex());
            window_contexts().clear();
        }
        {
            std::lock_guard lock(window_plan_mutex());
            window_plan_entries().clear();
        }
    }
} // namespace hgraph::ts_data_plan_factory_detail

// -- RFC 0035 seams: the window strategies for ts_data_family_conversions.cpp -
namespace hgraph::ts_data_seams
{
    namespace
    {
        using namespace ts_data_plan_factory_detail;
        using SizeBase = TSWContextBase<SizeTSWindowStorage>;
        using TimeBase = TSWContextBase<TimeTSWindowStorage>;

        [[nodiscard]] const TSWContextCommon &common(const void *context) noexcept
        {
            return *static_cast<const TSWContextCommon *>(context);
        }

        template <typename Storage>
        void replace_window(Storage &window, DateTime modified_time, std::size_t count, FillElementFn fill,
                            void *fill_context)
        {
            if constexpr (std::is_same_v<Storage, SizeTSWindowStorage>)
            {
                if (count > window.capacity())
                {
                    throw std::length_error("TSW fixed window source exceeds the configured period");
                }
            }
            window.begin_replacement(modified_time);
            for (std::size_t index = 0; index < count; ++index)
            {
                Value element{window.element_binding()};
                fill(fill_context, index, window.element_binding(), const_cast<void *>(element.view().data()));
                window.push(element.view(), modified_time);
            }
        }

        template <typename Storage>
        void push_window(Storage &window, DateTime modified_time, const TSDataTracking &tracking, FillElementFn fill, void *fill_context)
        {
            if (tracking.last_modified_time == modified_time)
            {
                const auto &tick = window.tick();
                // A manual expiry may have published earlier in this evaluation.
                // Its owner may still supply the single arrival for the same tick.
                if (!std::is_same_v<Storage, TimeTSWindowStorage> || tick.time != modified_time ||
                    tick.state.has(WindowTickState::Added) || tick.state.has(WindowTickState::Cleared) || tick.state.has(WindowTickState::Replaced) || tick.size() == 0)
                    throw std::logic_error("TSW from_python allows only one window tick per evaluation time");
            }
            Value element{window.element_binding()};
            fill(fill_context, 0, window.element_binding(), const_cast<void *>(element.view().data()));
            window.push(element.view(), modified_time);
            window.track_removals(tracking);
        }
    }  // namespace

    const TSWDataLayout &window_layout(const void *context) noexcept { return *common(context).layout; }

    // The layout lives on the common base, so these read it there: casting a
    // duration-window context to the size-window instantiation (or the other
    // way round) would be a cast between unrelated types.
    const TSDataTracking &window_tracking(const void *context, const void *memory) noexcept
    {
        return *MemoryUtils::cast<TSDataTracking>(static_cast<const std::byte *>(memory) +
                                                  common(context).layout->tracking_offset);
    }

    const void *window_value_memory(const void *context, const void *memory) noexcept
    {
        return static_cast<const std::byte *>(memory) + common(context).layout->value_offset;
    }

    ValueView window_arrival(const void *context, const void *memory, DateTime time)
    {
        return common(context).time_window ? TimeBase::window_arrival_view(context, memory, time)
                                           : SizeBase::window_arrival_view(context, memory, time);
    }

    std::size_t window_storage_size(const void *context, const void *storage) noexcept
    {
        return common(context).time_window ? ts_data_plan_factory_detail::storage<TimeTSWindowStorage>(storage).size()
                                           : ts_data_plan_factory_detail::storage<SizeTSWindowStorage>(storage).size();
    }

    const void *window_storage_element_at(const void *context, const void *storage, std::size_t index) noexcept
    {
        return common(context).time_window ? ts_data_plan_factory_detail::storage<TimeTSWindowStorage>(storage).element_at(index)
                                           : ts_data_plan_factory_detail::storage<SizeTSWindowStorage>(storage).element_at(index);
    }

    void window_replace(const void *context, void *memory, DateTime modified_time, std::size_t count,
                        FillElementFn fill, void *fill_context)
    {
        if (common(context).time_window)
        {
            replace_window(ts_data_plan_factory_detail::storage<TimeTSWindowStorage>(TimeBase::window_mutable_value_memory(context, memory)),
                           modified_time, count, fill, fill_context);
        }
        else
        {
            replace_window(ts_data_plan_factory_detail::storage<SizeTSWindowStorage>(SizeBase::window_mutable_value_memory(context, memory)),
                           modified_time, count, fill, fill_context);
        }
    }

    void window_push(const void *context, void *memory, DateTime modified_time, FillElementFn fill, void *fill_context)
    {
        const auto &tracking = window_tracking(context, memory);
        if (common(context).time_window)
        {
            push_window(ts_data_plan_factory_detail::storage<TimeTSWindowStorage>(TimeBase::window_mutable_value_memory(context, memory)),
                        modified_time, tracking, fill, fill_context);
        }
        else
        {
            push_window(ts_data_plan_factory_detail::storage<SizeTSWindowStorage>(SizeBase::window_mutable_value_memory(context, memory)),
                        modified_time, tracking, fill, fill_context);
        }
    }
}  // namespace hgraph::ts_data_seams
