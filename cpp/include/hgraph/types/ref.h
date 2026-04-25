#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/ts_input_view.h>
#include <hgraph/types/time_series/ts_output_view.h>

#include <compare>
#include <memory>
#include <string>
#include <type_traits>
#include <variant>
#include <vector>

namespace hgraph
{
    struct TimeSeriesReference;

    // Reverse-subscription token: each PEERED TimeSeriesReference owns one
    // and registers it with its target BaseState. When that state is
    // destroyed, the destructor invokes `invalidate_ref()` (free function in
    // the impl translation unit) which flips the owning ref to EMPTY so any
    // subsequent target_view()/is_peered()/is_valid() call sees a safe
    // EMPTY ref instead of a dangling LinkedTSContext.
    //
    // Stored by unique_ptr in the ref so its address is stable for the
    // duration of the registration.
    struct HGRAPH_EXPORT ReferenceInvalidator
    {
        TimeSeriesReference *owner{nullptr};
        // Tracks whether the target state's destructor has fired. Once true
        // the owning ref must NOT call target_state->unregister_ref_invalidator
        // because the state's memory is gone.
        bool target_destroyed{false};
    };

    struct HGRAPH_EXPORT TimeSeriesReference
    {
        enum class Kind : uint8_t { EMPTY = 0, PEERED = 1, NON_PEERED = 2 };

        TimeSeriesReference() noexcept = default;
        explicit TimeSeriesReference(LinkedTSContext target) noexcept;
        explicit TimeSeriesReference(std::vector<TimeSeriesReference> items);

        // Explicit special members because the invalidator (if PEERED) holds
        // a pointer back to `this` and must be reseated on copy/move/destroy.
        TimeSeriesReference(const TimeSeriesReference &other);
        TimeSeriesReference(TimeSeriesReference &&other) noexcept;
        TimeSeriesReference &operator=(const TimeSeriesReference &other);
        TimeSeriesReference &operator=(TimeSeriesReference &&other) noexcept;
        ~TimeSeriesReference() noexcept;

        [[nodiscard]] Kind kind() const noexcept { return m_kind; }
        [[nodiscard]] bool is_empty() const noexcept { return m_kind == Kind::EMPTY; }
        [[nodiscard]] bool is_peered() const noexcept { return m_kind == Kind::PEERED; }
        [[nodiscard]] bool is_non_peered() const noexcept { return m_kind == Kind::NON_PEERED; }
        [[nodiscard]] bool is_valid() const;
        [[nodiscard]] engine_time_t observed_time() const noexcept { return m_observed_time; }

        [[nodiscard]] const LinkedTSContext &target() const;
        [[nodiscard]] TSOutputView target_view(engine_time_t evaluation_time = MIN_DT) const;
        [[nodiscard]] const std::vector<TimeSeriesReference> &items() const;
        [[nodiscard]] const TimeSeriesReference &operator[](size_t ndx) const;

        [[nodiscard]] bool operator==(const TimeSeriesReference &other) const noexcept;
        [[nodiscard]] std::string to_string() const;

        static TimeSeriesReference make();
        static TimeSeriesReference make(const TSOutputView &output);
        static TimeSeriesReference make(const TSInputView &input);
        static TimeSeriesReference make(std::vector<TimeSeriesReference> items);

        static const TimeSeriesReference &empty();

      private:
        Kind m_kind{Kind::EMPTY};
        engine_time_t m_observed_time{MIN_DT};
        std::variant<std::monostate, LinkedTSContext, std::vector<TimeSeriesReference>> m_storage;
        // Only set when m_kind == PEERED and target.ts_state is non-null.
        std::unique_ptr<ReferenceInvalidator> m_invalidator;

        // Subscribe / unsubscribe the invalidator with target.ts_state.
        // No-op when target's ts_state is null or `this` is not PEERED.
        void register_invalidator() noexcept;
        void unregister_invalidator() noexcept;

        friend HGRAPH_EXPORT void invalidate_ref(ReferenceInvalidator &) noexcept;
    };

    // Called by ~BaseState() on each registered ReferenceInvalidator. Flips
    // the owning ref to EMPTY in place so dangling LinkedTSContexts cannot
    // be dereferenced after the target state's memory is reclaimed.
    HGRAPH_EXPORT void invalidate_ref(ReferenceInvalidator &) noexcept;

    [[nodiscard]] HGRAPH_EXPORT size_t atomic_hash(const TimeSeriesReference &value);
    [[nodiscard]] HGRAPH_EXPORT TimeSeriesReference atomic_default_value(std::type_identity<TimeSeriesReference>);
    [[nodiscard]] HGRAPH_EXPORT std::partial_ordering atomic_compare(const TimeSeriesReference &lhs,
                                                                     const TimeSeriesReference &rhs);
    [[nodiscard]] HGRAPH_EXPORT std::string to_string(const TimeSeriesReference &value);
}  // namespace hgraph
