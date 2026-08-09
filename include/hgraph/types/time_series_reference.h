#ifndef HGRAPH_CPP_ROOT_TIME_SERIES_REFERENCE_H
#define HGRAPH_CPP_ROOT_TIME_SERIES_REFERENCE_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/time_series/ts_output/base_view.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <stdexcept>
#include <string>
#include <vector>

namespace hgraph
{
    namespace detail
    {
        class TSOutputAlternativeStore;
    }

    class TSInputView;
    class TSOutputView;
    struct TSValueTypeMetaData;

    /**
     * Runtime value of a REF time-series.
     *
     * A ``TimeSeriesReference`` carries the binding state needed to point
     * at (or compose references to) another time-series. It is the C++
     * value type that backs the ``TimeSeriesReference`` atomic schema in
     * the type registry, so a value of any ``REF`` schema holds an instance
     * of this struct.
     *
     * Three kinds are supported:
     *
     * - **EMPTY** — unbound; the reference points at nothing yet. Still
     *   carries a target schema where one is known, so a later binding
     *   attempt can be validated.
     * - **PEERED** — directly bound to a single output endpoint. The
     *   reference stores the output handle without an evaluation time.
     * - **NON_PEERED** — composite reference whose target is itself a
     *   composite time-series (``REF<TSL<T>>``, ``REF<TSB<...>>``, etc.).
     *   Holds a vector of sub-references, one per structural slot of the
     *   composite.
     *
     * The ``target_schema`` pointer records the TS schema (the ``T`` in
     * the surrounding ``REF<T>``) the reference is intended to bind to.
     * It is the basis for binding-time validation: a candidate output's
     * schema must match (or be dereference-compatible with)
     * ``target_schema`` for the binding to succeed.
     */
    struct HGRAPH_EXPORT TimeSeriesReference
    {
        /** The reference's kind discriminator. */
        enum class Kind : uint8_t
        {
            EMPTY      = 0,
            PEERED     = 1,
            NON_PEERED = 2,
        };

        /** Default-construct an EMPTY reference with no target schema. */
        TimeSeriesReference() noexcept;

        /** Construct an EMPTY reference that records its expected target schema. */
        explicit TimeSeriesReference(const TSValueTypeMetaData *target_schema) noexcept;

        /** Construct a PEERED reference from an output endpoint handle. */
        explicit TimeSeriesReference(TSOutputHandle target);

        /** Construct a PEERED reference from an output view. */
        explicit TimeSeriesReference(const TSOutputView &target);

        /**
         * Construct a reference from an input view.
         *
         * Target-link input views produce a PEERED reference when bound and a
         * typed EMPTY reference when unbound. Non-peered structural prefixes
         * produce NON_PEERED references by recursively collecting child
         * references. Leaf shapes reached without a target link produce typed
         * EMPTY references.
         */
        explicit TimeSeriesReference(const TSInputView &source);

        /**
         * Construct a NON_PEERED composite reference. ``target_schema``
         * describes the composite TS kind (typically ``TSL`` or ``TSB``);
         * ``items`` holds one sub-reference per structural slot of the
         * composite.
         */
        TimeSeriesReference(const TSValueTypeMetaData *target_schema, std::vector<TimeSeriesReference> items);

        TimeSeriesReference(const TimeSeriesReference &other);
        TimeSeriesReference &operator=(const TimeSeriesReference &other);
        TimeSeriesReference(TimeSeriesReference &&other) noexcept;
        TimeSeriesReference &operator=(TimeSeriesReference &&other) noexcept;
        ~TimeSeriesReference() noexcept;

        /** Build an EMPTY reference for ``target_schema``. */
        [[nodiscard]] static TimeSeriesReference empty(const TSValueTypeMetaData *target_schema = nullptr) noexcept;

        /** Build a PEERED reference from an output endpoint handle. */
        [[nodiscard]] static TimeSeriesReference peered(TSOutputHandle target);

        /** Build a PEERED reference from an output view. */
        [[nodiscard]] static TimeSeriesReference peered(const TSOutputView &target);

        /** Build a NON_PEERED composite reference from already-created child references. */
        [[nodiscard]] static TimeSeriesReference non_peered(const TSValueTypeMetaData *target_schema,
                                                            std::vector<TimeSeriesReference> items);

        /** Discriminator: EMPTY / PEERED / NON_PEERED. */
        [[nodiscard]] Kind kind() const noexcept { return kind_; }
        /** True when ``kind() == Kind::EMPTY``. */
        [[nodiscard]] bool is_empty() const noexcept { return kind_ == Kind::EMPTY; }
        /** True when ``kind() == Kind::PEERED``. */
        [[nodiscard]] bool is_peered() const noexcept { return kind_ == Kind::PEERED; }
        /** True when ``kind() == Kind::NON_PEERED``. */
        [[nodiscard]] bool is_non_peered() const noexcept { return kind_ == Kind::NON_PEERED; }
        /** True when this PEERED reference carries a bound output handle. */
        [[nodiscard]] bool has_output() const noexcept;
        /** True when the referenced output is currently valid, or a composite reference has at least one item. */
        [[nodiscard]] bool is_valid(DateTime evaluation_time) const;

        /**
         * Schema describing the TS type this reference is pointed at — the
         * ``T`` in the surrounding ``REF<T>``. May be null when the
         * reference is unconstrained.
         */
        [[nodiscard]] const TSValueTypeMetaData *target_schema() const noexcept { return target_schema_; }

        /** Sub-references for a NON_PEERED reference. Throws otherwise. */
        [[nodiscard]] const std::vector<TimeSeriesReference> &items() const;
        /** Indexed sub-reference. Throws if not NON_PEERED or if ``index`` is out of range. */
        [[nodiscard]] const TimeSeriesReference &operator[](size_t index) const;

        /**
         * Return the same reference with a different declared target schema.
         * The endpoint or composite items are preserved and no compatibility
         * check is performed. This is the explicit unsafe primitive behind
         * ``downcast_ref``; normal binding code must not use it to bypass
         * schema validation.
         */
        [[nodiscard]] TimeSeriesReference with_target_schema_unchecked(
            const TSValueTypeMetaData *target_schema) const;

        /**
         * Two references compare equal when they share kind, target schema,
         * and (for NON_PEERED) sub-references.
         */
        [[nodiscard]] bool        operator==(const TimeSeriesReference &other) const noexcept;
        /** Hash compatible with ``std::hash<TimeSeriesReference>``. */
        [[nodiscard]] std::size_t hash() const noexcept;
        /** Human-readable representation; primarily for diagnostics. */
        [[nodiscard]] std::string to_string() const;

        /** Singleton empty reference with no target schema. */
        [[nodiscard]] static const TimeSeriesReference &empty_reference() noexcept;

        /**
         * The referenced output handle of a PEERED reference (throws
         * otherwise). RUNTIME PLUMBING ONLY — subscription and alternative
         * binding (race/reduce-with-race pending wake-ups); reference values
         * stay opaque at the API surfaces (no dereferencing from user code).
         */
        [[nodiscard]] const TSOutputHandle &target_output() const;

      private:
        friend class detail::TSOutputAlternativeStore;

        union Storage
        {
            TSOutputHandle target;
            std::vector<TimeSeriesReference> items;

            constexpr Storage() noexcept {}
            ~Storage() noexcept {}
        };

        void destroy() noexcept;
        void copy_from(const TimeSeriesReference &other);
        void move_from(TimeSeriesReference &&other) noexcept;
        [[nodiscard]] static TimeSeriesReference peered_as(const TSValueTypeMetaData *target_schema,
                                                           TSOutputHandle target);

        Kind                             kind_{Kind::EMPTY};
        const TSValueTypeMetaData       *target_schema_{nullptr};
        Storage                          storage_{};
    };
}  // namespace hgraph

namespace std
{
    /** Hash specialization so ``TimeSeriesReference`` works in unordered containers. */
    template <> struct hash<hgraph::TimeSeriesReference>
    {
        std::size_t operator()(const hgraph::TimeSeriesReference &ref) const noexcept { return ref.hash(); }
    };
}  // namespace std

#if HGRAPH_ENABLE_PYTHON_USER_NODES
#include <hgraph/types/value/value_ops.h>

#include <nanobind/nanobind.h>

namespace hgraph
{
    /**
     * TimeSeriesReference <-> Python conversion binds onto the atomic value
     * ops. The Python module owns the opaque wrapper and installs these hooks
     * during module initialization; the C++ runtime remains Python-free when
     * user-node support is disabled.
     */
    template <>
    struct python_conversion_traits<TimeSeriesReference>
    {
        using ToPythonHook   = nanobind::object (*)(const TimeSeriesReference &);
        using FromPythonHook = TimeSeriesReference (*)(nanobind::handle);

        [[nodiscard]] HGRAPH_EXPORT static ToPythonHook &to_python_hook() noexcept;
        [[nodiscard]] HGRAPH_EXPORT static FromPythonHook &from_python_hook() noexcept;
        HGRAPH_EXPORT static nanobind::object to_python(const TimeSeriesReference &value);
        HGRAPH_EXPORT static TimeSeriesReference from_python(nanobind::handle source);
    };
}  // namespace hgraph
#endif  // HGRAPH_ENABLE_PYTHON_USER_NODES

#endif  // HGRAPH_CPP_ROOT_TIME_SERIES_REFERENCE_H
