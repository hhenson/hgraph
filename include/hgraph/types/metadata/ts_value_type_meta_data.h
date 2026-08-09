//
// Created by Howard Henson on 20/04/2026.
//

#ifndef HGRAPH_CPP_ROOT_TS_VALUE_TYPE_META_DATA_H
#define HGRAPH_CPP_ROOT_TS_VALUE_TYPE_META_DATA_H

#include <hgraph/util/date_time.h>
#include <hgraph/types/metadata/type_record.h>
#include <hgraph/types/metadata/value_type_meta_data.h>

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace hgraph
{
    struct TSValueTypeMetaData;

    /**
     * The eight time-series kinds enumerated in the developer guide,
     * named with the canonical short labels users write in code:
     *
     * - ``TS``     — scalar time-series of one atomic type.
     * - ``TSS``    — unordered set of one scalar type.
     * - ``TSD``    — keyed dictionary with TS values.
     * - ``TSL``    — ordered list of one TS type.
     * - ``TSW``    — sliding window of one scalar type.
     * - ``TSB``    — named bundle of TS fields.
     * - ``REF``    — reference to another TS target.
     * - ``SIGNAL`` — zero-payload tick stream.
     */
    enum class TSTypeKind : uint8_t
    {
        TS = 0,
        TSS = 1,
        TSD = 2,
        TSL = 3,
        TSW = 4,
        TSB = 5,
        REF = 6,
        SIGNAL = 7,
    };

    /**
     * Field descriptor for a ``TSB`` bundle.
     *
     * Names live in the registry's interned string storage; ``index`` is
     * the field's position in declaration order.
     */
    struct TSFieldMetaData
    {
        /** Field name; registry-interned. */
        const char *name{nullptr};
        /** Field index in declaration order. */
        size_t index{0};
        /** Schema for this field's time-series type. */
        const TSValueTypeMetaData *type{nullptr};
    };

    /**
     * Schema descriptor for time-series-layer types.
     *
     * The shape of ``data`` depends on ``kind``: ``TSD`` carries a key
     * type and value-TS schema, ``TSL`` carries an element-TS schema and
     * fixed size, ``TSW`` carries tick or duration parameters, ``TSB``
     * carries a field array, ``REF`` carries the referenced TS schema.
     * Use the ``set_*`` methods to populate ``data`` consistently with
     * ``kind``.
     *
     * Always interned through ``TypeRegistry``: equivalent metadata
     * resolves to the same pointer. ``value_type`` records the underlying
     * value-layer schema where one applies (``TS``, ``TSS``, ``TSD`` key,
     * ``TSW``).
     */
    struct TSValueTypeMetaData final
    {
        /** Window-size parameters: tick-based or duration-based. */
        union WindowParams
        {
            /** Tick-count window: ``period`` of ticks, with ``min_period`` warm-up. */
            struct
            {
                size_t period;
                size_t min_period;
            } tick;
            /** Duration window: ``time_range`` evaluation-time delta with ``min_time_range`` warm-up. */
            struct
            {
                TimeDelta time_range;
                TimeDelta min_time_range;
            } duration;

            constexpr WindowParams()
                : tick{0, 0}
            {
            }
        };

        /** Empty payload used for kinds without extra metadata (``TS``, ``TSS``, ``SIGNAL``). */
        struct EmptyData
        {
        };

        /** ``TSD`` payload: key value-schema and per-key TS-schema. */
        struct TsdData
        {
            const ValueTypeMetaData *key_type{nullptr};
            const TSValueTypeMetaData *value_ts{nullptr};
        };

        /** ``TSL`` payload: element TS-schema and fixed size (``0`` for dynamic). */
        struct TslData
        {
            const TSValueTypeMetaData *element_ts{nullptr};
            size_t fixed_size{0};
        };

        /** ``TSW`` payload: a flag plus the tick or duration parameters. */
        struct TswData
        {
            bool is_duration_based{false};
            WindowParams window{};
        };

        /**
         * ``TSB`` payload: field array, count, and the bundle name.
         *
         * ``wrapped_un_named`` points to the structural (un-named) TSB
         * with the same field list when this TSB is *named*. It is null
         * for the un-named TSB itself. Used to distinguish nominal
         * identity (named TSBs with different names but identical
         * fields are separate schemas) from structural identity (the
         * un-named TSB is the canonical structural form, shared by
         * every named TSB that has its field list).
         */
        struct TsbData
        {
            const TSFieldMetaData *fields{nullptr};
            size_t field_count{0};
            const char *bundle_name{nullptr};
            const TSValueTypeMetaData *wrapped_un_named{nullptr};
        };

        /** ``REF`` payload: schema of the referenced time-series. */
        struct RefData
        {
            const TSValueTypeMetaData *referenced_ts{nullptr};
        };

        /** Tagged union for kind-specific metadata; the active member is determined by ``kind``. */
        union KindData
        {
            EmptyData empty;
            TsdData tsd;
            TslData tsl;
            TswData tsw;
            TsbData tsb;
            RefData ref;

            constexpr KindData()
                : empty{}
            {
            }
        };

        /** Default construct an empty descriptor (kind defaults to ``SIGNAL``). */
        constexpr TSValueTypeMetaData() noexcept = default;

        /** Construct with kind, optional underlying value type, and optional name. */
        constexpr TSValueTypeMetaData(TSTypeKind kind_,
                                      const ValueTypeMetaData *value_type_ = nullptr,
                                      const char *label_ = nullptr) noexcept
            : header(TypeFamily::TimeSeries, static_cast<TypeKind>(kind_), label_)
            , kind(kind_)
            , value_type(value_type_)
        {
        }

        /** Common family/kind/label prefix shared by all unified schemas. */
        SchemaHeader header{};
        /** Kind discriminator for ``data``. */
        TSTypeKind kind{TSTypeKind::SIGNAL};
        /**
         * Underlying value-layer schema the metadata was constructed against.
         * This is the value pointer passed to the constructor. For most
         * kinds it is also the consumer-visible current-value schema, but
         * kinds such as ``TSW`` keep scalar element type here while exposing
         * a list-shaped ``value_schema``. Collection schemas compose from
         * child ``value_schema`` values, not this constructor pointer.
         */
        const ValueTypeMetaData *value_type{nullptr};
        /** Kind-dependent payload; populated through the ``set_*`` helpers. */
        KindData data{};
        /**
         * Value-layer schema describing the runtime ``value`` of this
         * time-series. Pre-computed by ``TypeRegistry`` during registration
         * so reading the property is a plain field access.
         *
         * Per-kind mapping:
         *
         * - ``TS<T>``     — ``T``
         * - ``TSS<T>``    — ``Set<T>``
         * - ``TSD<K, V>`` — ``Map<K, V.value_schema>``
         * - ``TSL<T>``    — ``List<T.value_schema, fixed_size>``
         * - ``TSW<T>``    — ``List<T, period>`` (tick) / ``List<T, 0>`` (duration)
         * - ``TSB{f...}`` — ``Bundle{f: f.value_schema...}``
         * - ``REF<T>``    — ``TimeSeriesReference`` (the runtime value of
         *                    a reference is the reference token itself;
         *                    the schema's ``referenced_ts()`` still
         *                    records ``T`` for binding validation, but
         *                    it does not appear in ``value_schema``)
         * - ``SIGNAL``    — ``bool``
         */
        const ValueTypeMetaData *value_schema{nullptr};
        /**
         * Value-layer schema describing the runtime ``delta_value`` of this
         * time-series — the per-tick change set in its kind-specific shape.
         * Pre-computed by ``TypeRegistry``.
         *
         * Per-kind mapping:
         *
         * - ``TS<T>``     — ``T``
         * - ``TSS<T>``    — ``Bundle{added: Set<T>, removed: Set<T>}``
         * - ``TSD<K, V>`` — ``Bundle{removed: Set<K>, modified: Map<K, V.delta>}``
         * - ``TSL<T>``    — ``Map<i64, T.delta_value_schema>``
         * - ``TSW<T>``    — ``T`` (the element added this tick)
         * - ``TSB{f...}`` — ``Bundle{f: f.delta_value_schema...}``
         * - ``REF<T>``    — ``TimeSeriesReference`` (same as
         *                    ``value_schema`` for REFs)
         * - ``SIGNAL``    — ``bool``
         */
        const ValueTypeMetaData *delta_value_schema{nullptr};

        /** Registry-owned canonical diagnostic label. */
        [[nodiscard]] constexpr std::string_view name() const noexcept
        {
            return header.label == nullptr ? std::string_view{} : std::string_view{header.label};
        }

        /** Common schema prefix. */
        [[nodiscard]] constexpr const SchemaHeader &schema_header() const noexcept { return header; }

        /** Populate ``data.tsd`` for a ``TSD`` descriptor. */
        constexpr void set_tsd(const ValueTypeMetaData *key_type, const TSValueTypeMetaData *value_ts) noexcept
        {
            data.tsd = TsdData{key_type, value_ts};
        }

        /** Populate ``data.tsl`` for a ``TSL`` descriptor. */
        constexpr void set_tsl(const TSValueTypeMetaData *element_ts, size_t fixed_size) noexcept
        {
            data.tsl = TslData{element_ts, fixed_size};
        }

        /** Populate ``data.tsw`` for a tick-count ``TSW``. */
        constexpr void set_tsw_tick(size_t period, size_t min_period) noexcept
        {
            data.tsw = TswData{false, WindowParams{}};
            data.tsw.window.tick.period = period;
            data.tsw.window.tick.min_period = min_period;
        }

        /** Populate ``data.tsw`` for a duration-based ``TSW``. */
        constexpr void set_tsw_duration(TimeDelta time_range,
                                           TimeDelta min_time_range) noexcept
        {
            data.tsw = TswData{true, WindowParams{}};
            data.tsw.window.duration.time_range = time_range;
            data.tsw.window.duration.min_time_range = min_time_range;
        }

        /** Populate ``data.tsb`` for a ``TSB`` descriptor. ``wrapped_un_named`` is null on the un-named form. */
        constexpr void set_tsb(const TSFieldMetaData *fields,
                               size_t field_count,
                               const char *bundle_name,
                               const TSValueTypeMetaData *wrapped_un_named = nullptr) noexcept
        {
            data.tsb = TsbData{fields, field_count, bundle_name, wrapped_un_named};
        }

        /** Populate ``data.ref`` for a ``REF`` descriptor. */
        constexpr void set_ref(const TSValueTypeMetaData *referenced_ts) noexcept
        {
            data.ref = RefData{referenced_ts};
        }

        /** Key value-schema for ``TSD``; null for other kinds. */
        [[nodiscard]] constexpr const ValueTypeMetaData *key_type() const noexcept
        {
            return kind == TSTypeKind::TSD ? data.tsd.key_type : nullptr;
        }

        /** Element TS-schema for ``TSD`` (value-TS), ``TSL``, or ``REF`` (target). */
        [[nodiscard]] constexpr const TSValueTypeMetaData *element_ts() const noexcept
        {
            switch (kind)
            {
                case TSTypeKind::TSD: return data.tsd.value_ts;
                case TSTypeKind::TSL: return data.tsl.element_ts;
                case TSTypeKind::REF: return data.ref.referenced_ts;
                default: return nullptr;
            }
        }

        /** Fixed size of a static ``TSL``; zero for dynamic or non-list kinds. */
        [[nodiscard]] constexpr size_t fixed_size() const noexcept
        {
            return kind == TSTypeKind::TSL ? data.tsl.fixed_size : 0;
        }

        /** True when ``TSW`` is duration-based; false for tick-based or non-window. */
        [[nodiscard]] constexpr bool is_duration_based() const noexcept
        {
            return kind == TSTypeKind::TSW && data.tsw.is_duration_based;
        }

        /** Tick-count window period; zero for duration-based or non-window kinds. */
        [[nodiscard]] constexpr size_t period() const noexcept
        {
            return kind == TSTypeKind::TSW && !data.tsw.is_duration_based ? data.tsw.window.tick.period : 0;
        }

        /** Tick-count window warm-up size; zero for duration-based or non-window kinds. */
        [[nodiscard]] constexpr size_t min_period() const noexcept
        {
            return kind == TSTypeKind::TSW && !data.tsw.is_duration_based
                       ? data.tsw.window.tick.min_period
                       : 0;
        }

        /** Duration-window time range; zero for tick-based or non-window kinds. */
        [[nodiscard]] constexpr TimeDelta time_range() const noexcept
        {
            return kind == TSTypeKind::TSW && data.tsw.is_duration_based
                       ? data.tsw.window.duration.time_range
                       : TimeDelta{0};
        }

        /** Duration-window warm-up; zero for tick-based or non-window kinds. */
        [[nodiscard]] constexpr TimeDelta min_time_range() const noexcept
        {
            return kind == TSTypeKind::TSW && data.tsw.is_duration_based
                       ? data.tsw.window.duration.min_time_range
                       : TimeDelta{0};
        }

        /** Field array for ``TSB``; null for other kinds. */
        [[nodiscard]] constexpr const TSFieldMetaData *fields() const noexcept
        {
            return kind == TSTypeKind::TSB ? data.tsb.fields : nullptr;
        }

        /** Field count for ``TSB``; zero for other kinds. */
        [[nodiscard]] constexpr size_t field_count() const noexcept
        {
            return kind == TSTypeKind::TSB ? data.tsb.field_count : 0;
        }

        /** Bundle display name for ``TSB``; null for other kinds. */
        [[nodiscard]] constexpr const char *bundle_name() const noexcept
        {
            return kind == TSTypeKind::TSB ? data.tsb.bundle_name : nullptr;
        }

        /** For a *named* ``TSB``, the structural (un-named) twin; null on the un-named TSB itself or for non-TSB kinds. */
        [[nodiscard]] constexpr const TSValueTypeMetaData *wrapped_un_named_tsb() const noexcept
        {
            return kind == TSTypeKind::TSB ? data.tsb.wrapped_un_named : nullptr;
        }

        /** True when this TS metadata is a named TSB (carries a name). */
        [[nodiscard]] constexpr bool is_named_tsb() const noexcept
        {
            return kind == TSTypeKind::TSB && data.tsb.wrapped_un_named != nullptr;
        }
        /** True when this TS metadata is a structural (un-named) TSB. */
        [[nodiscard]] constexpr bool is_un_named_tsb() const noexcept
        {
            return kind == TSTypeKind::TSB && data.tsb.wrapped_un_named == nullptr;
        }

        /** Schema referenced by a ``REF``; null for other kinds. */
        [[nodiscard]] constexpr const TSValueTypeMetaData *referenced_ts() const noexcept
        {
            return kind == TSTypeKind::REF ? data.ref.referenced_ts : nullptr;
        }

        /** True for the keyed/structural kinds (``TSS``, ``TSD``, ``TSL``, ``TSB``). */
        [[nodiscard]] constexpr bool is_collection() const noexcept
        {
            return kind == TSTypeKind::TSS || kind == TSTypeKind::TSD || kind == TSTypeKind::TSL ||
                   kind == TSTypeKind::TSB;
        }

        /** True for kinds whose payload is a single scalar TS (``TS``, ``TSW``, ``SIGNAL``). */
        [[nodiscard]] constexpr bool is_scalar_ts() const noexcept
        {
            return kind == TSTypeKind::TS || kind == TSTypeKind::TSW || kind == TSTypeKind::SIGNAL;
        }
    };
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_TS_VALUE_TYPE_META_DATA_H
