#include <hgraph/manifest/schema_descriptor.h>

#include <hgraph/types/metadata/ts_value_type_meta_data.h>
#include <hgraph/types/metadata/value_type_meta_data.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/temporal.h>
#include <hgraph/types/value/specialized_views.h>
#include <hgraph/types/value/visitor.h>
#include <hgraph/types/wired_fn.h>
#include <hgraph/util/date_time.h>

#include <fmt/format.h>

#include <algorithm>

namespace hgraph::manifest
{
    namespace
    {
        // Stable wire enumeration for atomic scalar identity. The numeric
        // values are part of the canonical format — append-only.
        enum class WireAtomic : std::uint8_t
        {
            Unknown = 0,
            Bool = 1,
            Int = 2,
            Float = 3,
            Str = 4,
            Date = 5,
            DateTime = 6,
            TimeDelta = 7,
            Time = 8,
            CivilDateTime = 9,
            Period = 10,
            ZoneId = 11,
            ZonedDateTime = 12,
            InstantRange = 13,
            CivilDateRange = 14,
            InstantRangeSet = 15,
            CivilDateRangeSet = 16,
        };

        WireAtomic wire_atomic_for(const ValueTypeMetaData *meta)
        {
            if (meta == scalar_descriptor<Bool>::value_meta()) { return WireAtomic::Bool; }
            if (meta == scalar_descriptor<Int>::value_meta()) { return WireAtomic::Int; }
            if (meta == scalar_descriptor<Float>::value_meta()) { return WireAtomic::Float; }
            if (meta == scalar_descriptor<Str>::value_meta()) { return WireAtomic::Str; }
            if (meta == scalar_descriptor<Date>::value_meta()) { return WireAtomic::Date; }
            if (meta == scalar_descriptor<DateTime>::value_meta()) { return WireAtomic::DateTime; }
            if (meta == scalar_descriptor<TimeDelta>::value_meta()) { return WireAtomic::TimeDelta; }
            if (meta == scalar_descriptor<Time>::value_meta()) { return WireAtomic::Time; }
            if (meta == scalar_descriptor<CivilDateTime>::value_meta())
            {
                return WireAtomic::CivilDateTime;
            }
            if (meta == scalar_descriptor<Period>::value_meta()) { return WireAtomic::Period; }
            if (meta == scalar_descriptor<ZoneId>::value_meta()) { return WireAtomic::ZoneId; }
            if (meta == scalar_descriptor<ZonedDateTime>::value_meta())
            {
                return WireAtomic::ZonedDateTime;
            }
            if (meta == scalar_descriptor<InstantRange>::value_meta())
            {
                return WireAtomic::InstantRange;
            }
            if (meta == scalar_descriptor<CivilDateRange>::value_meta())
            {
                return WireAtomic::CivilDateRange;
            }
            if (meta == scalar_descriptor<InstantRangeSet>::value_meta())
            {
                return WireAtomic::InstantRangeSet;
            }
            if (meta == scalar_descriptor<CivilDateRangeSet>::value_meta())
            {
                return WireAtomic::CivilDateRangeSet;
            }
            return WireAtomic::Unknown;
        }

        // Descriptor field tags (value scope).
        constexpr std::uint32_t k_tag_kind = 1;
        constexpr std::uint32_t k_tag_flags = 2;
        constexpr std::uint32_t k_tag_atomic = 3;
        constexpr std::uint32_t k_tag_atomic_name = 4;  // registered scalar without a wire tag
        constexpr std::uint32_t k_tag_element = 5;
        constexpr std::uint32_t k_tag_key = 6;
        constexpr std::uint32_t k_tag_fixed_size = 7;
        constexpr std::uint32_t k_tag_fields = 8;
        constexpr std::uint32_t k_tag_nominal = 9;

        // Nominal (named-bundle) scope tags.
        constexpr std::uint32_t k_tag_namespace = 1;
        constexpr std::uint32_t k_tag_local_name = 2;
        constexpr std::uint32_t k_tag_generic_arguments = 3;
        constexpr std::uint32_t k_tag_abstract = 4;
        constexpr std::uint32_t k_tag_discriminator = 5;
        constexpr std::uint32_t k_tag_discriminator_value = 6;

        // Field scope tags.
        constexpr std::uint32_t k_tag_field_name = 1;
        constexpr std::uint32_t k_tag_field_enum_value = 2;
        constexpr std::uint32_t k_tag_field_type = 3;

        // Time-series scope tags.
        constexpr std::uint32_t k_ts_tag_kind = 1;
        constexpr std::uint32_t k_ts_tag_value = 2;
        constexpr std::uint32_t k_ts_tag_element = 3;
        constexpr std::uint32_t k_ts_tag_key = 4;
        constexpr std::uint32_t k_ts_tag_fixed_size = 5;
        constexpr std::uint32_t k_ts_tag_fields = 6;
        constexpr std::uint32_t k_ts_tag_bundle_name = 7;
        constexpr std::uint32_t k_ts_tag_window_duration = 8;
        constexpr std::uint32_t k_ts_tag_window_a = 9;
        constexpr std::uint32_t k_ts_tag_window_b = 10;

        void append_string(CanonicalWriter &writer, std::uint32_t tag, const char *text)
        {
            writer.tag(tag);
            writer.string_field(text == nullptr ? std::string_view{} : std::string_view{text});
        }
    }  // namespace

    void append_value_descriptor(CanonicalWriter &writer, const ValueTypeMetaData *meta)
    {
        CanonicalWriter scope;
        if (meta == nullptr)
        {
            writer.scope(scope);  // empty scope = absent schema
            return;
        }

        const auto kind = meta->value_kind();
        scope.tag(k_tag_kind);
        scope.varint(static_cast<std::uint64_t>(kind));
        scope.tag(k_tag_flags);
        scope.varint(static_cast<std::uint64_t>(meta->flags));

        if (kind == ValueTypeKind::Atomic)
        {
            const auto atomic = wire_atomic_for(meta);
            scope.tag(k_tag_atomic);
            scope.varint(static_cast<std::uint64_t>(atomic));
            if (atomic == WireAtomic::Unknown)
            {
                // Registered (extension/python) scalar: nominal identity by
                // registered name, following the named-bundle rule.
                append_string(scope, k_tag_atomic_name, meta->header.label);
            }
            if (has_flag(meta->flags, ValueTypeFlags::Enum) && meta->fields != nullptr)
            {
                scope.tag(k_tag_fields);
                scope.varint(meta->field_count);
                for (std::size_t i = 0; i < meta->field_count; ++i)
                {
                    const auto &field = meta->fields[i];
                    CanonicalWriter field_scope;
                    append_string(field_scope, k_tag_field_name, field.name);
                    field_scope.tag(k_tag_field_enum_value);
                    field_scope.svarint(field.enum_value);
                    scope.scope(field_scope);
                }
            }
        }

        if (meta->element_type != nullptr)
        {
            scope.tag(k_tag_element);
            append_value_descriptor(scope, meta->element_type);
        }
        if (meta->key_type != nullptr)
        {
            scope.tag(k_tag_key);
            append_value_descriptor(scope, meta->key_type);
        }
        if (meta->fixed_size != 0)
        {
            scope.tag(k_tag_fixed_size);
            scope.varint(meta->fixed_size);
        }
        if (kind != ValueTypeKind::Atomic && meta->fields != nullptr && meta->field_count != 0)
        {
            scope.tag(k_tag_fields);
            scope.varint(meta->field_count);
            for (std::size_t i = 0; i < meta->field_count; ++i)
            {
                const auto &field = meta->fields[i];
                CanonicalWriter field_scope;
                if (field.name != nullptr)
                {
                    append_string(field_scope, k_tag_field_name, field.name);
                }
                field_scope.tag(k_tag_field_type);
                append_value_descriptor(field_scope, field.type);
                scope.scope(field_scope);
            }
        }
        if (meta->is_named_bundle() && meta->bundle_hierarchy != nullptr)
        {
            const auto &nominal = *meta->bundle_hierarchy;
            CanonicalWriter nominal_scope;
            append_string(nominal_scope, k_tag_namespace, nominal.namespace_name);
            append_string(nominal_scope, k_tag_local_name, nominal.local_name);
            if (!nominal.generic_arguments.empty())
            {
                nominal_scope.tag(k_tag_generic_arguments);
                nominal_scope.varint(nominal.generic_arguments.size());
                for (const auto *argument : nominal.generic_arguments)
                {
                    append_value_descriptor(nominal_scope, argument);
                }
            }
            if (nominal.is_abstract)
            {
                nominal_scope.tag(k_tag_abstract);
                nominal_scope.varint(1);
            }
            append_string(nominal_scope, k_tag_discriminator, nominal.discriminator);
            if (nominal.discriminator_value != nullptr)
            {
                append_string(nominal_scope, k_tag_discriminator_value, nominal.discriminator_value);
            }
            scope.tag(k_tag_nominal);
            scope.scope(nominal_scope);
        }

        writer.scope(scope);
    }

    void append_ts_descriptor(CanonicalWriter &writer, const TSValueTypeMetaData *meta)
    {
        CanonicalWriter scope;
        if (meta == nullptr)
        {
            writer.scope(scope);
            return;
        }

        scope.tag(k_ts_tag_kind);
        scope.varint(static_cast<std::uint64_t>(meta->kind));

        switch (meta->kind)
        {
        case TSTypeKind::TS:
        case TSTypeKind::TSS:
            scope.tag(k_ts_tag_value);
            append_value_descriptor(scope, meta->value_type);
            break;
        case TSTypeKind::TSD:
            scope.tag(k_ts_tag_key);
            append_value_descriptor(scope, meta->data.tsd.key_type);
            scope.tag(k_ts_tag_element);
            append_ts_descriptor(scope, meta->data.tsd.value_ts);
            break;
        case TSTypeKind::TSL:
            scope.tag(k_ts_tag_element);
            append_ts_descriptor(scope, meta->data.tsl.element_ts);
            if (meta->data.tsl.fixed_size != 0)
            {
                scope.tag(k_ts_tag_fixed_size);
                scope.varint(meta->data.tsl.fixed_size);
            }
            break;
        case TSTypeKind::TSW: {
            scope.tag(k_ts_tag_value);
            append_value_descriptor(scope, meta->value_type);
            const auto &window = meta->data.tsw;
            scope.tag(k_ts_tag_window_duration);
            scope.varint(window.is_duration_based ? 1u : 0u);
            scope.tag(k_ts_tag_window_a);
            if (window.is_duration_based)
            {
                scope.svarint(window.window.duration.time_range.count());
            }
            else
            {
                scope.varint(window.window.tick.period);
            }
            scope.tag(k_ts_tag_window_b);
            if (window.is_duration_based)
            {
                scope.svarint(window.window.duration.min_time_range.count());
            }
            else
            {
                scope.varint(window.window.tick.min_period);
            }
            break;
        }
        case TSTypeKind::TSB: {
            const auto &bundle = meta->data.tsb;
            if (bundle.bundle_name != nullptr)
            {
                append_string(scope, k_ts_tag_bundle_name, bundle.bundle_name);
            }
            scope.tag(k_ts_tag_fields);
            scope.varint(bundle.field_count);
            for (std::size_t i = 0; i < bundle.field_count; ++i)
            {
                const auto &field = bundle.fields[i];
                CanonicalWriter field_scope;
                append_string(field_scope, k_tag_field_name, field.name);
                field_scope.tag(k_tag_field_type);
                append_ts_descriptor(field_scope, field.type);
                scope.scope(field_scope);
            }
            break;
        }
        case TSTypeKind::REF:
            scope.tag(k_ts_tag_element);
            append_ts_descriptor(scope, meta->data.ref.referenced_ts);
            break;
        case TSTypeKind::SIGNAL:
            break;
        }

        writer.scope(scope);
    }

    std::vector<std::byte> value_descriptor(const ValueTypeMetaData *meta)
    {
        CanonicalWriter writer;
        append_value_descriptor(writer, meta);
        return writer.take();
    }

    std::vector<std::byte> ts_descriptor(const TSValueTypeMetaData *meta)
    {
        CanonicalWriter writer;
        append_ts_descriptor(writer, meta);
        return writer.take();
    }

    bool manifest_scalar_encodable(const ValueTypeMetaData *meta, std::string *reason)
    {
        if (meta == nullptr)
        {
            if (reason != nullptr) { *reason = "value has no schema"; }
            return false;
        }
        const auto kind = meta->try_value_kind();
        if (!kind.has_value())
        {
            if (reason != nullptr) { *reason = "value kind is not a manifest value kind"; }
            return false;
        }
        switch (*kind)
        {
        case ValueTypeKind::Atomic: {
            if (has_flag(meta->flags, ValueTypeFlags::Enum)) { return true; }
            if (wire_atomic_for(meta) != WireAtomic::Unknown) { return true; }
            // Wired callables encode as registered identity; anonymous ones
            // are refused per VALUE at encode time (the schema alone cannot
            // tell).
            if (meta == scalar_descriptor<WiredFn>::value_meta()) { return true; }
            if (reason != nullptr)
            {
                *reason = fmt::format(
                    "scalar type '{}' has no canonical manifest encoding",
                    meta->name());
            }
            return false;
        }
        case ValueTypeKind::Tuple:
        case ValueTypeKind::Bundle: {
            for (std::size_t i = 0; i < meta->field_count; ++i)
            {
                if (!manifest_scalar_encodable(meta->fields[i].type, reason)) { return false; }
            }
            return true;
        }
        case ValueTypeKind::List:
        case ValueTypeKind::Set:
            return manifest_scalar_encodable(meta->element_type, reason);
        case ValueTypeKind::Map:
            return manifest_scalar_encodable(meta->key_type, reason) &&
                   manifest_scalar_encodable(meta->element_type, reason);
        case ValueTypeKind::CyclicBuffer:
        case ValueTypeKind::Queue:
        case ValueTypeKind::Any:
            if (reason != nullptr)
            {
                *reason = fmt::format(
                    "value kind of '{}' has no canonical manifest encoding (runtime container)",
                    meta->name());
            }
            return false;
        }
        return false;
    }

    namespace
    {
        void encode_atomic(CanonicalWriter &writer, const ValueView &value,
                           const ValueTypeMetaData *meta)
        {
            if (has_flag(meta->flags, ValueTypeFlags::Enum))
            {
                writer.svarint(value.checked_as<Int>());
                return;
            }
            if (meta == scalar_descriptor<WiredFn>::value_meta())
            {
                // A wired callable is an implementation reference, not a
                // value: its manifest form is the REGISTERED identity it was
                // derived from. An anonymous callable has none and makes the
                // graph non-manifestable (RFC 0022: core does not infer
                // identity from a function address or RTTI name); NodeManifestOps
                // (stage 2) supersedes this with authored descriptors.
                const auto &fn = value.checked_as<WiredFn>();
                if (fn.lifted != nullptr && fn.lifted->name != nullptr)
                {
                    writer.varint(1);
                    writer.string_field(fn.lifted->name);
                    return;
                }
                if (!fn.operator_name.empty())
                {
                    writer.varint(2);
                    writer.string_field(fn.operator_name);
                    return;
                }
                throw UnsupportedManifestValue(
                    "anonymous wired callable has no registered implementation identity");
            }
            switch (wire_atomic_for(meta))
            {
            case WireAtomic::Bool: writer.varint(value.checked_as<Bool>() ? 1u : 0u); return;
            case WireAtomic::Int: writer.svarint(value.checked_as<Int>()); return;
            case WireAtomic::Float: writer.fixed_double(value.checked_as<Float>()); return;
            case WireAtomic::Str: writer.string_field(value.checked_as<Str>()); return;
            case WireAtomic::Date: {
                const auto date = value.checked_as<Date>();
                writer.svarint(static_cast<std::int64_t>(
                    std::chrono::sys_days{date}.time_since_epoch().count()));
                return;
            }
            case WireAtomic::DateTime:
                writer.svarint(value.checked_as<DateTime>().time_since_epoch().count());
                return;
            case WireAtomic::TimeDelta:
                writer.svarint(value.checked_as<TimeDelta>().count());
                return;
            case WireAtomic::Time:
                writer.svarint(value.checked_as<Time>().microseconds);
                return;
            default:
                throw UnsupportedManifestValue(fmt::format(
                    "scalar type '{}' has no canonical manifest encoding", meta->name()));
            }
        }
    }  // namespace

    void encode_manifest_scalar(CanonicalWriter &writer, const ValueView &value)
    {
        visit(
            value,
            [&](const AtomicView &atomic) { encode_atomic(writer, atomic, atomic.schema()); },
            [&](const TupleView &tuple) {
                writer.varint(tuple.size());
                for (std::size_t i = 0; i < tuple.size(); ++i)
                {
                    encode_manifest_scalar(writer, tuple.at(i));
                }
            },
            [&](const BundleView &bundle) {
                writer.varint(bundle.size());
                for (std::size_t i = 0; i < bundle.size(); ++i)
                {
                    encode_manifest_scalar(writer, bundle.at(i));
                }
            },
            [&](const ListView &list) {
                writer.varint(list.size());
                for (std::size_t i = 0; i < list.size(); ++i)
                {
                    encode_manifest_scalar(writer, list.at(i));
                }
            },
            [&](const SetView &set) {
                std::vector<std::vector<std::byte>> encoded;
                encoded.reserve(set.size());
                for (const auto element : set.elements())
                {
                    CanonicalWriter element_writer;
                    encode_manifest_scalar(element_writer, element);
                    encoded.push_back(element_writer.take());
                }
                std::sort(encoded.begin(), encoded.end());
                writer.varint(encoded.size());
                for (const auto &bytes : encoded) { writer.bytes_field(bytes); }
            },
            [&](const MapView &map) {
                std::vector<std::pair<std::vector<std::byte>, std::vector<std::byte>>> encoded;
                encoded.reserve(map.size());
                for (const auto &[key, mapped] : map.entries())
                {
                    CanonicalWriter key_writer;
                    encode_manifest_scalar(key_writer, key);
                    CanonicalWriter value_writer;
                    encode_manifest_scalar(value_writer, mapped);
                    encoded.emplace_back(key_writer.take(), value_writer.take());
                }
                std::sort(encoded.begin(), encoded.end(),
                          [](const auto &lhs, const auto &rhs) { return lhs.first < rhs.first; });
                writer.varint(encoded.size());
                for (const auto &[key_bytes, value_bytes] : encoded)
                {
                    writer.bytes_field(key_bytes);
                    writer.bytes_field(value_bytes);
                }
            },
            [&](const CyclicBufferView &) -> void {
                throw UnsupportedManifestValue(
                    "cyclic-buffer values have no canonical manifest encoding");
            },
            [&](const QueueView &) -> void {
                throw UnsupportedManifestValue(
                    "queue values have no canonical manifest encoding");
            });
    }
}  // namespace hgraph::manifest
