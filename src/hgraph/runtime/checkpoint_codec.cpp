#include <hgraph/runtime/checkpoint_codec.h>

#include <hgraph/manifest/schema_descriptor.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/value/binary_codec.h>

#include <ankerl/unordered_dense.h>

#include <bit>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <utility>
#include <vector>

// RFC 0039. The layout is:
//
//   marker, version, kind, base time, [component header], body length,
//   string table, value-schema table, time-series-schema table, body, checksum
//
// The encoder builds the body while interning table entries and emits the
// tables ahead of it, so a decoder resolves every entry once before it reads
// an image and nothing is walked twice on either side.
namespace hgraph
{
    namespace
    {
        constexpr std::string_view image_marker{"hgraph.checkpoint-image"};
        constexpr std::size_t max_depth{256};

        enum class ImageKind : std::uint8_t { Graph = 0, Component = 1 };

        // Endpoint flag word. The first seven bits cover every leaf, so a
        // valid atomic endpoint with a payload of its declared type is one byte.
        enum : std::uint64_t
        {
            ts_valid = 1u << 0,
            ts_payload = 1u << 1,
            ts_children = 1u << 2,
            ts_keyed = 1u << 3,
            ts_reference = 1u << 4,
            ts_window = 1u << 5,
            ts_explicit_schema = 1u << 6,
            ts_explicit_payload_schema = 1u << 7,
            ts_dense_slots = 1u << 8,
            ts_all_published = 1u << 9,
            ts_key_set_valid = 1u << 10,
            ts_mixed_key_schemas = 1u << 11,
            ts_known_flags = (1u << 12) - 1,
        };

        enum : std::uint64_t
        {
            node_output = 1u << 0,
            node_error = 1u << 1,
            node_recordable_state = 1u << 2,
            node_ingress = 1u << 3,
            node_alternatives = 1u << 4,
            node_input_activity = 1u << 5,
            node_custom_payload = 1u << 6,
            node_custom_endpoints = 1u << 7,
            node_children = 1u << 8,
            node_known_flags = (1u << 9) - 1,
        };

        enum class ValueRecipe : std::uint8_t { Named = 0, Structural = 1, Frame = 2, Series = 3 };
        enum class ChildKeys : std::uint8_t { None = 0, Uniform = 1, Mixed = 2 };

        [[noreturn]] void malformed(std::string_view detail)
        {
            throw std::runtime_error("invalid checkpoint image: " + std::string{detail});
        }

        void check_depth(std::size_t depth)
        {
            if (depth > max_depth) { malformed("nesting exceeds supported depth"); }
        }

        [[nodiscard]] std::string_view as_text(const std::vector<std::byte> &bytes) noexcept
        {
            return {reinterpret_cast<const char *>(bytes.data()), bytes.size()};
        }

        [[nodiscard]] std::uint64_t ticks(DateTime time) noexcept
        {
            return std::bit_cast<std::uint64_t>(static_cast<std::int64_t>(time.time_since_epoch().count()));
        }

        [[nodiscard]] DateTime from_ticks(std::uint64_t value) noexcept
        {
            return DateTime{TimeDelta{std::bit_cast<std::int64_t>(value)}};
        }

        // FNV-1a over little-endian 64-bit words, the tail folded one byte at
        // a time. Word-wise so that verifying an image costs a small fraction
        // of decoding it; specified here because the value is durable.
        [[nodiscard]] std::uint64_t checksum(std::string_view bytes) noexcept
        {
            constexpr std::uint64_t prime{0x100000001b3ull};
            std::uint64_t hash{0xcbf29ce484222325ull};
            std::size_t at{0};
            for (; at + 8 <= bytes.size(); at += 8)
            {
                std::uint64_t word{};
                std::memcpy(&word, bytes.data() + at, 8);
                if constexpr (std::endian::native == std::endian::big) { word = std::byteswap(word); }
                hash = (hash ^ word) * prime;
            }
            for (; at < bytes.size(); ++at)
                hash = (hash ^ static_cast<unsigned char>(bytes[at])) * prime;
            return hash;
        }

        void write_fixed(std::uint64_t value, std::string &out)
        {
            if constexpr (std::endian::native == std::endian::big) { value = std::byteswap(value); }
            out.append(reinterpret_cast<const char *>(&value), sizeof(value));
        }

        void write_text(std::string_view text, std::string &out)
        {
            write_varint(text.size(), out);
            out.append(text);
        }

        // Offsets use modular arithmetic so that every pair of times round-trips,
        // including the MIN/MAX sentinels, without signed overflow.
        void write_offset(DateTime time, DateTime reference, std::string &out)
        {
            const auto delta = std::bit_cast<std::int64_t>(ticks(time) - ticks(reference));
            write_varint((static_cast<std::uint64_t>(delta) << 1u) ^ static_cast<std::uint64_t>(delta >> 63), out);
        }

        void write_delta(std::size_t value, std::size_t previous, std::string &out)
        {
            const auto delta = std::bit_cast<std::int64_t>(static_cast<std::uint64_t>(value) -
                                                           static_cast<std::uint64_t>(previous));
            write_varint((static_cast<std::uint64_t>(delta) << 1u) ^ static_cast<std::uint64_t>(delta >> 63), out);
        }

        // The schema a position implies. An image in the implied form names no
        // schema; built-in representations only ever produce that form.
        [[nodiscard]] const TSValueTypeMetaData *implied_child(const TSValueTypeMetaData *parent,
                                                               std::size_t index) noexcept
        {
            if (parent == nullptr) { return nullptr; }
            switch (parent->kind)
            {
                case TSTypeKind::TSB: return index < parent->field_count() ? parent->fields()[index].type : nullptr;
                case TSTypeKind::TSL:
                case TSTypeKind::TSD: return parent->element_ts();
                default: return nullptr;
            }
        }

        [[nodiscard]] const ValueTypeMetaData *implied_payload(const TSValueTypeMetaData *schema) noexcept
        {
            return schema->kind == TSTypeKind::TS || schema->kind == TSTypeKind::SIGNAL ? schema->value_type
                                                                                       : nullptr;
        }

        [[nodiscard]] const ValueTypeMetaData *implied_key(const TSValueTypeMetaData *schema) noexcept
        {
            switch (schema->kind)
            {
                case TSTypeKind::TSD: return schema->key_type();
                case TSTypeKind::TSS: return schema->value_type != nullptr ? schema->value_type->element_type : nullptr;
                default: return nullptr;
            }
        }

        struct Encoder
        {
            std::string body{};
            std::string strings{};
            std::string value_schemas{};
            std::string ts_schemas{};
            ankerl::unordered_dense::map<std::string_view, std::size_t> string_index{};
            ankerl::unordered_dense::map<const ValueTypeMetaData *, std::size_t> value_index{};
            ankerl::unordered_dense::map<const TSValueTypeMetaData *, std::size_t> ts_index{};
            std::vector<BoundBinaryConverter> converters{};

            std::size_t string_ref(std::string_view text)
            {
                const auto [entry, added] = string_index.try_emplace(text, string_index.size());
                if (added) { write_text(text, strings); }
                return entry->second;
            }

            // Children are interned before their owner, so a recipe only ever
            // refers to a smaller index and a decoder needs no forward pass.
            std::size_t value_ref(const ValueTypeMetaData *schema, std::size_t depth = 0)
            {
                check_depth(depth);
                if (schema == nullptr) { malformed("missing value schema"); }
                if (const auto found = value_index.find(schema); found != value_index.end()) { return found->second; }
                auto &registry = TypeRegistry::instance();
                std::string record;
                write_text(schema->name(), record);
                write_text(as_text(manifest::value_descriptor(schema)), record);
                if (registry.value_type(schema->name()) == schema)
                {
                    write_varint(static_cast<std::uint8_t>(ValueRecipe::Named), record);
                }
                else if (registry.is_frame(schema))
                {
                    write_varint(static_cast<std::uint8_t>(ValueRecipe::Frame), record);
                    write_varint(value_ref(schema->element_type, depth + 1), record);
                    write_varint(schema->key_type != nullptr, record);
                    if (schema->key_type != nullptr) { write_varint(value_ref(schema->key_type, depth + 1), record); }
                }
                else if (registry.is_series(schema))
                {
                    write_varint(static_cast<std::uint8_t>(ValueRecipe::Series), record);
                    write_varint(value_ref(schema->element_type, depth + 1), record);
                }
                else
                {
                    write_varint(static_cast<std::uint8_t>(ValueRecipe::Structural), record);
                    write_varint(static_cast<std::uint64_t>(schema->value_kind()), record);
                    write_varint(static_cast<std::uint64_t>(schema->flags), record);
                    write_varint(schema->fixed_size, record);
                    switch (schema->value_kind())
                    {
                        case ValueTypeKind::List:
                        case ValueTypeKind::Set:
                            write_varint(value_ref(schema->element_type, depth + 1), record);
                            break;
                        case ValueTypeKind::Map:
                            write_varint(value_ref(schema->key_type, depth + 1), record);
                            write_varint(value_ref(schema->element_type, depth + 1), record);
                            break;
                        case ValueTypeKind::Tuple:
                        case ValueTypeKind::Bundle:
                            write_varint(schema->field_count, record);
                            for (std::size_t index = 0; index < schema->field_count; ++index)
                            {
                                const auto *name = schema->fields[index].name;
                                write_text(name == nullptr ? std::string_view{} : std::string_view{name}, record);
                                write_varint(value_ref(schema->fields[index].type, depth + 1), record);
                            }
                            break;
                        default:
                            malformed("unregistered value schema '" + std::string{schema->name()} +
                                      "' cannot be reconstructed");
                    }
                }
                const auto index = value_index.size();
                value_index.emplace(schema, index);
                converters.push_back(bind_binary_converter(schema));
                value_schemas.append(record);
                return index;
            }

            std::size_t ts_ref(const TSValueTypeMetaData *schema, std::size_t depth = 0)
            {
                check_depth(depth);
                if (schema == nullptr) { malformed("missing endpoint schema"); }
                if (const auto found = ts_index.find(schema); found != ts_index.end()) { return found->second; }
                std::string record;
                write_text(schema->name(), record);
                write_text(as_text(manifest::ts_descriptor(schema)), record);
                const bool named = TypeRegistry::instance().time_series_type(schema->name()) == schema;
                write_varint(named, record);
                if (!named)
                {
                    write_varint(static_cast<std::uint64_t>(schema->kind), record);
                    switch (schema->kind)
                    {
                        case TSTypeKind::REF:
                            write_varint(ts_ref(schema->referenced_ts(), depth + 1), record);
                            break;
                        case TSTypeKind::TS:
                            write_varint(value_ref(schema->value_type, depth + 1), record);
                            break;
                        case TSTypeKind::TSS:
                            write_varint(value_ref(schema->value_type->element_type, depth + 1), record);
                            break;
                        case TSTypeKind::TSW:
                            write_varint(value_ref(schema->value_schema->element_type, depth + 1), record);
                            write_varint(schema->is_duration_based(), record);
                            if (schema->is_duration_based())
                            {
                                write_varint(std::bit_cast<std::uint64_t>(
                                    static_cast<std::int64_t>(schema->time_range().count())), record);
                                write_varint(std::bit_cast<std::uint64_t>(
                                    static_cast<std::int64_t>(schema->min_time_range().count())), record);
                            }
                            else
                            {
                                write_varint(schema->period(), record);
                                write_varint(schema->min_period(), record);
                            }
                            break;
                        case TSTypeKind::TSD:
                            write_varint(value_ref(schema->data.tsd.key_type, depth + 1), record);
                            write_varint(ts_ref(schema->data.tsd.value_ts, depth + 1), record);
                            break;
                        case TSTypeKind::TSL:
                            write_varint(schema->data.tsl.fixed_size, record);
                            write_varint(ts_ref(schema->data.tsl.element_ts, depth + 1), record);
                            break;
                        case TSTypeKind::TSB:
                            write_varint(schema->data.tsb.field_count, record);
                            for (std::size_t index = 0; index < schema->data.tsb.field_count; ++index)
                            {
                                write_text(schema->data.tsb.fields[index].name, record);
                                write_varint(ts_ref(schema->data.tsb.fields[index].type, depth + 1), record);
                            }
                            break;
                        default:
                            malformed("unregistered endpoint schema '" + std::string{schema->name()} +
                                      "' cannot be reconstructed");
                    }
                }
                const auto index = ts_index.size();
                ts_index.emplace(schema, index);
                ts_schemas.append(record);
                return index;
            }

            void value(const Value &source, std::size_t schema_index)
            {
                converters[schema_index].write(source.view(), body);
            }

            // A value whose schema is not implied by its position.
            void tagged_value(const Value &source)
            {
                write_varint(source.has_value(), body);
                if (!source.has_value()) { return; }
                const auto index = value_ref(source.schema());
                write_varint(index, body);
                value(source, index);
            }

            void path(const std::vector<std::size_t> &parts)
            {
                write_varint(parts.size(), body);
                for (const auto part : parts) { write_varint(part, body); }
            }

            void locator(const TSCheckpointLocator &target)
            {
                if (target.graph_path.size() % 2 != 0 || target.endpoint > 4 ||
                    (target.endpoint != 4 && target.custom_endpoint != 0))
                    malformed("invalid endpoint locator");
                path(target.graph_path);
                write_varint(target.node, body);
                write_varint(target.endpoint, body);
                write_varint(target.custom_endpoint, body);
                path(target.endpoint_path);
                write_varint(target.bindings.size(), body);
                for (const auto &binding : target.bindings)
                {
                    if (binding.requested_schema == nullptr) { malformed("binding locator has no schema"); }
                    write_varint(ts_ref(binding.requested_schema), body);
                    path(binding.path);
                }
            }

            void reference(const TSReferenceCheckpointImage &image, std::size_t depth)
            {
                check_depth(depth);
                write_varint(static_cast<std::uint8_t>(image.kind), body);
                write_varint(image.target_schema != nullptr, body);
                if (image.target_schema != nullptr) { write_varint(ts_ref(image.target_schema), body); }
                write_varint(image.target.has_value(), body);
                if (image.target) { locator(*image.target); }
                write_varint(image.items.size(), body);
                for (const auto &item : image.items) { reference(item, depth + 1); }
            }

            void endpoint(const TSCheckpointImage &image, const TSValueTypeMetaData *implied, DateTime reference_time,
                          std::size_t depth)
            {
                check_depth(depth);
                if (image.schema == nullptr) { malformed("missing endpoint schema"); }
                if (image.version != TSCheckpointImage::current_version)
                    malformed("unsupported endpoint image version");
                if (image.reference && (image.schema->kind != TSTypeKind::REF || image.payload.has_value()))
                    malformed("reference metadata on a non-reference endpoint or beside a value payload");

                const bool keyed = image.slot_capacity != 0 || !image.keys.empty() || !image.slots.empty() ||
                                   !image.free_slots.empty() || !image.published.empty() ||
                                   image.key_set_last_modified_time != MIN_DT;
                bool dense = keyed && image.free_slots.empty() && image.slots.size() == image.keys.size() &&
                             image.slot_capacity == image.slots.size();
                for (std::size_t index = 0; dense && index < image.slots.size(); ++index)
                    dense = image.slots[index] == index;
                bool all_published = keyed && image.published.size() == image.keys.size();
                for (std::size_t index = 0; all_published && index < image.published.size(); ++index)
                    all_published = image.published[index];
                const auto *key_schema = implied_key(image.schema);
                bool mixed_keys = false;
                for (const auto &key : image.keys)
                    if (!key.has_value() || key.schema() != key_schema) { mixed_keys = true; break; }
                const auto *payload_schema = implied_payload(image.schema);
                const bool explicit_payload = image.payload.has_value() && image.payload.schema() != payload_schema;

                std::uint64_t flags{0};
                if (image.last_modified_time != MIN_DT) { flags |= ts_valid; }
                if (image.payload.has_value()) { flags |= ts_payload; }
                if (!image.children.empty()) { flags |= ts_children; }
                if (keyed) { flags |= ts_keyed; }
                if (image.reference) { flags |= ts_reference; }
                if (!image.window_times.empty()) { flags |= ts_window; }
                if (image.schema != implied) { flags |= ts_explicit_schema; }
                if (explicit_payload) { flags |= ts_explicit_payload_schema; }
                if (dense) { flags |= ts_dense_slots; }
                if (all_published) { flags |= ts_all_published; }
                if (image.key_set_last_modified_time != MIN_DT) { flags |= ts_key_set_valid; }
                if (mixed_keys) { flags |= ts_mixed_key_schemas; }
                write_varint(flags, body);

                if (flags & ts_explicit_schema) { write_varint(ts_ref(image.schema), body); }
                if (flags & ts_valid) { write_offset(image.last_modified_time, reference_time, body); }
                const auto own_time = (flags & ts_valid) ? image.last_modified_time : reference_time;
                if (flags & ts_payload)
                {
                    const auto index = value_ref(image.payload.schema());
                    if (explicit_payload) { write_varint(index, body); }
                    value(image.payload, index);
                }
                if (flags & ts_reference)
                {
                    validate_ts_reference_checkpoint(*image.reference);
                    reference(*image.reference, depth + 1);
                }
                if (flags & ts_window)
                {
                    write_varint(image.window_times.size(), body);
                    auto previous = own_time;
                    for (const auto time : image.window_times)
                    {
                        write_offset(time, previous, body);
                        previous = time;
                    }
                }
                if (flags & ts_keyed)
                {
                    if (flags & ts_key_set_valid) { write_offset(image.key_set_last_modified_time, own_time, body); }
                    write_varint(image.keys.size(), body);
                    if (mixed_keys) { for (const auto &key : image.keys) { tagged_value(key); } }
                    else if (!image.keys.empty())
                    {
                        const auto index = value_ref(key_schema);
                        for (const auto &key : image.keys) { value(key, index); }
                    }
                    if (!dense)
                    {
                        write_varint(image.slot_capacity, body);
                        write_varint(image.slots.size(), body);
                        std::size_t previous{0};
                        for (const auto slot : image.slots) { write_delta(slot, previous, body); previous = slot; }
                        write_varint(image.free_slots.size(), body);
                        previous = 0;
                        for (const auto slot : image.free_slots) { write_delta(slot, previous, body); previous = slot; }
                    }
                    if (!all_published)
                    {
                        write_varint(image.published.size(), body);
                        const auto at = body.size();
                        body.append((image.published.size() + 7) / 8, '\0');
                        for (std::size_t index = 0; index < image.published.size(); ++index)
                            if (image.published[index])
                                body[at + index / 8] = static_cast<char>(
                                    static_cast<unsigned char>(body[at + index / 8]) | (1u << (index % 8)));
                    }
                }
                if (flags & ts_children)
                {
                    write_varint(image.children.size(), body);
                    for (std::size_t index = 0; index < image.children.size(); ++index)
                        endpoint(image.children[index], implied_child(image.schema, index), own_time, depth + 1);
                }
            }

            void root_endpoint(const TSCheckpointImage &image, DateTime reference_time, std::size_t depth)
            {
                endpoint(image, nullptr, reference_time, depth);
            }

            void time_or_absent(DateTime time, DateTime reference_time)
            {
                write_varint(time != MIN_DT, body);
                if (time != MIN_DT) { write_offset(time, reference_time, body); }
            }

            void graph(const GraphCheckpointImage &image, DateTime reference_time, std::size_t depth)
            {
                check_depth(depth);
                write_varint(image.nodes.size(), body);
                for (const auto &node : image.nodes)
                {
                    write_varint(string_ref(node.id), body);
                    write_varint(string_ref(node.signature), body);
                    std::uint64_t flags{0};
                    if (node.output) { flags |= node_output; }
                    if (node.error) { flags |= node_error; }
                    if (node.recordable_state) { flags |= node_recordable_state; }
                    if (node.ingress) { flags |= node_ingress; }
                    if (!node.alternatives.empty()) { flags |= node_alternatives; }
                    if (!node.input_activity.empty()) { flags |= node_input_activity; }
                    if (node.custom.payload.has_value()) { flags |= node_custom_payload; }
                    if (!node.custom.endpoints.empty()) { flags |= node_custom_endpoints; }
                    if (!node.custom.children.empty()) { flags |= node_children; }
                    write_varint(flags, body);
                    for (const auto *endpoint_image : {&node.output, &node.error, &node.recordable_state, &node.ingress})
                        if (*endpoint_image) { root_endpoint(**endpoint_image, reference_time, depth + 1); }
                    if (flags & node_alternatives)
                    {
                        write_varint(node.alternatives.size(), body);
                        for (const auto &alternative : node.alternatives)
                        {
                            locator(alternative.binding);
                            root_endpoint(alternative.clocks, reference_time, depth + 1);
                        }
                    }
                    if (flags & node_input_activity)
                    {
                        write_varint(node.input_activity.size(), body);
                        for (const auto &activity : node.input_activity)
                        {
                            if (activity.mode != TSInputActivityMode::Value &&
                                activity.mode != TSInputActivityMode::Structural)
                                malformed("unsupported input activity mode");
                            path(activity.path);
                            write_varint(static_cast<std::uint8_t>(activity.mode), body);
                        }
                    }
                    if (flags & node_custom_payload)
                    {
                        const auto index = value_ref(node.custom.payload.schema());
                        write_varint(index, body);
                        value(node.custom.payload, index);
                    }
                    if (flags & node_custom_endpoints)
                    {
                        write_varint(node.custom.endpoints.size(), body);
                        for (const auto &endpoint_image : node.custom.endpoints)
                            root_endpoint(endpoint_image, reference_time, depth + 1);
                    }
                    if (flags & node_children) { children(node.custom.children, reference_time, depth); }
                }
            }

            void children(const std::vector<ChildGraphCheckpoint> &items, DateTime reference_time, std::size_t depth)
            {
                write_varint(items.size(), body);
                // Siblings of one owner share a key schema; name it once.
                bool any_key{false}, every_key{true}, one_schema{true};
                const ValueTypeMetaData *key_schema{nullptr};
                for (const auto &child : items)
                {
                    if (!child.key.has_value()) { every_key = false; continue; }
                    any_key = true;
                    if (key_schema == nullptr) { key_schema = child.key.schema(); }
                    else if (child.key.schema() != key_schema) { one_schema = false; }
                }
                const auto keys = !any_key ? ChildKeys::None
                                  : every_key && one_schema ? ChildKeys::Uniform : ChildKeys::Mixed;
                write_varint(static_cast<std::uint8_t>(keys), body);
                std::size_t key_index{0};
                if (keys == ChildKeys::Uniform)
                {
                    key_index = value_ref(key_schema);
                    write_varint(key_index, body);
                }
                std::size_t previous{0};
                for (const auto &child : items)
                {
                    if (!child.graph) { malformed("missing child graph image"); }
                    write_delta(child.slot, previous, body);
                    previous = child.slot;
                    if (keys == ChildKeys::Uniform) { value(child.key, key_index); }
                    else if (keys == ChildKeys::Mixed) { tagged_value(child.key); }
                    time_or_absent(child.key_last_modified_time, reference_time);
                    graph(*child.graph, reference_time, depth + 1);
                }
            }

            void finish(std::string_view header, std::string &out)
            {
                const auto start = out.size();
                out.append(header);
                write_varint(body.size(), out);
                write_varint(string_index.size(), out);
                out.append(strings);
                write_varint(value_index.size(), out);
                out.append(value_schemas);
                write_varint(ts_index.size(), out);
                out.append(ts_schemas);
                out.append(body);
                write_fixed(checksum(std::string_view{out}.substr(start)), out);
            }
        };

        struct Decoder
        {
            BinaryReader &reader;
            std::vector<std::string> strings{};
            std::vector<const ValueTypeMetaData *> value_schemas{};
            std::vector<BoundBinaryConverter> converters{};
            std::vector<const TSValueTypeMetaData *> ts_schemas{};
            ankerl::unordered_dense::map<const ValueTypeMetaData *, std::size_t> value_positions{};

            [[nodiscard]] std::uint64_t number() { return read_varint(reader); }

            [[nodiscard]] bool boolean()
            {
                const auto value = number();
                if (value > 1) { malformed("invalid boolean"); }
                return value != 0;
            }

            [[nodiscard]] std::size_t size()
            {
                const auto value = number();
                if (value > std::numeric_limits<std::size_t>::max()) { malformed("size out of range"); }
                return static_cast<std::size_t>(value);
            }

            // Every element of a sequence occupies at least one byte, so a
            // count larger than the remaining input is refused before it can
            // size an allocation.
            [[nodiscard]] std::size_t count()
            {
                const auto value = number();
                if (value > reader.remaining()) { malformed("invalid sequence size"); }
                reader.consume_work(value);
                return static_cast<std::size_t>(value);
            }

            [[nodiscard]] std::string_view text()
            {
                const auto length = number();
                if (length > reader.remaining()) { malformed("truncated text"); }
                const auto *bytes = reader.take(static_cast<std::size_t>(length));
                return {reinterpret_cast<const char *>(bytes), static_cast<std::size_t>(length)};
            }

            [[nodiscard]] DateTime offset(DateTime reference_time)
            {
                const auto encoded = number();
                const auto delta = static_cast<std::uint64_t>((encoded >> 1u) ^ (~(encoded & 1u) + 1u));
                return from_ticks(ticks(reference_time) + delta);
            }

            [[nodiscard]] std::size_t delta(std::size_t previous)
            {
                const auto encoded = number();
                const auto change = static_cast<std::uint64_t>((encoded >> 1u) ^ (~(encoded & 1u) + 1u));
                return static_cast<std::size_t>(static_cast<std::uint64_t>(previous) + change);
            }

            template <typename T> [[nodiscard]] T table_entry(const std::vector<T> &table, std::string_view what)
            {
                const auto index = number();
                if (index >= table.size()) { malformed("unknown " + std::string{what} + " index"); }
                return table[static_cast<std::size_t>(index)];
            }

            [[nodiscard]] std::size_t value_index()
            {
                const auto index = number();
                if (index >= value_schemas.size()) { malformed("unknown value schema index"); }
                return static_cast<std::size_t>(index);
            }

            [[nodiscard]] std::size_t value_index_of(const ValueTypeMetaData *schema) const
            {
                const auto found = value_positions.find(schema);
                if (found == value_positions.end()) { malformed("implied value schema is absent from the image"); }
                return found->second;
            }

            void read_strings()
            {
                const auto entries = count();
                strings.reserve(entries);
                for (std::size_t index = 0; index < entries; ++index) { strings.emplace_back(text()); }
            }

            void read_value_schemas()
            {
                auto &registry = TypeRegistry::instance();
                const auto entries = count();
                value_schemas.reserve(entries);
                converters.reserve(entries);
                const auto earlier = [&] { return table_entry(value_schemas, "value schema"); };
                for (std::size_t entry = 0; entry < entries; ++entry)
                {
                    const auto name = text();
                    const auto description = text();
                    const ValueTypeMetaData *schema{nullptr};
                    switch (static_cast<ValueRecipe>(number()))
                    {
                        case ValueRecipe::Named: schema = registry.value_type(name); break;
                        case ValueRecipe::Frame: {
                            const auto *row = earlier();
                            const auto *metadata = boolean() ? earlier() : nullptr;
                            schema = registry.frame(row, metadata);
                            break;
                        }
                        case ValueRecipe::Series: schema = registry.series(earlier()); break;
                        case ValueRecipe::Structural: schema = structural_value_schema(registry, earlier); break;
                        default: malformed("unsupported value schema recipe");
                    }
                    if (schema == nullptr) { malformed("unknown value schema " + std::string{name}); }
                    if (schema->name() != name || as_text(manifest::value_descriptor(schema)) != description)
                        malformed("value schema changed");
                    value_positions.try_emplace(schema, value_schemas.size());
                    value_schemas.push_back(schema);
                    converters.push_back(bind_binary_converter(schema));
                }
            }

            template <typename Earlier>
            [[nodiscard]] const ValueTypeMetaData *structural_value_schema(TypeRegistry &registry, Earlier &&earlier)
            {
                const auto kind = number();
                if (kind > static_cast<std::uint64_t>(ValueTypeKind::Any)) { malformed("invalid value kind"); }
                const auto raw_flags = number();
                if (raw_flags > std::numeric_limits<std::uint32_t>::max()) { malformed("invalid value flags"); }
                const auto flags = static_cast<ValueTypeFlags>(raw_flags);
                const auto extent = size();
                const auto flag = [flags](ValueTypeFlags value) { return (flags & value) != ValueTypeFlags::None; };
                switch (static_cast<ValueTypeKind>(kind))
                {
                    case ValueTypeKind::List: {
                        const auto *element = earlier();
                        if (flag(ValueTypeFlags::ShapedArray)) { return registry.array(element, extent); }
                        if (flag(ValueTypeFlags::Nullable)) { return registry.nullable_tuple(element); }
                        if (flag(ValueTypeFlags::Mutable)) { return registry.mutable_list(element); }
                        if (flag(ValueTypeFlags::FixedEmpty)) { return registry.fixed_list(element, extent); }
                        return registry.list(element, extent, flag(ValueTypeFlags::VariadicTuple));
                    }
                    case ValueTypeKind::Set: {
                        const auto *element = earlier();
                        return flag(ValueTypeFlags::Mutable) ? registry.mutable_set(element) : registry.set(element);
                    }
                    case ValueTypeKind::Map: {
                        const auto *key = earlier();
                        const auto *element = earlier();
                        return flag(ValueTypeFlags::Mutable) ? registry.mutable_map(key, element)
                                                             : registry.map(key, element);
                    }
                    case ValueTypeKind::Tuple:
                    case ValueTypeKind::Bundle: {
                        const auto fields_count = count();
                        std::vector<const ValueTypeMetaData *> elements;
                        std::vector<std::pair<std::string, const ValueTypeMetaData *>> fields;
                        elements.reserve(fields_count);
                        fields.reserve(fields_count);
                        for (std::size_t index = 0; index < fields_count; ++index)
                        {
                            std::string field{text()};
                            const auto *element = earlier();
                            elements.push_back(element);
                            fields.emplace_back(std::move(field), element);
                        }
                        return kind == static_cast<std::uint64_t>(ValueTypeKind::Tuple)
                                   ? registry.tuple(elements) : registry.un_named_bundle(fields);
                    }
                    default: malformed("unsupported value schema recipe");
                }
            }

            void read_ts_schemas()
            {
                auto &registry = TypeRegistry::instance();
                const auto entries = count();
                ts_schemas.reserve(entries);
                const auto value = [&] { return table_entry(value_schemas, "value schema"); };
                const auto earlier = [&] { return table_entry(ts_schemas, "endpoint schema"); };
                for (std::size_t entry = 0; entry < entries; ++entry)
                {
                    const auto name = text();
                    const auto description = text();
                    const TSValueTypeMetaData *schema{nullptr};
                    if (boolean()) { schema = registry.time_series_type(name); }
                    else
                    {
                        const auto kind = number();
                        if (kind > static_cast<std::uint64_t>(TSTypeKind::SIGNAL)) { malformed("invalid endpoint kind"); }
                        switch (static_cast<TSTypeKind>(kind))
                        {
                            case TSTypeKind::REF: schema = registry.ref(earlier()); break;
                            case TSTypeKind::TS: schema = registry.ts(value()); break;
                            case TSTypeKind::TSS: schema = registry.tss(value()); break;
                            case TSTypeKind::TSW: {
                                const auto *element = value();
                                const bool duration = boolean();
                                const auto period = number();
                                const auto minimum = number();
                                if (duration)
                                {
                                    const auto range = std::bit_cast<std::int64_t>(period);
                                    const auto min_range = std::bit_cast<std::int64_t>(minimum);
                                    if (range <= 0 || min_range < 0 || min_range > range)
                                        malformed("invalid duration window extent");
                                    schema = registry.tsw_duration(element, TimeDelta{range}, TimeDelta{min_range});
                                }
                                else
                                {
                                    if (period == 0 || period > std::numeric_limits<std::size_t>::max() ||
                                        minimum > period)
                                        malformed("invalid count window extent");
                                    schema = registry.tsw(element, static_cast<std::size_t>(period),
                                                          static_cast<std::size_t>(minimum));
                                }
                                break;
                            }
                            case TSTypeKind::TSD: {
                                const auto *key = value();
                                schema = registry.tsd(key, earlier());
                                break;
                            }
                            case TSTypeKind::TSL: {
                                const auto extent = size();
                                schema = registry.tsl(earlier(), extent);
                                break;
                            }
                            case TSTypeKind::TSB: {
                                const auto fields_count = count();
                                std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
                                fields.reserve(fields_count);
                                for (std::size_t index = 0; index < fields_count; ++index)
                                {
                                    std::string field{text()};
                                    fields.emplace_back(std::move(field), earlier());
                                }
                                schema = registry.un_named_tsb(fields);
                                break;
                            }
                            default: malformed("unsupported endpoint schema recipe");
                        }
                    }
                    if (schema == nullptr) { malformed("unknown endpoint schema " + std::string{name}); }
                    if (schema->name() != name || as_text(manifest::ts_descriptor(schema)) != description)
                        malformed("endpoint schema changed");
                    ts_schemas.push_back(schema);
                }
            }

            [[nodiscard]] Value tagged_value()
            {
                if (!boolean()) { return {}; }
                return converters[value_index()].read(reader);
            }

            [[nodiscard]] std::vector<std::size_t> path()
            {
                const auto parts = count();
                std::vector<std::size_t> result;
                result.reserve(parts);
                for (std::size_t index = 0; index < parts; ++index) { result.push_back(size()); }
                return result;
            }

            [[nodiscard]] TSCheckpointLocator locator()
            {
                TSCheckpointLocator target;
                target.graph_path = path();
                target.node = size();
                const auto endpoint_role = number();
                if (endpoint_role > 4 || target.graph_path.size() % 2 != 0) { malformed("invalid endpoint locator"); }
                target.endpoint = static_cast<std::uint32_t>(endpoint_role);
                target.custom_endpoint = size();
                if (endpoint_role != 4 && target.custom_endpoint != 0) { malformed("unexpected custom endpoint ordinal"); }
                target.endpoint_path = path();
                const auto bindings = count();
                target.bindings.reserve(bindings);
                for (std::size_t index = 0; index < bindings; ++index)
                {
                    TSCheckpointBindingStep step;
                    step.requested_schema = table_entry(ts_schemas, "endpoint schema");
                    step.path = path();
                    target.bindings.push_back(std::move(step));
                }
                return target;
            }

            [[nodiscard]] TSReferenceCheckpointImage reference(std::size_t depth)
            {
                check_depth(depth);
                TSReferenceCheckpointImage image;
                const auto kind = number();
                if (kind > static_cast<std::uint8_t>(TSReferenceCheckpointKind::NonPeered))
                    malformed("unsupported reference kind");
                image.kind = static_cast<TSReferenceCheckpointKind>(kind);
                if (boolean()) { image.target_schema = table_entry(ts_schemas, "endpoint schema"); }
                if (boolean()) { image.target = locator(); }
                const auto items = count();
                image.items.reserve(items);
                for (std::size_t index = 0; index < items; ++index) { image.items.push_back(reference(depth + 1)); }
                return image;
            }

            [[nodiscard]] TSCheckpointImage endpoint(const TSValueTypeMetaData *implied, DateTime reference_time,
                                                     std::size_t depth)
            {
                check_depth(depth);
                auto scope = reader.enter();
                TSCheckpointImage image;
                const auto flags = number();
                if ((flags & ~static_cast<std::uint64_t>(ts_known_flags)) != 0) { malformed("unknown endpoint flags"); }
                if ((flags & (ts_dense_slots | ts_all_published | ts_key_set_valid | ts_mixed_key_schemas)) != 0 &&
                    (flags & ts_keyed) == 0)
                    malformed("keyed flags on an unkeyed endpoint");
                if ((flags & ts_explicit_payload_schema) != 0 && (flags & ts_payload) == 0)
                    malformed("payload schema without a payload");
                image.schema = (flags & ts_explicit_schema) ? table_entry(ts_schemas, "endpoint schema") : implied;
                if (image.schema == nullptr) { malformed("endpoint has no schema"); }
                if (flags & ts_valid) { image.last_modified_time = offset(reference_time); }
                const auto own_time = (flags & ts_valid) ? image.last_modified_time : reference_time;
                if (flags & ts_payload)
                {
                    std::size_t index{};
                    if (flags & ts_explicit_payload_schema) { index = value_index(); }
                    else
                    {
                        const auto *payload_schema = implied_payload(image.schema);
                        if (payload_schema == nullptr) { malformed("endpoint implies no payload schema"); }
                        index = value_index_of(payload_schema);
                    }
                    image.payload = converters[index].read(reader);
                }
                if (flags & ts_reference)
                {
                    if (image.schema->kind != TSTypeKind::REF || image.payload.has_value())
                        malformed("reference metadata on a non-reference endpoint or beside a value payload");
                    image.reference = reference(depth + 1);
                    validate_ts_reference_checkpoint(*image.reference);
                }
                if (flags & ts_window)
                {
                    const auto samples = count();
                    image.window_times.reserve(samples);
                    auto previous = own_time;
                    for (std::size_t index = 0; index < samples; ++index)
                    {
                        previous = offset(previous);
                        image.window_times.push_back(previous);
                    }
                }
                if (flags & ts_keyed)
                {
                    if (flags & ts_key_set_valid) { image.key_set_last_modified_time = offset(own_time); }
                    const auto keys = count();
                    image.keys.reserve(keys);
                    if (flags & ts_mixed_key_schemas)
                    {
                        for (std::size_t index = 0; index < keys; ++index) { image.keys.push_back(tagged_value()); }
                    }
                    else if (keys != 0)
                    {
                        const auto *key_schema = implied_key(image.schema);
                        if (key_schema == nullptr) { malformed("endpoint implies no key schema"); }
                        const auto &converter = converters[value_index_of(key_schema)];
                        for (std::size_t index = 0; index < keys; ++index) { image.keys.push_back(converter.read(reader)); }
                    }
                    if (flags & ts_dense_slots)
                    {
                        image.slot_capacity = keys;
                        image.slots.resize(keys);
                        for (std::size_t index = 0; index < keys; ++index) { image.slots[index] = index; }
                    }
                    else
                    {
                        image.slot_capacity = size();
                        const auto slots = count();
                        image.slots.reserve(slots);
                        std::size_t previous{0};
                        for (std::size_t index = 0; index < slots; ++index)
                        {
                            previous = delta(previous);
                            image.slots.push_back(previous);
                        }
                        const auto free_slots = count();
                        image.free_slots.reserve(free_slots);
                        previous = 0;
                        for (std::size_t index = 0; index < free_slots; ++index)
                        {
                            previous = delta(previous);
                            image.free_slots.push_back(previous);
                        }
                    }
                    if (flags & ts_all_published) { image.published.assign(keys, true); }
                    else
                    {
                        const auto published = number();
                        if (published > reader.remaining() * 8) { malformed("invalid published plane size"); }
                        reader.consume_work(published);
                        const auto *bits = reader.take(static_cast<std::size_t>((published + 7) / 8));
                        // Sized by assignment: vector<bool>::reserve trips
                        // -Werror=array-bounds on GCC 14.
                        image.published.assign(static_cast<std::size_t>(published), false);
                        for (std::size_t index = 0; index < published; ++index)
                            image.published[index] =
                                (std::to_integer<unsigned>(bits[index / 8]) & (1u << (index % 8))) != 0;
                    }
                }
                if (flags & ts_children)
                {
                    const auto children = count();
                    image.children.reserve(children);
                    for (std::size_t index = 0; index < children; ++index)
                        image.children.push_back(
                            endpoint(implied_child(image.schema, index), own_time, depth + 1));
                }
                return image;
            }

            [[nodiscard]] DateTime time_or_absent(DateTime reference_time)
            {
                return boolean() ? offset(reference_time) : MIN_DT;
            }

            [[nodiscard]] GraphCheckpointImage graph(DateTime reference_time, std::size_t depth)
            {
                check_depth(depth);
                auto scope = reader.enter();
                GraphCheckpointImage image;
                const auto nodes = count();
                image.nodes.reserve(nodes);
                for (std::size_t index = 0; index < nodes; ++index)
                {
                    NodeCheckpointImage node;
                    node.id = table_entry(strings, "string");
                    node.signature = table_entry(strings, "string");
                    const auto flags = number();
                    if ((flags & ~static_cast<std::uint64_t>(node_known_flags)) != 0) { malformed("unknown node flags"); }
                    if (flags & node_output) { node.output = endpoint(nullptr, reference_time, depth + 1); }
                    if (flags & node_error) { node.error = endpoint(nullptr, reference_time, depth + 1); }
                    if (flags & node_recordable_state)
                        node.recordable_state = endpoint(nullptr, reference_time, depth + 1);
                    if (flags & node_ingress) { node.ingress = endpoint(nullptr, reference_time, depth + 1); }
                    if (flags & node_alternatives)
                    {
                        const auto alternatives = count();
                        node.alternatives.reserve(alternatives);
                        for (std::size_t alternative = 0; alternative < alternatives; ++alternative)
                        {
                            EndpointBindingCheckpoint saved;
                            saved.binding = locator();
                            saved.clocks = endpoint(nullptr, reference_time, depth + 1);
                            node.alternatives.push_back(std::move(saved));
                        }
                    }
                    if (flags & node_input_activity)
                    {
                        const auto activities = count();
                        node.input_activity.reserve(activities);
                        for (std::size_t activity = 0; activity < activities; ++activity)
                        {
                            TSInputActivityEntry entry;
                            entry.path = path();
                            const auto mode = number();
                            if (mode != static_cast<std::uint8_t>(TSInputActivityMode::Value) &&
                                mode != static_cast<std::uint8_t>(TSInputActivityMode::Structural))
                                malformed("unsupported input activity mode");
                            entry.mode = static_cast<TSInputActivityMode>(mode);
                            node.input_activity.push_back(std::move(entry));
                        }
                    }
                    if (flags & node_custom_payload) { node.custom.payload = converters[value_index()].read(reader); }
                    if (flags & node_custom_endpoints)
                    {
                        const auto endpoints = count();
                        node.custom.endpoints.reserve(endpoints);
                        for (std::size_t item = 0; item < endpoints; ++item)
                            node.custom.endpoints.push_back(endpoint(nullptr, reference_time, depth + 1));
                    }
                    if (flags & node_children) { node.custom.children = children(reference_time, depth); }
                    image.nodes.push_back(std::move(node));
                }
                return image;
            }

            [[nodiscard]] std::vector<ChildGraphCheckpoint> children(DateTime reference_time, std::size_t depth)
            {
                const auto entries = count();
                std::vector<ChildGraphCheckpoint> result;
                result.reserve(entries);
                const auto keys = number();
                if (keys > static_cast<std::uint8_t>(ChildKeys::Mixed)) { malformed("invalid child key mode"); }
                const BoundBinaryConverter *converter{nullptr};
                if (keys == static_cast<std::uint8_t>(ChildKeys::Uniform)) { converter = &converters[value_index()]; }
                std::size_t previous{0};
                for (std::size_t index = 0; index < entries; ++index)
                {
                    ChildGraphCheckpoint child;
                    previous = delta(previous);
                    child.slot = previous;
                    if (converter != nullptr) { child.key = converter->read(reader); }
                    else if (keys == static_cast<std::uint8_t>(ChildKeys::Mixed)) { child.key = tagged_value(); }
                    child.key_last_modified_time = time_or_absent(reference_time);
                    child.graph = std::make_shared<GraphCheckpointImage>(graph(reference_time, depth + 1));
                    result.push_back(std::move(child));
                }
                return result;
            }
        };

        void write_header(ImageKind kind, DateTime base_time, std::string &header)
        {
            write_text(image_marker, header);
            write_varint(checkpoint_image_format_version, header);
            write_varint(static_cast<std::uint8_t>(kind), header);
            write_fixed(ticks(base_time), header);
        }

        [[nodiscard]] std::uint64_t read_fixed(BinaryReader &reader)
        {
            std::uint64_t value{};
            std::memcpy(&value, reader.take(sizeof(value)), sizeof(value));
            if constexpr (std::endian::native == std::endian::big) { value = std::byteswap(value); }
            return value;
        }

        // Verify the checksum over the whole image before interpreting any of
        // it, then hand back a reader positioned after the fixed header.
        struct OpenedImage
        {
            BinaryReader reader;
            DateTime base_time;
        };

        [[nodiscard]] OpenedImage open_image(std::string_view bytes, ImageKind expected)
        {
            if (bytes.size() < sizeof(std::uint64_t)) { malformed("truncated image"); }
            const auto content = bytes.substr(0, bytes.size() - sizeof(std::uint64_t));
            // The work budget scales with the input: every decoded element is
            // at least one byte, except zero-width values inside collections,
            // which the constant allowance covers.
            BinaryDecodeLimits limits;
            limits.max_work = 1'000'000 + 16 * static_cast<std::uint64_t>(bytes.size());
            limits.max_depth = 4 * max_depth;
            BinaryReader tail{bytes, content.size(), limits};
            const auto stored = read_fixed(tail);
            BinaryReader reader{content, 0, limits};
            Decoder probe{reader};
            if (probe.text() != image_marker) { malformed("unsupported format"); }
            if (probe.number() != checkpoint_image_format_version) { malformed("unsupported image version"); }
            if (stored != checksum(content)) { malformed("checksum mismatch"); }
            if (probe.number() != static_cast<std::uint8_t>(expected)) { malformed("unexpected image kind"); }
            const auto base_time = from_ticks(read_fixed(reader));
            return {reader, base_time};
        }

        [[nodiscard]] GraphCheckpointImage read_graph(BinaryReader &reader, DateTime base_time)
        {
            Decoder decoder{reader};
            const auto body_length = decoder.size();
            decoder.read_strings();
            decoder.read_value_schemas();
            decoder.read_ts_schemas();
            if (reader.remaining() != body_length) { malformed("body length disagrees with the image"); }
            auto graph = decoder.graph(base_time, 0);
            if (reader.remaining() != 0) { malformed("trailing data"); }
            return graph;
        }
    }

    bool is_checkpoint_image(std::string_view bytes) noexcept
    {
        // One length byte precedes the marker: it is shorter than 128 bytes.
        return bytes.size() > image_marker.size() &&
               static_cast<unsigned char>(bytes.front()) == image_marker.size() &&
               bytes.substr(1, image_marker.size()) == image_marker;
    }

    void encode_component_checkpoint(const ComponentCheckpoint &checkpoint, std::string &out)
    {
        if (checkpoint.version != ComponentCheckpoint::current_version || checkpoint.component_id.empty() ||
            checkpoint.completed_until <= checkpoint.cut)
            malformed("invalid completed component boundary");
        std::string header;
        write_header(ImageKind::Component, checkpoint.cut, header);
        write_text(checkpoint.component_id, header);
        write_text(checkpoint.graph_signature, header);
        write_offset(checkpoint.completed_until, checkpoint.cut, header);
        Encoder encoder;
        encoder.graph(checkpoint.graph, checkpoint.cut, 0);
        encoder.finish(header, out);
    }

    ComponentCheckpoint decode_component_checkpoint(std::string_view bytes)
    {
        auto [reader, base_time] = open_image(bytes, ImageKind::Component);
        Decoder header{reader};
        ComponentCheckpoint checkpoint;
        checkpoint.component_id = header.text();
        checkpoint.graph_signature = header.text();
        checkpoint.cut = base_time;
        checkpoint.completed_until = header.offset(base_time);
        if (checkpoint.component_id.empty() || checkpoint.completed_until <= checkpoint.cut)
            malformed("invalid completed component boundary");
        checkpoint.graph = read_graph(reader, base_time);
        return checkpoint;
    }

    void encode_graph_checkpoint(const GraphCheckpointImage &graph, std::string &out, DateTime base_time)
    {
        std::string header;
        write_header(ImageKind::Graph, base_time, header);
        Encoder encoder;
        encoder.graph(graph, base_time, 0);
        encoder.finish(header, out);
    }

    GraphCheckpointImage decode_graph_checkpoint(std::string_view bytes)
    {
        auto [reader, base_time] = open_image(bytes, ImageKind::Graph);
        return read_graph(reader, base_time);
    }
}
