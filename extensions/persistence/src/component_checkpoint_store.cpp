#include <hgraph/persistence/component_checkpoint_store.h>

#include <hgraph/persistence/store_location.h>
#include <hgraph/manifest/schema_descriptor.h>
#include <hgraph/runtime/checkpoint_codec.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/value/json_codec.h>

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/table.h>
#include <arrow/util/key_value_metadata.h>

#include <bit>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <utility>

namespace hgraph::persistence
{
    namespace
    {
        // The envelope is one Arrow cell plus two metadata entries. The image
        // inside it is core's canonical encoding (RFC 0039); this layer owns
        // only publication, the predecessor link and format selection.
        constexpr std::string_view format_name{"hgraph.component-checkpoint.v2"};
        constexpr std::string_view format_key{"hgraph.checkpoint.format"};
        constexpr std::string_view predecessor_key{"hgraph.checkpoint.predecessor"};
        constexpr std::size_t max_depth{256};

        [[noreturn]] void malformed(std::string_view detail)
        {
            throw std::runtime_error("invalid component checkpoint: " + std::string{detail});
        }

        void check(const arrow::Status &status)
        {
            if (!status.ok()) { throw std::runtime_error(status.ToString()); }
        }

        void check_depth(std::size_t depth)
        {
            if (depth > max_depth) { malformed("nesting exceeds supported depth"); }
        }

        // Version 1, as published by hgraph 0.8.25-0.8.27: self-describing
        // per image, eight-byte integers, JSON values. Read-only; a day
        // recovered from one publishes a version 2 successor.
        namespace v1
        {
        constexpr std::string_view format_name{"hgraph.component-checkpoint.v1"};

        [[nodiscard]] std::string value_description(const ValueTypeMetaData *schema)
        {
            const auto bytes = manifest::value_descriptor(schema);
            return {reinterpret_cast<const char *>(bytes.data()), bytes.size()};
        }

        [[nodiscard]] std::string ts_description(const TSValueTypeMetaData *schema)
        {
            const auto bytes = manifest::ts_descriptor(schema);
            return {reinterpret_cast<const char *>(bytes.data()), bytes.size()};
        }

        struct Reader
        {
            std::string_view bytes;
            std::size_t offset{0};

            [[nodiscard]] std::uint64_t number()
            {
                if (bytes.size() - offset < 8) { malformed("truncated integer"); }
                std::uint64_t result{0};
                for (unsigned shift = 0; shift < 64; shift += 8)
                {
                    result |= static_cast<std::uint64_t>(
                                  static_cast<unsigned char>(bytes[offset++])) << shift;
                }
                return result;
            }

            [[nodiscard]] bool boolean()
            {
                const auto value = number();
                if (value > 1) { malformed("invalid boolean"); }
                return value != 0;
            }

            [[nodiscard]] std::size_t count()
            {
                const auto value = number();
                // Every sequence element has at least an eight-byte field.
                // Bound counts before allocating storage for malformed files.
                if (value > (bytes.size() - offset) / 8) { malformed("invalid sequence size"); }
                return static_cast<std::size_t>(value);
            }

            [[nodiscard]] std::string_view text()
            {
                const auto length = number();
                if (length > bytes.size() - offset) { malformed("truncated text"); }
                const auto result = bytes.substr(offset, static_cast<std::size_t>(length));
                offset += static_cast<std::size_t>(length);
                return result;
            }

            [[nodiscard]] DateTime time()
            {
                return DateTime{TimeDelta{std::bit_cast<std::int64_t>(number())}};
            }
        };

        [[nodiscard]] const ValueTypeMetaData *decode_value_schema(Reader &reader, std::size_t depth)
        {
            check_depth(depth);
            const auto name = reader.text();
            const auto description = reader.text();
            auto &registry = TypeRegistry::instance();
            const ValueTypeMetaData *schema{};
            if (reader.boolean()) { schema = registry.value_type(name); }
            else
            {
                const auto kind = reader.number();
                if (kind > static_cast<std::uint64_t>(ValueTypeKind::Any)) { malformed("invalid value kind"); }
                const auto raw_flags = reader.number();
                if (raw_flags > std::numeric_limits<std::uint32_t>::max()) { malformed("invalid value flags"); }
                const auto flags = static_cast<ValueTypeFlags>(raw_flags);
                const auto extent = reader.number();
                if (extent > std::numeric_limits<std::size_t>::max()) { malformed("value extent out of range"); }
                const auto size = static_cast<std::size_t>(extent);
                const auto flag = [flags](ValueTypeFlags value) { return (flags & value) != ValueTypeFlags::None; };
                switch (static_cast<ValueTypeKind>(kind))
                {
                    case ValueTypeKind::List: {
                        const auto *element = decode_value_schema(reader, depth + 1);
                        if (flag(ValueTypeFlags::ShapedArray)) { schema = registry.array(element, size); }
                        else if (flag(ValueTypeFlags::Nullable)) { schema = registry.nullable_tuple(element); }
                        else if (flag(ValueTypeFlags::Mutable)) { schema = registry.mutable_list(element); }
                        else if (flag(ValueTypeFlags::FixedEmpty)) { schema = registry.fixed_list(element, size); }
                        else { schema = registry.list(element, size, flag(ValueTypeFlags::VariadicTuple)); }
                        break;
                    }
                    case ValueTypeKind::Set: {
                        const auto *element = decode_value_schema(reader, depth + 1);
                        schema = flag(ValueTypeFlags::Mutable) ? registry.mutable_set(element) : registry.set(element);
                        break;
                    }
                    case ValueTypeKind::Map: {
                        const auto *key = decode_value_schema(reader, depth + 1);
                        const auto *element = decode_value_schema(reader, depth + 1);
                        schema = flag(ValueTypeFlags::Mutable) ? registry.mutable_map(key, element) : registry.map(key, element);
                        break;
                    }
                    case ValueTypeKind::Tuple:
                    case ValueTypeKind::Bundle: {
                        const auto count = reader.count();
                        std::vector<const ValueTypeMetaData *> elements;
                        std::vector<std::pair<std::string, const ValueTypeMetaData *>> fields;
                        for (std::size_t index = 0; index < count; ++index)
                        {
                            std::string field{reader.text()};
                            const auto *element = decode_value_schema(reader, depth + 1);
                            elements.push_back(element);
                            fields.emplace_back(std::move(field), element);
                        }
                        schema = kind == static_cast<std::uint64_t>(ValueTypeKind::Tuple)
                                     ? registry.tuple(elements) : registry.un_named_bundle(fields);
                        break;
                    }
                    default:
                        malformed("unsupported value schema recipe");
                }
            }
            if (schema == nullptr) { malformed("unknown value schema " + std::string{name}); }
            if (schema->name() != name || value_description(schema) != description)
            {
                malformed("value schema changed");
            }
            return schema;
        }

        [[nodiscard]] const TSValueTypeMetaData *decode_ts_schema(Reader &reader, std::size_t depth)
        {
            check_depth(depth);
            const auto name = reader.text();
            const auto description = reader.text();
            auto &registry = TypeRegistry::instance();
            const TSValueTypeMetaData *schema{};
            if (reader.boolean()) { schema = registry.time_series_type(name); }
            else
            {
                const auto kind = reader.number();
                if (kind > static_cast<std::uint64_t>(TSTypeKind::SIGNAL)) { malformed("invalid endpoint kind"); }
                switch (static_cast<TSTypeKind>(kind))
                {
                    case TSTypeKind::REF:
                        schema = registry.ref(decode_ts_schema(reader, depth + 1));
                        break;
                    case TSTypeKind::TS:
                        schema = registry.ts(decode_value_schema(reader, depth + 1));
                        break;
                    case TSTypeKind::TSS:
                        schema = registry.tss(decode_value_schema(reader, depth + 1));
                        break;
                    case TSTypeKind::TSW: {
                        const auto *element = decode_value_schema(reader, depth + 1);
                        const bool duration = reader.boolean();
                        const auto period = reader.number();
                        const auto minimum = reader.number();
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
                            if (period == 0 || period > std::numeric_limits<std::size_t>::max() || minimum > period)
                                malformed("invalid count window extent");
                            schema = registry.tsw(element, static_cast<std::size_t>(period),
                                                  static_cast<std::size_t>(minimum));
                        }
                        break;
                    }
                    case TSTypeKind::TSD: {
                        const auto *key = decode_value_schema(reader, depth + 1);
                        const auto *element = decode_ts_schema(reader, depth + 1);
                        schema = registry.tsd(key, element);
                        break;
                    }
                    case TSTypeKind::TSL: {
                        const auto size = reader.number();
                        if (size > std::numeric_limits<std::size_t>::max()) { malformed("list extent out of range"); }
                        schema = registry.tsl(decode_ts_schema(reader, depth + 1), static_cast<std::size_t>(size));
                        break;
                    }
                    case TSTypeKind::TSB: {
                        const auto count = reader.count();
                        std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
                        for (std::size_t index = 0; index < count; ++index)
                        {
                            std::string field{reader.text()};
                            const auto *element = decode_ts_schema(reader, depth + 1);
                            fields.emplace_back(std::move(field), element);
                        }
                        schema = registry.un_named_tsb(fields);
                        break;
                    }
                    default:
                        malformed("unsupported endpoint schema recipe");
                }
            }
            if (schema == nullptr) { malformed("unknown endpoint schema " + std::string{name}); }
            if (schema->name() != name || ts_description(schema) != description)
            {
                malformed("endpoint schema changed");
            }
            return schema;
        }

        [[nodiscard]] Value decode_value(Reader &reader)
        {
            if (!reader.boolean()) { return {}; }
            const auto *schema = decode_value_schema(reader, 0);
            return from_json_string(schema, reader.text());
        }

        [[nodiscard]] std::size_t decode_index(Reader &reader)
        {
            const auto index = reader.number();
            if (index > std::numeric_limits<std::size_t>::max()) { malformed("locator index out of range"); }
            return static_cast<std::size_t>(index);
        }

        [[nodiscard]] std::vector<std::size_t> decode_path(Reader &reader)
        {
            std::vector<std::size_t> path;
            const auto count = reader.count();
            path.reserve(count);
            for (std::size_t index = 0; index < count; ++index) { path.push_back(decode_index(reader)); }
            return path;
        }

        [[nodiscard]] TSCheckpointLocator decode_locator(Reader &reader)
        {
            TSCheckpointLocator locator;
            locator.graph_path = decode_path(reader);
            locator.node = decode_index(reader);
            const auto endpoint = reader.number();
            if (endpoint > 4 || locator.graph_path.size() % 2 != 0) { malformed("invalid endpoint locator"); }
            locator.endpoint = static_cast<std::uint32_t>(endpoint);
            locator.custom_endpoint = decode_index(reader);
            if (endpoint != 4 && locator.custom_endpoint != 0) { malformed("unexpected custom endpoint ordinal"); }
            locator.endpoint_path = decode_path(reader);
            const auto count = reader.count();
            locator.bindings.reserve(count);
            for (std::size_t index = 0; index < count; ++index)
            {
                TSCheckpointBindingStep binding;
                binding.requested_schema = decode_ts_schema(reader, 0);
                binding.path = decode_path(reader);
                locator.bindings.push_back(std::move(binding));
            }
            return locator;
        }

        [[nodiscard]] TSReferenceCheckpointImage decode_reference(Reader &reader, std::size_t depth)
        {
            check_depth(depth);
            TSReferenceCheckpointImage reference;
            const auto kind = reader.number();
            if (kind > static_cast<std::uint8_t>(TSReferenceCheckpointKind::NonPeered))
                malformed("unsupported reference kind");
            reference.kind = static_cast<TSReferenceCheckpointKind>(kind);
            if (reader.boolean()) { reference.target_schema = decode_ts_schema(reader, 0); }
            if (reader.boolean()) { reference.target = decode_locator(reader); }
            const auto count = reader.count();
            reference.items.reserve(count);
            for (std::size_t index = 0; index < count; ++index)
                reference.items.push_back(decode_reference(reader, depth + 1));
            return reference;
        }

        [[nodiscard]] TSCheckpointImage decode_ts(Reader &reader, std::size_t depth)
        {
            check_depth(depth);
            TSCheckpointImage image;
            const auto version = reader.number();
            if (version != TSCheckpointImage::current_version) { malformed("unsupported endpoint image version"); }
            image.version = static_cast<std::uint32_t>(version);
            image.schema = decode_ts_schema(reader, 0);
            image.last_modified_time = reader.time();
            image.payload = decode_value(reader);
            if (reader.boolean())
            {
                image.reference = decode_reference(reader, depth + 1);
                if (image.schema->kind != TSTypeKind::REF || image.payload.has_value())
                    malformed("reference metadata on a non-reference endpoint or beside a value payload");
                validate_ts_reference_checkpoint(*image.reference);
            }
            auto count = reader.count();
            image.window_times.reserve(count);
            for (std::size_t index = 0; index < count; ++index) { image.window_times.push_back(reader.time()); }
            const auto capacity = reader.number();
            if (capacity > std::numeric_limits<std::size_t>::max()) { malformed("slot capacity out of range"); }
            image.slot_capacity = static_cast<std::size_t>(capacity);
            image.key_set_last_modified_time = reader.time();
            count = reader.count();
            image.keys.reserve(count);
            for (std::size_t index = 0; index < count; ++index) { image.keys.push_back(decode_value(reader)); }
            count = reader.count();
            image.slots.reserve(count);
            for (std::size_t index = 0; index < count; ++index)
            {
                const auto slot = reader.number();
                if (slot > std::numeric_limits<std::size_t>::max()) { malformed("slot out of range"); }
                image.slots.push_back(static_cast<std::size_t>(slot));
            }
            count = reader.count();
            image.free_slots.reserve(count);
            for (std::size_t index = 0; index < count; ++index)
            {
                const auto slot = reader.number();
                if (slot > std::numeric_limits<std::size_t>::max()) { malformed("free slot out of range"); }
                image.free_slots.push_back(static_cast<std::size_t>(slot));
            }
            count = reader.count();
            image.published.reserve(count);
            for (std::size_t index = 0; index < count; ++index) { image.published.push_back(reader.boolean()); }
            count = reader.count();
            image.children.reserve(count);
            for (std::size_t index = 0; index < count; ++index) { image.children.push_back(decode_ts(reader, depth + 1)); }
            return image;
        }

        [[nodiscard]] GraphCheckpointImage decode_graph(Reader &reader, std::size_t depth)
        {
            check_depth(depth);
            GraphCheckpointImage graph;
            const auto count = reader.count();
            graph.nodes.reserve(count);
            for (std::size_t index = 0; index < count; ++index)
            {
                NodeCheckpointImage node;
                node.id = reader.text();
                node.signature = reader.text();
                for (auto *endpoint : {&node.output, &node.error, &node.recordable_state, &node.ingress})
                {
                    if (reader.boolean()) { *endpoint = decode_ts(reader, depth + 1); }
                }
                const auto alternatives = reader.count();
                node.alternatives.reserve(alternatives);
                for (std::size_t alternative = 0; alternative < alternatives; ++alternative)
                {
                    EndpointBindingCheckpoint saved;
                    saved.binding = decode_locator(reader);
                    saved.clocks = decode_ts(reader, depth + 1);
                    node.alternatives.push_back(std::move(saved));
                }
                const auto activities = reader.count();
                node.input_activity.reserve(activities);
                for (std::size_t activity_index = 0; activity_index < activities; ++activity_index)
                {
                    TSInputActivityEntry activity;
                    const auto length = reader.count();
                    activity.path.reserve(length);
                    for (std::size_t part = 0; part < length; ++part)
                    {
                        const auto slot = reader.number();
                        if (slot > std::numeric_limits<std::size_t>::max()) { malformed("input path out of range"); }
                        activity.path.push_back(static_cast<std::size_t>(slot));
                    }
                    const auto mode = reader.number();
                    if (mode != static_cast<std::uint8_t>(TSInputActivityMode::Value) &&
                        mode != static_cast<std::uint8_t>(TSInputActivityMode::Structural))
                    {
                        malformed("unsupported input activity mode");
                    }
                    activity.mode = static_cast<TSInputActivityMode>(mode);
                    node.input_activity.push_back(std::move(activity));
                }
                node.custom.payload = decode_value(reader);
                const auto endpoints = reader.count();
                node.custom.endpoints.reserve(endpoints);
                for (std::size_t endpoint = 0; endpoint < endpoints; ++endpoint)
                    node.custom.endpoints.push_back(decode_ts(reader, depth + 1));
                const auto children = reader.count();
                node.custom.children.reserve(children);
                for (std::size_t child_index = 0; child_index < children; ++child_index)
                {
                    ChildGraphCheckpoint child;
                    const auto slot = reader.number();
                    if (slot > std::numeric_limits<std::size_t>::max()) { malformed("child slot out of range"); }
                    child.slot = static_cast<std::size_t>(slot);
                    child.key = decode_value(reader);
                    child.key_last_modified_time = reader.time();
                    child.graph = std::make_shared<GraphCheckpointImage>(decode_graph(reader, depth + 1));
                    node.custom.children.push_back(std::move(child));
                }
                graph.nodes.push_back(std::move(node));
            }
            return graph;
        }

        [[nodiscard]] ComponentCheckpoint decode_checkpoint(std::string_view bytes, std::string_view parent)
        {
            Reader reader{bytes};
            if (reader.text() != format_name) { malformed("unsupported format"); }
            ComponentCheckpoint checkpoint;
            const auto version = reader.number();
            if (version != ComponentCheckpoint::current_version) { malformed("unsupported component image version"); }
            checkpoint.version = static_cast<std::uint32_t>(version);
            checkpoint.component_id = reader.text();
            checkpoint.graph_signature = reader.text();
            checkpoint.cut = reader.time();
            checkpoint.completed_until = reader.time();
            if (checkpoint.component_id.empty() || checkpoint.completed_until <= checkpoint.cut)
            {
                malformed("invalid completed component boundary");
            }
            if (reader.text() != parent) { malformed("inconsistent format metadata"); }
            checkpoint.graph = decode_graph(reader, 0);
            if (reader.offset != reader.bytes.size()) { malformed("trailing data"); }
            return checkpoint;
        }
        }  // namespace v1

        [[nodiscard]] Frame encode_checkpoint(const ComponentCheckpoint &checkpoint,
                                              std::string_view predecessor, bool verify)
        {
            std::string image;
            encode_component_checkpoint(checkpoint, image);
            if (verify)
            {
                // Byte equality of the canonical form is the one comparison
                // that holds for handle-identity values such as Frame.
                std::string again;
                encode_component_checkpoint(decode_component_checkpoint(image), again);
                if (again != image) { malformed("image does not survive its own codec"); }
            }
            arrow::LargeBinaryBuilder builder;
            check(builder.Append(image));
            std::shared_ptr<arrow::Array> array;
            check(builder.Finish(&array));
            const auto metadata = arrow::key_value_metadata(
                {std::string{format_key}, std::string{predecessor_key}},
                {std::string{format_name}, std::string{predecessor}});
            return Frame{arrow::Table::Make(
                arrow::schema({arrow::field("checkpoint", arrow::large_binary(), false)}, metadata),
                {std::move(array)})};
        }

        [[nodiscard]] ComponentCheckpoint decode_checkpoint(const Frame &frame)
        {
            if (!frame.has_value() || frame.table->num_columns() != 1 ||
                frame.table->num_rows() != 1 ||
                frame.table->field(0)->name() != "checkpoint" ||
                frame.table->field(0)->type()->id() != arrow::Type::LARGE_BINARY)
            {
                malformed("expected a single checkpoint image");
            }
            auto combined = frame.table->CombineChunks();
            if (!combined.ok()) { throw std::runtime_error(combined.status().ToString()); }
            const auto &array = static_cast<const arrow::LargeBinaryArray &>(
                *(*combined)->column(0)->chunk(0));
            if (array.IsNull(0)) { malformed("null image"); }
            const auto metadata = frame.table->schema()->metadata();
            if (!metadata) { malformed("missing format metadata"); }
            const auto format = metadata->Get(std::string{format_key});
            const auto parent = metadata->Get(std::string{predecessor_key});
            if (!format.ok() || !parent.ok()) { malformed("missing format metadata"); }
            const auto image = array.GetView(0);
            // The envelope names the format and the image must agree with it:
            // neither a relabelled envelope nor a substituted image is read
            // under the other format's rules.
            if (*format == v1::format_name) { return v1::decode_checkpoint(image, *parent); }
            if (*format != format_name) { malformed("unsupported format"); }
            if (!is_checkpoint_image(image)) { malformed("unsupported format"); }
            try { return decode_component_checkpoint(image); }
            catch (const std::exception &error)
            {
                malformed(error.what());
            }
        }
    }

    ComponentCheckpointStore::ComponentCheckpointStore(store::FrameStoreConfig config)
    {
        if (!config.immutable)
        {
            throw std::invalid_argument("component checkpoints require immutable storage");
        }
        frames_ = store::make_frame_store(std::move(config));
    }

    bool ComponentCheckpointStore::contains(std::string_view key) const
    {
        return frames_.contains(key);
    }

    ComponentCheckpoint ComponentCheckpointStore::read(std::string_view key) const
    {
        const auto frame = frames_.read(key);
        if (!frame.has_value())
        {
            throw std::runtime_error("component checkpoint not found: " + std::string{key});
        }
        return decode_checkpoint(frame);
    }

    void ComponentCheckpointStore::write(std::string_view key,
                                         const ComponentCheckpoint &checkpoint,
                                         std::optional<std::string_view> predecessor,
                                         bool verify) const
    {
        store::require_valid_key(key);
        if (predecessor)
        {
            store::require_valid_key(*predecessor);
            if (*predecessor == key) { throw std::invalid_argument("checkpoint cannot replace its predecessor"); }
            if (!frames_.contains(*predecessor))
            {
                throw std::runtime_error("checkpoint predecessor does not exist");
            }
        }
        // Encoding fails closed: a value the codec cannot represent throws
        // here, before anything is published.
        auto frame = encode_checkpoint(checkpoint, predecessor.value_or(""), verify);
        frames_.write(key, std::move(frame));
    }

    void configure_component_recovery(GlobalStateView state, const ComponentCheckpointStore &store,
                                      std::string component_id, std::string checkpoint_key,
                                      std::optional<std::string> restore_key, std::string revision)
    {
        store::require_valid_key(checkpoint_key);
        if (restore_key) { store::require_valid_key(*restore_key); }
        if (restore_key && *restore_key == checkpoint_key)
        {
            throw std::invalid_argument("checkpoint key must differ from restore key");
        }
        if (store.contains(checkpoint_key))
        {
            throw std::runtime_error("component checkpoint already exists: " + checkpoint_key);
        }
        ComponentRecoveryConfig config;
        config.component_id = std::move(component_id);
        config.revision = std::move(revision);
        if (restore_key)
        {
            config.load = [store, key = *restore_key]() -> std::optional<ComponentCheckpoint> {
                return store.read(key);
            };
        }
        config.commit = [store, key = std::move(checkpoint_key), predecessor = std::move(restore_key)](
                            const ComponentCheckpoint &checkpoint) {
            store.write(key, checkpoint,
                        predecessor ? std::optional<std::string_view>{*predecessor} : std::nullopt);
        };
        hgraph::configure_component_recovery(state, std::move(config));
    }
}
