#include <hgraph/lib/std/operators/impl/json_impl.h>
#include <hgraph/types/time_series/visitor.h>
#include <hgraph/types/value/visitor.h>

#include <fmt/format.h>
#include <simdjson.h>

#include <cstdint>
#include <limits>
#include <ostream>
#include <stdexcept>

namespace hgraph::stdlib::json_tree
{
    namespace
    {
        [[nodiscard]] std::string append_key(std::string_view pointer, const Str &key)
        {
            std::string out{pointer};
            out += '/';
            for (const char c : key)
            {
                if (c == '~') { out += "~0"; }
                else if (c == '/') { out += "~1"; }
                else { out += c; }
            }
            return out;
        }

        [[nodiscard]] std::string append_index(std::string_view pointer, Int index)
        {
            std::string out{pointer};
            out += '/';
            out += std::to_string(index);
            return out;
        }

    }  // namespace

    const ValueTypeMetaData *json_meta()
    {
        // Generation-checked per-thread cache: per-tick callers stay off the
        // registry mutex, and a (test-only) registry reset invalidates the
        // interned pointer this holds.
        thread_local const ValueTypeMetaData *meta{nullptr};
        thread_local std::uint64_t generation{0};
        const auto current = TypeRegistry::instance().reset_generation();
        if (meta == nullptr || generation != current)
        {
            meta       = TypeRegistry::instance().json();
            generation = current;
        }
        return meta;
    }

    ValueTypeRef json_value_binding()
    {
        thread_local ValueTypeRef binding{nullptr};
        thread_local std::uint64_t generation{0};
        const auto current = TypeRegistry::instance().reset_generation();
        if (!binding.bound() || generation != current)
        {
            binding    = ValuePlanFactory::instance().type_for(json_meta());
            generation = current;
        }
        return binding;
    }

    bool is_json_ts(const TSValueTypeMetaData *ts) noexcept
    {
        return ts != nullptr && ts->kind == TSTypeKind::TS && ts->value_schema == json_meta();
    }

    struct JsonValue::Document
    {
        explicit Document(Str text)
            : padded(text)
            , parser(std::make_unique<simdjson::dom::parser>())
        {
            auto parsed = parser->parse(padded);
            if (auto error = parsed.get(root))
            {
                throw std::invalid_argument(fmt::format("json_decode: {}", simdjson::error_message(error)));
            }
        }

        simdjson::padded_string                padded;
        std::unique_ptr<simdjson::dom::parser> parser;
        simdjson::dom::element                 root;
    };

    struct JsonValue::Access
    {
        [[nodiscard]] static std::optional<simdjson::dom::element> try_element(const JsonValue &value)
        {
            if (!value.document_) { return std::nullopt; }
            if (value.pointer_.empty()) { return value.document_->root; }
            simdjson::dom::element result;
            auto                   lookup = value.document_->root.at_pointer(value.pointer_);
            if (lookup.get(result)) { return std::nullopt; }
            return result;
        }

        [[nodiscard]] static simdjson::dom::element element(const JsonValue &value)
        {
            auto result = try_element(value);
            if (!result.has_value()) { throw std::invalid_argument("JSON tree: path does not exist"); }
            return *result;
        }

        [[nodiscard]] static JsonValue make(std::shared_ptr<Document> document, std::string pointer)
        {
            return JsonValue{std::move(document), std::move(pointer)};
        }
    };

    JsonValue::JsonValue(std::shared_ptr<Document> document, std::string pointer)
        : document_(std::move(document))
        , pointer_(std::move(pointer))
    {
    }

    const ValueTypeMetaData *json_lazy_meta()
    {
        // register_scalar is idempotent but takes the registry mutex and
        // re-registers the plan per call; generation-checked so a registry
        // reset re-registers.
        thread_local const ValueTypeMetaData *meta{nullptr};
        thread_local std::uint64_t generation{0};
        const auto current = TypeRegistry::instance().reset_generation();
        if (meta == nullptr || generation != current)
        {
            meta       = TypeRegistry::instance().register_scalar<JsonValue>("__hgraph_json_lazy");
            generation = current;
        }
        return meta;
    }

    const JsonValue *try_lazy(const ValueView &value)
    {
        return value.valid() && value.schema() == json_lazy_meta() ? &value.checked_as<JsonValue>() : nullptr;
    }

    Value lazy_value(JsonValue value)
    {
        static_cast<void>(json_lazy_meta());
        return Value{std::move(value)};
    }

    Value box(Value inner)
    {
        Value node{json_value_binding()};
        if (inner.has_value()) { MutableAnyView{node.begin_mutation()}.set(std::move(inner)); }
        return node;
    }

    Value to_node(const ValueView &value)
    {
        const auto *meta = value.schema();
        if (meta == json_meta()) { return Value{value}; }

        // JSON assigns a concrete meaning to an empty dynamic box. The
        // general value visitor rejects absence, so preserve that policy here
        // and let populated boxes recurse into their contained value.
        if (value.is_any())
        {
            auto boxed = value.as_any();
            return boxed.has_value() ? to_node(boxed.get()) : box(Value{});
        }

        const auto indexed_to_node = [](const IndexedValueView &values) {
            ListBuilder items{json_value_binding()};
            for (const auto element : values)
            {
                Value node = to_node(element);
                items.push_back_copy(node.view().data());
            }
            return box(items.build());
        };

        return visit(
            value,
            [](AtomicView scalar) {
                return box(Value{static_cast<const ValueView &>(scalar)});
            },
            [&](ListView values) { return indexed_to_node(values); },
            [&](TupleView values) { return indexed_to_node(values); },
            [](MapView values) {
                const ValueTypeRef str_binding = TypeRegistry::instance().scalar_type<Str>();
                MapBuilder entries{str_binding, json_value_binding()};
                for (const auto [key, item] : values)
                {
                    Value       key_text;
                    const auto *key_meta = key.schema();
                    if (key_meta == scalar_descriptor<Str>::value_meta()) { key_text = Value{key}; }
                    else if (key_meta == scalar_descriptor<Int>::value_meta())
                    {
                        key_text = Value{Str{fmt::format("{}", key.checked_as<Int>())}};
                    }
                    else if (key_meta == scalar_descriptor<Float>::value_meta())
                    {
                        key_text = Value{Str{fmt::format("{}", key.checked_as<Float>())}};
                    }
                    else if (key_meta == scalar_descriptor<Bool>::value_meta())
                    {
                        key_text = Value{Str{key.checked_as<Bool>() ? "true" : "false"}};
                    }
                    else { throw std::invalid_argument("JSON tree: object keys must be strings"); }
                    Value node = to_node(item);
                    entries.set_item_copy(key_text.view().data(), node.view().data());
                }
                return box(entries.build());
            },
            [](ValueView) -> Value {
                throw std::invalid_argument("JSON tree: unsupported value kind");
            });
    }

    ValueView unbox(const ValueView &node)
    {
        auto any = node.as_any();
        return any.has_value() ? any.get() : ValueView{};
    }

    Value concatenate_arrays(const ValueView &lhs, const ValueView &rhs)
    {
        Value lhs_materialized;
        Value rhs_materialized;
        const auto array_value = [](const ValueView &node, Value &materialized,
                                    std::string_view side) -> ValueView {
            ValueView inner = unbox(node);
            if (const auto *lazy = try_lazy(inner))
            {
                materialized = lazy->materialize();
                inner = unbox(materialized.view());
            }
            if (!inner.valid() || inner.schema()->value_kind() != ValueTypeKind::List)
            {
                const std::string_view actual = inner.valid() ? inner.schema()->name() : "null";
                throw std::invalid_argument(fmt::format(
                    "JSON array concatenation requires array operands; {} operand is {}",
                    side, actual));
            }
            return inner;
        };

        const ValueView lhs_array = array_value(lhs, lhs_materialized, "left");
        const ValueView rhs_array = array_value(rhs, rhs_materialized, "right");
        ListBuilder result{json_value_binding()};
        for (const ValueView item : lhs_array.as_list())
        {
            result.push_back_copy(item.data());
        }
        for (const ValueView item : rhs_array.as_list())
        {
            result.push_back_copy(item.data());
        }
        return box(result.build());
    }

    bool equals(const ValueView &lhs, const ValueView &rhs)
    {
        auto lhs_inner = unbox(lhs);
        auto rhs_inner = unbox(rhs);
        if (!lhs_inner.valid() || !rhs_inner.valid()) { return !lhs_inner.valid() && !rhs_inner.valid(); }

        const auto *lhs_lazy = try_lazy(lhs_inner);
        const auto *rhs_lazy = try_lazy(rhs_inner);
        if (lhs_lazy != nullptr && rhs_lazy != nullptr) { return *lhs_lazy == *rhs_lazy; }
        if (lhs_lazy != nullptr)
        {
            Value materialized = lhs_lazy->materialize();
            return equals(materialized.view(), rhs);
        }
        if (rhs_lazy != nullptr)
        {
            Value materialized = rhs_lazy->materialize();
            return equals(lhs, materialized.view());
        }
        return lhs.equals(rhs);
    }

    std::partial_ordering compare(const ValueView &lhs, const ValueView &rhs)
    {
        if (equals(lhs, rhs)) { return std::partial_ordering::equivalent; }

        auto lhs_inner = unbox(lhs);
        auto rhs_inner = unbox(rhs);
        if (!lhs_inner.valid() || !rhs_inner.valid())
        {
            return lhs_inner.valid() ? std::partial_ordering::greater : std::partial_ordering::less;
        }

        if (const auto *lhs_lazy = try_lazy(lhs_inner))
        {
            Value materialized = lhs_lazy->materialize();
            return compare(materialized.view(), rhs);
        }
        if (const auto *rhs_lazy = try_lazy(rhs_inner))
        {
            Value materialized = rhs_lazy->materialize();
            return compare(lhs, materialized.view());
        }
        return lhs.compare(rhs);
    }

    void encode(const ValueView &node, std::string &out)
    {
        auto inner = unbox(node);
        if (!inner.valid()) { out += "null"; return; }
        if (const auto *lazy = try_lazy(inner))
        {
            out += lazy->encode();
            return;
        }
        const auto *meta = inner.schema();
        if (meta == scalar_descriptor<Bool>::value_meta())
        {
            out += inner.checked_as<Bool>() ? "true" : "false";
            return;
        }
        if (meta == scalar_descriptor<Int>::value_meta())
        {
            out += fmt::format("{}", inner.checked_as<Int>());
            return;
        }
        if (meta == scalar_descriptor<Float>::value_meta())
        {
            out += fmt::format("{}", inner.checked_as<Float>());
            return;
        }
        if (meta == scalar_descriptor<Str>::value_meta())
        {
            json_detail::append_escaped(inner.checked_as<Str>(), out);
            return;
        }
        if (meta->value_kind() == ValueTypeKind::List)
        {
            out += '[';
            bool first = true;
            const auto values = inner.as_list();
            for (const auto element : values)
            {
                if (!first) { out += ", "; }
                first = false;
                encode(element, out);
            }
            out += ']';
            return;
        }
        if (meta->value_kind() == ValueTypeKind::Map)
        {
            out += '{';
            bool first = true;
            const auto values = inner.as_map();
            for (const auto [key, item] : values)
            {
                if (!first) { out += ", "; }
                first = false;
                json_detail::append_escaped(key.checked_as<Str>(), out);
                out += ": ";
                encode(item, out);
            }
            out += '}';
            return;
        }
        throw std::invalid_argument("JSON tree: unsupported node content");
    }

    [[nodiscard]] Value value_from_simdjson(simdjson::dom::element element)
    {
        switch (element.type())
        {
            case simdjson::dom::element_type::NULL_VALUE:
                return box(Value{});
            case simdjson::dom::element_type::BOOL:
                return box(Value{Bool{static_cast<bool>(element)}});
            case simdjson::dom::element_type::INT64:
                return box(Value{static_cast<Int>(static_cast<std::int64_t>(element))});
            case simdjson::dom::element_type::UINT64: {
                const auto value = static_cast<std::uint64_t>(element);
                if (value > static_cast<std::uint64_t>(std::numeric_limits<Int>::max()))
                {
                    throw std::invalid_argument("JSON tree: unsigned integer is too large for Int");
                }
                return box(Value{static_cast<Int>(value)});
            }
            case simdjson::dom::element_type::DOUBLE:
                return box(Value{static_cast<Float>(static_cast<double>(element))});
            case simdjson::dom::element_type::STRING:
                return box(Value{Str{std::string_view(element)}});
            case simdjson::dom::element_type::ARRAY: {
                ListBuilder items{json_value_binding()};
                for (simdjson::dom::element child : simdjson::dom::array(element))
                {
                    Value node = value_from_simdjson(child);
                    items.push_back_copy(node.view().data());
                }
                return box(items.build());
            }
            case simdjson::dom::element_type::OBJECT: {
                MapBuilder entries{ValuePlanFactory::instance().type_for(
                                       scalar_descriptor<Str>::value_meta()),
                                   json_value_binding()};
                for (simdjson::dom::key_value_pair field : simdjson::dom::object(element))
                {
                    Value key{Str{std::string_view(field.key)}};
                    Value node = value_from_simdjson(field.value);
                    entries.set_item_copy(key.view().data(), node.view().data());
                }
                return box(entries.build());
            }
            case simdjson::dom::element_type::BIGINT:
                throw std::invalid_argument("JSON tree: bigint is not supported");
        }
        throw std::invalid_argument("JSON tree: unsupported simdjson element");
    }

    void encode_simdjson(simdjson::dom::element element, std::string &out)
    {
        switch (element.type())
        {
            case simdjson::dom::element_type::NULL_VALUE:
                out += "null";
                return;
            case simdjson::dom::element_type::BOOL:
                out += static_cast<bool>(element) ? "true" : "false";
                return;
            case simdjson::dom::element_type::INT64:
                out += fmt::format("{}", static_cast<std::int64_t>(element));
                return;
            case simdjson::dom::element_type::UINT64:
                out += fmt::format("{}", static_cast<std::uint64_t>(element));
                return;
            case simdjson::dom::element_type::DOUBLE:
                out += fmt::format("{}", static_cast<double>(element));
                return;
            case simdjson::dom::element_type::STRING:
                json_detail::append_escaped(std::string_view(element), out);
                return;
            case simdjson::dom::element_type::ARRAY: {
                out += '[';
                bool first = true;
                for (simdjson::dom::element child : simdjson::dom::array(element))
                {
                    if (!first) { out += ", "; }
                    first = false;
                    encode_simdjson(child, out);
                }
                out += ']';
                return;
            }
            case simdjson::dom::element_type::OBJECT: {
                out += '{';
                bool first = true;
                for (simdjson::dom::key_value_pair field : simdjson::dom::object(element))
                {
                    if (!first) { out += ", "; }
                    first = false;
                    json_detail::append_escaped(std::string_view(field.key), out);
                    out += ": ";
                    encode_simdjson(field.value, out);
                }
                out += '}';
                return;
            }
            case simdjson::dom::element_type::BIGINT:
                throw std::invalid_argument("JSON tree: bigint is not supported");
        }
        throw std::invalid_argument("JSON tree: unsupported simdjson element");
    }

    [[nodiscard]] bool simdjson_equal(simdjson::dom::element lhs, simdjson::dom::element rhs)
    {
        const auto lhs_type = lhs.type();
        if (lhs_type != rhs.type()) { return false; }
        switch (lhs_type)
        {
            case simdjson::dom::element_type::NULL_VALUE:
                return true;
            case simdjson::dom::element_type::BOOL:
                return static_cast<bool>(lhs) == static_cast<bool>(rhs);
            case simdjson::dom::element_type::INT64:
                return static_cast<std::int64_t>(lhs) == static_cast<std::int64_t>(rhs);
            case simdjson::dom::element_type::UINT64:
                return static_cast<std::uint64_t>(lhs) == static_cast<std::uint64_t>(rhs);
            case simdjson::dom::element_type::DOUBLE:
                return static_cast<double>(lhs) == static_cast<double>(rhs);
            case simdjson::dom::element_type::STRING:
                return std::string_view(lhs) == std::string_view(rhs);
            case simdjson::dom::element_type::ARRAY: {
                auto lhs_array = simdjson::dom::array(lhs);
                auto rhs_array = simdjson::dom::array(rhs);
                if (lhs_array.size() != rhs_array.size()) { return false; }
                auto rhs_it = rhs_array.begin();
                for (simdjson::dom::element lhs_child : lhs_array)
                {
                    if (rhs_it == rhs_array.end()) { return false; }
                    if (!simdjson_equal(lhs_child, *rhs_it)) { return false; }
                    ++rhs_it;
                }
                return true;
            }
            case simdjson::dom::element_type::OBJECT: {
                auto lhs_object = simdjson::dom::object(lhs);
                auto rhs_object = simdjson::dom::object(rhs);
                if (lhs_object.size() != rhs_object.size()) { return false; }
                for (simdjson::dom::key_value_pair lhs_field : lhs_object)
                {
                    bool found = false;
                    for (simdjson::dom::key_value_pair rhs_field : rhs_object)
                    {
                        if (std::string_view(lhs_field.key) == std::string_view(rhs_field.key))
                        {
                            if (!simdjson_equal(lhs_field.value, rhs_field.value)) { return false; }
                            found = true;
                            break;
                        }
                    }
                    if (!found) { return false; }
                }
                return true;
            }
            case simdjson::dom::element_type::BIGINT:
                return false;
        }
        return false;
    }

    JsonValue JsonValue::parse(Str text)
    {
        return Access::make(std::make_shared<Document>(std::move(text)), std::string{});
    }

    Value JsonValue::materialize() const
    {
        if (materialized_) { return *materialized_; }
        Value result = value_from_simdjson(Access::element(*this));
        materialized_ = std::make_shared<Value>(result);
        return result;
    }

    std::string JsonValue::encode() const
    {
        std::string out;
        encode_simdjson(Access::element(*this), out);
        return out;
    }

    std::optional<JsonValue> JsonValue::child(Str key) const
    {
        if (!document_) { return std::nullopt; }
        JsonValue candidate = Access::make(document_, append_key(pointer_, key));
        return Access::try_element(candidate).has_value() ? std::optional<JsonValue>{std::move(candidate)}
                                                          : std::nullopt;
    }

    std::optional<JsonValue> JsonValue::child(Int index) const
    {
        if (!document_) { return std::nullopt; }
        Int resolved = index;
        if (resolved < 0)
        {
            auto current = Access::try_element(*this);
            if (!current.has_value()) { return std::nullopt; }
            if (current->type() != simdjson::dom::element_type::ARRAY) { return std::nullopt; }
            const auto size = simdjson::dom::array(*current).size();
            resolved += static_cast<Int>(size);
        }
        if (resolved < 0) { return std::nullopt; }
        JsonValue candidate = Access::make(document_, append_index(pointer_, resolved));
        return Access::try_element(candidate).has_value() ? std::optional<JsonValue>{std::move(candidate)}
                                                          : std::nullopt;
    }

    std::optional<Int> JsonValue::as_int() const
    {
        auto current = Access::try_element(*this);
        if (!current.has_value()) { return std::nullopt; }
        try
        {
            if (current->type() == simdjson::dom::element_type::INT64)
            {
                return static_cast<Int>(static_cast<std::int64_t>(*current));
            }
            if (current->type() == simdjson::dom::element_type::UINT64)
            {
                const auto value = static_cast<std::uint64_t>(*current);
                if (value <= static_cast<std::uint64_t>(std::numeric_limits<Int>::max()))
                {
                    return static_cast<Int>(value);
                }
            }
        }
        catch (...) { return std::nullopt; }
        return std::nullopt;
    }

    std::optional<Float> JsonValue::as_float() const
    {
        auto current = Access::try_element(*this);
        if (!current.has_value()) { return std::nullopt; }
        try
        {
            if (current->type() == simdjson::dom::element_type::DOUBLE)
            {
                return static_cast<Float>(static_cast<double>(*current));
            }
            if (current->type() == simdjson::dom::element_type::INT64)
            {
                return static_cast<Float>(static_cast<std::int64_t>(*current));
            }
            if (current->type() == simdjson::dom::element_type::UINT64)
            {
                return static_cast<Float>(static_cast<std::uint64_t>(*current));
            }
        }
        catch (...) { return std::nullopt; }
        return std::nullopt;
    }

    std::optional<Str> JsonValue::as_str() const
    {
        auto current = Access::try_element(*this);
        if (!current.has_value()) { return std::nullopt; }
        try
        {
            if (current->type() == simdjson::dom::element_type::STRING)
            {
                return Str{std::string_view(*current)};
            }
        }
        catch (...) { return std::nullopt; }
        return std::nullopt;
    }

    std::optional<Bool> JsonValue::as_bool() const
    {
        auto current = Access::try_element(*this);
        if (!current.has_value()) { return std::nullopt; }
        try
        {
            if (current->type() == simdjson::dom::element_type::BOOL)
            {
                return Bool{static_cast<bool>(*current)};
            }
        }
        catch (...) { return std::nullopt; }
        return std::nullopt;
    }

    std::size_t JsonValue::hash() const noexcept
    {
        try { return materialize().hash(); }
        catch (...) { return 0; }
    }

    bool JsonValue::operator==(const JsonValue &other) const noexcept
    {
        try
        {
            auto lhs = Access::try_element(*this);
            auto rhs = Access::try_element(other);
            if (lhs.has_value() && rhs.has_value()) { return simdjson_equal(*lhs, *rhs); }
            return materialize().equals(other.materialize());
        }
        catch (...) { return false; }
    }

    std::partial_ordering JsonValue::operator<=>(const JsonValue &other) const noexcept
    {
        try { return materialize().compare(other.materialize()); }
        catch (...) { return std::partial_ordering::unordered; }
    }

    std::ostream &operator<<(std::ostream &out, const JsonValue &value)
    {
        try { out << value.encode(); }
        catch (...) { out << "<invalid JSON>"; }
        return out;
    }

    void publish(const TSOutputView &out, Value &&node)
    {
        auto mutation = out.begin_mutation(out.evaluation_time());
        static_cast<void>(mutation.move_value_from(std::move(node)));
    }
}  // namespace hgraph::stdlib::json_tree

std::size_t std::hash<hgraph::stdlib::json_tree::JsonValue>::operator()(
    const hgraph::stdlib::json_tree::JsonValue &value) const noexcept
{
    return value.hash();
}

namespace hgraph::stdlib
{
    namespace json_ts_detail
    {
        /** One node per TS-shape level: every converter the walkers need,
            resolved once at start (converter lookup takes the counted
            converter-interning mutex). ``children``: TSD/TSL hold the single
            (uniform) element plan; TSB holds one per field. */
        struct TsJsonPlan
        {
            BoundJsonConverter        value{};    // leaf / whole-value
            BoundJsonConverter        element{};  // TSS element
            BoundJsonConverter        delta{};    // TSL index-map delta
            BoundJsonConverter        key{};      // TSD key
            const ValueTypeMetaData  *key_meta{nullptr};
            std::vector<std::unique_ptr<TsJsonPlan>> children{};
        };

        TsJsonPlan *build_ts_json_plan(const TSValueTypeMetaData *schema)
        {
            auto plan = std::make_unique<TsJsonPlan>();
            switch (schema->kind)
            {
                case TSTypeKind::TSD: {
                    const auto *dict = time_series_schema_as<AnyTSD>(schema);
                    plan->key_meta   = dict->key_type();
                    plan->key        = bind_json_converter(plan->key_meta);
                    plan->children.emplace_back(build_ts_json_plan(dict->element_ts()));
                    break;
                }
                case TSTypeKind::TSS: {
                    plan->value   = bind_json_converter(schema->value_schema);
                    plan->element = bind_json_converter(
                        schema->value_schema->element_type);
                    break;
                }
                case TSTypeKind::TSL: {
                    const auto *list = time_series_schema_as<AnyTSL>(schema);
                    plan->value      = bind_json_converter(schema->value_schema);
                    plan->delta      = bind_json_converter(
                        schema->delta_value_schema);
                    plan->children.emplace_back(build_ts_json_plan(list->element_ts()));
                    break;
                }
                case TSTypeKind::TSB: {
                    // write_ts_delta recurses the fields; apply_ts_json
                    // treats a TSB as a whole-value leaf — carry both.
                    const auto *bundle = time_series_schema_as<AnyTSB>(schema);
                    plan->value = bind_json_converter(schema->value_schema);
                    for (std::size_t index = 0; index < bundle->field_count(); ++index)
                    {
                        plan->children.emplace_back(
                            build_ts_json_plan(bundle->fields()[index].type));
                    }
                    break;
                }
                default: {
                    plan->value = bind_json_converter(schema->value_schema);
                    break;
                }
            }
            return plan.release();
        }

        void free_ts_json_plan(TsJsonPlan *plan) noexcept { delete plan; }

        namespace
        {
            void write_json_key(const BoundJsonConverter &converter,
                                const ValueView &key,
                                std::string &out)
            {
                // Keys are STRINGIFIED (python json.dumps): a Str renders as
                // its JSON string; other scalars render then quote.
                std::string rendered;
                converter.write(key, rendered);
                if (!rendered.empty() && rendered.front() == '"') { out += rendered; }
                else
                {
                    out.push_back('"');
                    out += rendered;
                    out.push_back('"');
                }
            }

            void write_separator(bool &first, std::string &out)
            {
                if (!first) { out += ", "; }
                first = false;
            }
        }  // namespace

        void write_ts_delta(const TSInputView &ts, const TsJsonPlan &plan, std::string &out)
        {
            visit(
                ts,
                [&](TSDInputView dict) {
                    bool first = true;
                    out.push_back('{');
                    for (const ValueView key : dict.removed_keys())
                    {
                        write_separator(first, out);
                        write_json_key(plan.key, key, out);
                        out += ": null";
                    }
                    for (auto &&[key, child] : dict.modified_items())
                    {
                        write_separator(first, out);
                        write_json_key(plan.key, key, out);
                        out += ": ";
                        write_ts_delta(child, *plan.children[0], out);
                    }
                    out.push_back('}');
                },
                [&](TSSInputView set) {
                    const auto &element = plan.element;
                    std::string added;
                    std::string removed;
                    bool        any_added   = false;
                    bool        any_removed = false;
                    for (const ValueView value : set.added())
                    {
                        if (any_added) { added += ", "; }
                        any_added = true;
                        element.write(value, added);
                    }
                    for (const ValueView value : set.removed())
                    {
                        if (any_removed) { removed += ", "; }
                        any_removed = true;
                        element.write(value, removed);
                    }
                    bool first = true;
                    out.push_back('{');
                    if (any_added)
                    {
                        write_separator(first, out);
                        out += "\"added\": [";
                        out += added;
                        out.push_back(']');
                    }
                    if (any_removed)
                    {
                        write_separator(first, out);
                        out += "\"removed\": [";
                        out += removed;
                        out.push_back(']');
                    }
                    out.push_back('}');
                },
                [&](TSLInputView list) {
                    bool first = true;
                    out.push_back('{');
                    for (std::size_t index = 0; index < list.size(); ++index)
                    {
                        auto child = list[index];
                        if (!child.modified()) { continue; }
                        write_separator(first, out);
                        out += fmt::format("\"{}\": ", index);
                        write_ts_delta(child, *plan.children[0], out);
                    }
                    out.push_back('}');
                },
                [&](TSBInputView bundle) {
                    bool first = true;
                    out.push_back('{');
                    for (std::size_t index = 0; index < bundle.size(); ++index)
                    {
                        auto child = bundle.at(index);
                        if (!child.modified()) { continue; }
                        write_separator(first, out);
                        out += fmt::format("\"{}\": ", bundle.schema()->fields()[index].name);
                        write_ts_delta(child, *plan.children[index], out);
                    }
                    out.push_back('}');
                },
                [&](TSInputView leaf) { plan.value.write(leaf.value(), out); });
        }

        void apply_ts_json(const TSOutputView &out, const TsJsonPlan &plan,
                           json_fragment::Cursor &cursor)
        {
            visit(
                out,
                [&](TSDOutputView dict) {
                    if (!json_fragment::consume(cursor, '{'))
                    {
                        json_fragment::fail(cursor, "expected '{' for a TSD");
                    }
                    auto mutation = dict.begin_mutation(dict.evaluation_time());
                    if (json_fragment::consume(cursor, '}')) { return; }
                    const bool string_key =
                        plan.key_meta == scalar_descriptor<Str>::value_meta();
                    while (true)
                    {
                        const std::string key_text = json_fragment::parse_string(cursor);
                        const Value       key =
                            string_key ? Value{Str{key_text}}
                                       : from_json_string(plan.key, key_text);
                        if (!json_fragment::consume(cursor, ':'))
                        {
                            json_fragment::fail(cursor, "expected ':' after a TSD key");
                        }
                        if (json_fragment::consume_null(cursor)) { static_cast<void>(mutation.erase(key.view())); }
                        else
                        {
                            auto element = mutation.at(key.view());
                            apply_ts_json(
                                TSOutputView{dict.base().output(), element, dict.evaluation_time()},
                                *plan.children[0], cursor);
                        }
                        if (json_fragment::consume(cursor, ',')) { continue; }
                        if (json_fragment::consume(cursor, '}')) { break; }
                        json_fragment::fail(cursor, "expected ',' or '}' in a TSD object");
                    }
                },
                [&](TSSOutputView set) {
                    if (json_fragment::peek(cursor) == '[')
                    {
                        // A bare array replaces the whole membership.
                        const Value parsed = json_fragment::parse_value(
                            plan.value, cursor);
                        apply_current_value(set.base(), parsed.view());
                        return;
                    }
                    if (!json_fragment::consume(cursor, '{'))
                    {
                        json_fragment::fail(cursor, "expected '{' or '[' for a TSS");
                    }
                    auto        mutation = set.begin_mutation(set.evaluation_time());
                    const auto &element  = plan.element;
                    if (json_fragment::consume(cursor, '}')) { return; }
                    while (true)
                    {
                        const std::string part = json_fragment::parse_string(cursor);
                        if (!json_fragment::consume(cursor, ':') || !json_fragment::consume(cursor, '['))
                        {
                            json_fragment::fail(cursor, "expected an added/removed array");
                        }
                        if (!json_fragment::consume(cursor, ']'))
                        {
                            while (true)
                            {
                                const Value value = json_fragment::parse_value(element, cursor);
                                if (part == "added") { static_cast<void>(mutation.add(value.view())); }
                                else { static_cast<void>(mutation.remove(value.view())); }
                                if (json_fragment::consume(cursor, ',')) { continue; }
                                if (json_fragment::consume(cursor, ']')) { break; }
                                json_fragment::fail(cursor, "expected ',' or ']' in a set delta");
                            }
                        }
                        if (json_fragment::consume(cursor, ',')) { continue; }
                        if (json_fragment::consume(cursor, '}')) { break; }
                        json_fragment::fail(cursor, "expected ',' or '}' in a set delta");
                    }
                },
                [&](TSLOutputView list) {
                    if (json_fragment::peek(cursor) == '[')
                    {
                        const Value parsed = json_fragment::parse_value(
                            plan.value, cursor);
                        apply_current_value(list.base(), parsed.view());
                        return;
                    }
                    // The index-object form IS the canonical TSL delta (an
                    // index map); its converter reads quoted keys.
                    const Value parsed = json_fragment::parse_value(
                        plan.delta, cursor);
                    apply_delta(list.base(), parsed.view());
                },
                [&](TSOutputView leaf) {
                    const Value parsed = json_fragment::parse_value(
                        plan.value, cursor);
                    apply_current_value(leaf, parsed.view());
                });
        }
    }  // namespace json_ts_detail

    void register_json_operators()
    {
        register_overload<json_object_, json_object_impl>();
        register_overload<json_array_, json_array_impl>();
        register_graph_overload<combine_json, combine_json_impl>();
        register_graph_overload<combine_json, combine_json_array_impl>();
        register_overload<add_, add_json_arrays_impl>();
        register_overload<json_encode, json_encode_impl>();
        register_overload<json_encode, json_encode_bytes_impl>();
        register_overload<json_decode, json_decode_impl>();
        register_overload<json_decode, json_decode_bytes_impl>();
        register_overload<getitem_, getitem_json_impl<true>>();
        register_overload<getitem_, getitem_json_impl<false>>();
        register_overload<json_as_int, json_as_impl<Int>>();
        register_overload<json_as_float, json_as_impl<Float>>();
        register_overload<json_as_str, json_as_impl<Str>>();
        register_overload<json_as_bool, json_as_impl<Bool>>();
        register_overload<to_json, to_json_value_impl>();
        register_overload<to_json, to_json_delta_impl>();
        register_overload<from_json, from_json_impl>();
    }
}  // namespace hgraph::stdlib
