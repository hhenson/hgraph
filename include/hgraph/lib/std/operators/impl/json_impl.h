#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_JSON_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_JSON_IMPL_H

#include <hgraph/lib/std/operators/impl/higher_order_impl.h>
#include <hgraph/types/operator_type_resolution.h>
#include <hgraph/lib/std/operators/container.h>
#include <hgraph/lib/std/operators/json.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/time_series/ts_delta.h>
#include <hgraph/types/value/json_codec.h>

#include <cstddef>
#include <iosfwd>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace hgraph::static_schema_detail
{
    template <>
    struct scalar_name<JsonCodecState>
    {
        static constexpr std::string_view value{"JsonCodecState"};
    };
}  // namespace hgraph::static_schema_detail

namespace hgraph::stdlib
{
    using namespace hgraph::operator_type_resolution;

    /**
     * ``to_json`` — erased implementations over any time-series. The composed
     * ``JsonConverter`` is resolved ONCE in ``start`` and carried in node
     * State (the lifecycle form of the builder pattern); ``eval`` is a plain
     * converter invocation. The ``delta`` flag is a wiring-time constant, so
     * value vs delta is resolved by OVERLOAD SELECTION (``requires_`` on the
     * flag) — no hot-path branches.
     */
    namespace json_ts_detail
    {
        /** hgraph's FRIENDLY JSON delta form, recursively over the TS shape:
            TSD = {key: child-delta | null}, TSS = {added/removed arrays,
            empty sides omitted}, TSL = {index: child-delta}, TSB = modified
            fields, leaves = the value. */
        void write_ts_delta(const TSInputView &ts, std::string &out);

        /** The inverse: parse ``cursor`` per the OUTPUT's TS shape and apply
            (a leading '[' on TSS/TSL is a whole-value replacement). */
        void apply_ts_json(const TSOutputView &out, json_fragment::Cursor &cursor);
    }  // namespace json_ts_detail

    struct to_json_value_impl
    {
        static constexpr auto name = "to_json";

        static std::vector<std::pair<std::string_view, Value>> defaults()
        {
            return {{"delta", Value{Bool{false}}}};
        }

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            const auto *delta = context.scalar_as<Bool>("delta");
            return delta != nullptr && !*delta;
        }

        static void start(In<"ts", TsVar<"S">> ts, State<JsonCodecState> codec)
        {
            codec.set(JsonCodecState{&json_converter(ts.base().schema()->value_schema)});
        }

        static void eval(In<"ts", TsVar<"S">> ts, Scalar<"delta", Bool> delta, State<JsonCodecState> codec,
                         Out<TS<Str>> out)
        {
            static_cast<void>(delta);   // resolved at wiring; always false here
            std::string text;
            codec.get().converter->write(ts.value(), text);
            out.set(std::move(text));
        }
    };

    /** ``to_json(ts, delta=true)`` — serialises the canonical per-tick delta. */
    struct to_json_delta_impl
    {
        static constexpr auto name = "to_json";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            const auto *delta = context.scalar_as<Bool>("delta");
            return delta != nullptr && *delta;
        }

        static void eval(In<"ts", TsVar<"S">> ts, Scalar<"delta", Bool> delta, Out<TS<Str>> out)
        {
            static_cast<void>(delta);   // resolved at wiring; always true here
            std::string text;
            json_ts_detail::write_ts_delta(ts.base(), text);
            out.set(std::move(text));
        }
    };

    /**
     * ``from_json`` — parses the incoming string into the resolved output's
     * VALUE schema (converter resolved once in ``start``) and applies the
     * parsed value as the tick's delta. The output type is supplied at the
     * wiring site: ``wire<from_json, TS<MySchema>>(w, ts)``.
     */
    struct from_json_impl
    {
        static constexpr auto name = "from_json";

        static void eval(In<"ts", TS<Str>> ts, Out<TsVar<"O">> out)
        {
            const Str &text = ts.value();
            json_fragment::Cursor cursor{std::string_view{text}, 0};
            json_ts_detail::apply_ts_json(static_cast<const TSOutputView &>(out), cursor);
        }
    };


    // -----------------------------------------------------------------
    // Dynamic-JSON value tree (ruling 2026-07-06, parity_matrix.rst):
    // JSON = an Any-storage value holding Bool | Int | Float | Str |
    // List<JSON> | Map<Str, JSON>; an EMPTY box is JSON null. Erased
    // C++ operators are the PRIMARY API; python is sugar.
    // -----------------------------------------------------------------

    namespace json_tree
    {
        [[nodiscard]] ValueTypeRef json_value_binding();

        class JsonValue
        {
          public:
            JsonValue() = default;

            [[nodiscard]] static JsonValue parse(Str text);

            [[nodiscard]] std::optional<JsonValue> child(Str key) const;
            [[nodiscard]] std::optional<JsonValue> child(Int index) const;
            [[nodiscard]] std::optional<Int>       as_int() const;
            [[nodiscard]] std::optional<Float>     as_float() const;
            [[nodiscard]] std::optional<Str>       as_str() const;
            [[nodiscard]] std::optional<Bool>      as_bool() const;
            [[nodiscard]] Value                    materialize() const;
            [[nodiscard]] std::string              encode() const;
            [[nodiscard]] std::size_t              hash() const noexcept;

            [[nodiscard]] bool operator==(const JsonValue &other) const noexcept;
            [[nodiscard]] std::partial_ordering operator<=>(const JsonValue &other) const noexcept;

          private:
            struct Document;
            struct Access;

            JsonValue(std::shared_ptr<Document> document, std::string pointer);

            std::shared_ptr<Document>  document_{};
            std::string                pointer_{};
            mutable std::shared_ptr<const Value> materialized_{};

            friend struct Access;
        };

        std::ostream &operator<<(std::ostream &out, const JsonValue &value);
    }  // namespace json_tree

    inline void resolve_json_output(ResolutionMap &resolution,
                                    std::string_view local_var = "O",
                                    bool graph_output = false)
    {
        const TSValueTypeMetaData *output = TypeRegistry::instance().ts(json_tree::json_meta());
        if (graph_output)
        {
            if (output_bound(resolution)) { return; }
            bind_output(resolution, output, local_var);
            return;
        }
        if (local_output_bound(resolution, local_var)) { return; }
        bind_local_output(resolution, output, local_var);
    }
}  // namespace hgraph::stdlib

template <>
struct std::hash<hgraph::stdlib::json_tree::JsonValue>
{
    std::size_t operator()(const hgraph::stdlib::json_tree::JsonValue &value) const noexcept;
};

namespace hgraph::stdlib
{
    using namespace hgraph::operator_type_resolution;

    namespace json_tree
    {
        [[nodiscard]] const ValueTypeMetaData *json_lazy_meta();
        [[nodiscard]] const JsonValue         *try_lazy(const ValueView &value);
        [[nodiscard]] Value                    lazy_value(JsonValue value);

        /** Box an inner value into a JSON node (empty inner = null). */
        [[nodiscard]] Value box(Value inner);

        /** Convert an arbitrary VALUE into a JSON node (recursive). */
        [[nodiscard]] Value     to_node(const ValueView &value);
        [[nodiscard]] ValueView unbox(const ValueView &node);
        void                    encode(const ValueView &node, std::string &out);
        void                    publish(const TSOutputView &out, Value &&node);
    }  // namespace json_tree

    /** Runtime node behind ``combine_json``: an un-named TSB of values +
        the parallel key names -> a JSON object node. */
    struct json_object_impl
    {
        static constexpr auto name = "__json_object";

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext)
        {
            resolve_json_output(resolution);
        }

        static void eval(In<"values", TsVar<"V">> values, Scalar<"keys", ScalarVar<"K">> keys, Out<TsVar<"O">> out)
        {
            // keys is a tuple[str, ...] scalar parallel to the value bundle.
            auto       names = keys.value().as_list();
            MapBuilder entries{ValuePlanFactory::instance().type_for(
                                   scalar_descriptor<Str>::value_meta()),
                               json_tree::json_value_binding()};
            const auto count = values.base().schema()->field_count();
            for (std::size_t index = 0; index < count && index < names.size(); ++index)
            {
                auto child = values.base().indexed_child_at(index);
                if (!child.valid()) { continue; }
                Value node = json_tree::to_node(child.value());
                entries.set_item_copy(names.at(index).data(), node.view().data());
            }
            json_tree::publish(static_cast<const TSOutputView &>(out), json_tree::box(entries.build()));
        }
    };

    /** combine_json(**kwargs) — composes the value bundle + key list onto
        the runtime node. */
    struct combine_json_impl
    {
        static constexpr auto name = "combine_json";

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext)
        {
            resolve_json_output(resolution, "O", true);
        }

        static WiringPortRef compose(Wiring &w, VarKwIn<"kwargs"> kwargs)
        {
            auto &registry = TypeRegistry::instance();
            std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
            std::vector<WiringPortRef>                                       children;
            ListBuilder names{ValuePlanFactory::instance().type_for(scalar_descriptor<Str>::value_meta())};
            fields.reserve(kwargs.size());
            children.reserve(kwargs.size());
            for (const auto &[key, port] : kwargs)
            {
                names.push_back(Str{key});
                fields.emplace_back(key, port.schema);
                children.push_back(port);
            }
            WiringPortRef packed =
                WiringPortRef::structural_source(registry.un_named_tsb(fields), std::move(children));

            WiringArg values_arg;
            values_arg.kind = WiringArg::Kind::TimeSeries;
            values_arg.port = packed;
            values_arg.name = "values";
            WiringArg keys_arg;
            keys_arg.kind         = WiringArg::Kind::Scalar;
            keys_arg.scalar_value = names.build();
            keys_arg.scalar_meta  = keys_arg.scalar_value.schema();
            keys_arg.name         = "keys";
            std::array<WiringArg, 2> args{values_arg, keys_arg};
            return wire_operator(w, "__json_object", args).output.erased();
        }
    };

    struct json_encode_impl
    {
        static constexpr auto name = "json_encode";

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext)
        {
            return json_tree::is_json_ts(resolution.find_ts("S"));
        }

        static void eval(In<"ts", TsVar<"S">> ts, Out<TS<Str>> out)
        {
            std::string result;
            json_tree::encode(ts.base().value(), result);
            out.set(Str{result});
        }
    };

    /** json_encode[bytes]: the encoded text as UTF-8 bytes. */
    struct json_encode_bytes_impl
    {
        static constexpr auto name = "json_encode_bytes";

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext)
        {
            return json_tree::is_json_ts(resolution.find_ts("S"));
        }

        static void eval(In<"ts", TsVar<"S">> ts, Out<TS<Bytes>> out)
        {
            std::string result;
            json_tree::encode(ts.base().value(), result);
            out.set(Bytes{std::move(result)});
        }
    };

    struct json_decode_impl
    {
        static constexpr auto name = "json_decode";

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext)
        {
            resolve_json_output(resolution);
        }

        static Value parse_all(const Str &text)
        {
            return json_tree::box(json_tree::lazy_value(json_tree::JsonValue::parse(text)));
        }

        static void eval(In<"ts", TS<Str>> ts, Out<TsVar<"O">> out)
        {
            const Str text = ts.value();   // keep the storage alive past the view
            json_tree::publish(static_cast<const TSOutputView &>(out), parse_all(text));
        }
    };

    /** json_decode over UTF-8 bytes. */
    struct json_decode_bytes_impl
    {
        static constexpr auto name = "json_decode_bytes";

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext)
        {
            resolve_json_output(resolution);
        }

        static void eval(In<"ts", TS<Bytes>> ts, Out<TsVar<"O">> out)
        {
            const Bytes text = ts.value();
            json_tree::publish(static_cast<const TSOutputView &>(out),
                               json_decode_impl::parse_all(Str{text.data}));
        }
    };

    /** getitem_ over a JSON node: string key (object) / int index (array);
        SILENT when absent or the node has the wrong shape. */
    template <bool ByName>
    struct getitem_json_impl
    {
        static constexpr auto name = ByName ? "getitem_json_by_name" : "getitem_json_by_index";

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            if (context.args.size() != 2 || context.args[0].kind != WiringArg::Kind::TimeSeries) { return false; }
            if (!json_tree::is_json_ts(context.args[0].port.schema)) { return false; }
            if (context.args[1].kind != WiringArg::Kind::Scalar) { return false; }
            const auto *key_meta = context.args[1].scalar_value.schema();
            return ByName ? key_meta == scalar_descriptor<Str>::value_meta()
                          : key_meta == scalar_descriptor<Int>::value_meta();
        }

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext)
        {
            resolve_json_output(resolution);
        }

        static void eval(In<"ts", TsVar<"S">> ts, Scalar<"key", std::conditional_t<ByName, Str, Int>> key,
                         Out<TsVar<"O">> out)
        {
            auto inner = json_tree::unbox(ts.base().value());
            if (!inner.valid()) { return; }
            if (const auto *lazy = json_tree::try_lazy(inner))
            {
                auto child = [&]() {
                    if constexpr (ByName) { return lazy->child(key.value()); }
                    else { return lazy->child(key.value()); }
                }();
                if (!child.has_value()) { return; }
                json_tree::publish(static_cast<const TSOutputView &>(out),
                                   json_tree::box(json_tree::lazy_value(std::move(*child))));
                return;
            }
            if constexpr (ByName)
            {
                if (inner.schema()->value_kind() != ValueTypeKind::Map) { return; }
                auto map = inner.as_map();
                Value key_value{key.value()};
                if (!map.contains(key_value.view())) { return; }
                json_tree::publish(static_cast<const TSOutputView &>(out), Value{map.at(key_value.view())});
            }
            else
            {
                if (inner.schema()->value_kind() != ValueTypeKind::List) { return; }
                auto list  = inner.as_list();
                Int  index = key.value();
                if (index < 0) { index += static_cast<Int>(list.size()); }   // python semantics
                if (index < 0 || index >= static_cast<Int>(list.size())) { return; }
                json_tree::publish(static_cast<const TSOutputView &>(out),
                                   Value{list.at(static_cast<std::size_t>(index))});
            }
        }
    };

    /** Leaf coercions: json_as_int / float / str / bool. */
    template <typename T>
    struct json_as_impl
    {
        static constexpr auto name = std::is_same_v<T, Int>     ? "json_as_int"
                                     : std::is_same_v<T, Float> ? "json_as_float"
                                     : std::is_same_v<T, Str>   ? "json_as_str"
                                                                : "json_as_bool";

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext)
        {
            return json_tree::is_json_ts(resolution.find_ts("S"));
        }

        static void eval(In<"ts", TsVar<"S">> ts, Out<TS<T>> out)
        {
            auto inner = json_tree::unbox(ts.base().value());
            if (!inner.valid()) { return; }
            if (const auto *lazy = json_tree::try_lazy(inner))
            {
                if constexpr (std::is_same_v<T, Int>)
                {
                    if (auto value = lazy->as_int()) { out.set(*value); }
                }
                else if constexpr (std::is_same_v<T, Float>)
                {
                    if (auto value = lazy->as_float()) { out.set(*value); }
                }
                else if constexpr (std::is_same_v<T, Str>)
                {
                    if (auto value = lazy->as_str()) { out.set(std::move(*value)); }
                }
                else if constexpr (std::is_same_v<T, Bool>)
                {
                    if (auto value = lazy->as_bool()) { out.set(*value); }
                }
                return;
            }
            if (inner.schema() == scalar_descriptor<T>::value_meta())
            {
                out.set(inner.checked_as<T>());
                return;
            }
            if constexpr (std::is_same_v<T, Float>)
            {
                if (inner.schema() == scalar_descriptor<Int>::value_meta())
                {
                    out.set(static_cast<Float>(inner.checked_as<Int>()));
                }
            }
        }
    };

    /** Register the JSON operator overloads. */
    void register_json_operators();
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_JSON_IMPL_H
