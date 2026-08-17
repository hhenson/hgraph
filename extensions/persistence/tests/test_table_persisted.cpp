#include <hgraph/persistence/recording_store.h>
#include <hgraph/lib/std/operators/impl/table_impl.h>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_realization.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/record_replay.h>
#include <hgraph/types/table_config.h>
#include <hgraph/types/registry_reset.h>
#include <hgraph/types/temporal.h>
#include <hgraph/types/value/table_codec.h>
#include <hgraph/types/value/value_builder.h>

#include <arrow/api.h>

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <limits>

// Step 3 of the record/replay/table design record: the Arrow-backed Frame
// value kind + the interned per-schema TableConverter (bitemporal
// [date, as_of, *columns] rows written directly into Arrow builders) and the
// to_table / from_table operators.

struct ExtensionTableScalar
{
    std::int64_t value{0};
    bool         operator==(const ExtensionTableScalar &) const = default;
};

namespace polymorphic_table_repro
{
    struct Event
    {
    };
}

namespace hgraph::static_schema_detail
{
    template <>
    struct scalar_name<ExtensionTableScalar>
    {
        static constexpr std::string_view value{"tests.ExtensionTableScalar"};
    };
}  // namespace hgraph::static_schema_detail

namespace hgraph
{
    template <>
    struct scalar_descriptor<polymorphic_table_repro::Event>
    {
        [[nodiscard]] static constexpr bool is_concrete() noexcept { return true; }
        [[nodiscard]] static const ValueTypeMetaData *value_meta()
        {
            auto &registry = TypeRegistry::instance();
            return registry.bundle(
                "tests.table", "Event",
                {{"event_id", registry.value_type("str")}}, {}, true);
        }
    };
}

namespace hgraph::testing
{
    template <>
    struct ts_harness<TS<polymorphic_table_repro::Event>>
        : bundle_ts_harness<TS<polymorphic_table_repro::Event>>
    {
    };
}

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    int extension_describes{0};
    int extension_emits{0};
    int extension_applies{0};

    void describe_extension_scalar(TableLayout &layout, const TSValueTypeMetaData *schema,
                                   std::string, std::vector<std::size_t>, std::size_t)
    {
        ++extension_describes;
        layout.leaf_ts = schema;
        layout.value_col_start = layout.keys.size();
        layout.keys.push_back("value");
        layout.col_metas.push_back(scalar_descriptor<Int>::value_meta());
        layout.value_cols.push_back(
            TableLayout::Column{.name = "value", .leaf = scalar_descriptor<Int>::value_meta()});
    }

    void emit_extension_scalar(const TableLayout &, const TSInputView &ts, Int, DateTime now,
                               DateTime as_of, bool, const TableRowSink &sink, bool)
    {
        ++extension_emits;
        const Value when{now};
        const Value revision{as_of};
        const Value cell{ts.value().checked_as<ExtensionTableScalar>().value};
        sink.cell(sink.context, 0, when.view());
        sink.cell(sink.context, 1, revision.view());
        sink.cell(sink.context, 2, cell.view());
        sink.end_row(sink.context);
    }

    void apply_extension_scalar(const TableLayout &, const TableRowSource &source,
                                const TSOutputView &out)
    {
        ++extension_applies;
        const Value cell = source.cell(source.context, 0, 2);
        const Value wrapped{ExtensionTableScalar{cell.view().checked_as<Int>()}};
        apply_current_value(out, wrapped.view());
    }

    const TableTypeOps extension_table_ops{&describe_extension_scalar, &emit_extension_scalar,
                                           &apply_extension_scalar};

    void describe_polymorphic_event(TableLayout &layout,
                                    const TSValueTypeMetaData *schema,
                                    std::string, std::vector<std::size_t>,
                                    std::size_t)
    {
        layout.leaf_ts = schema;
        layout.value_col_start = layout.keys.size();
        layout.keys.push_back("event");
        layout.col_metas.push_back(schema->value_schema);
        layout.value_cols.push_back(
            TableLayout::Column{.name = "event", .leaf = schema->value_schema});
    }

    void emit_polymorphic_event(const TableLayout &layout, const TSInputView &ts,
                                Int, DateTime now, DateTime as_of, bool,
                                const TableRowSink &sink, bool)
    {
        const Value when{now};
        const Value revision{as_of};
        sink.cell(sink.context, 0, when.view());
        sink.cell(sink.context, 1, revision.view());
        sink.cell(sink.context, layout.value_col_start, ts.value());
        sink.end_row(sink.context);
    }

    void apply_polymorphic_event(const TableLayout &layout,
                                 const TableRowSource &source,
                                 const TSOutputView &out)
    {
        const Value cell =
            source.cell(source.context, 0, layout.value_col_start);
        apply_current_value(out, cell.view());
    }

    const TableTypeOps polymorphic_event_table_ops{
        &describe_polymorphic_event, &emit_polymorphic_event,
        &apply_polymorphic_event};

    void describe_all_null_extension_scalar(TableLayout &layout,
                                             const TSValueTypeMetaData *schema, std::string,
                                             std::vector<std::size_t>, std::size_t)
    {
        ++extension_describes;
        layout.leaf_ts = schema;
        layout.value_col_start = layout.keys.size();
        layout.keys.push_back("value");
        layout.col_metas.push_back(scalar_descriptor<Int>::value_meta());
    }

    void describe_empty_extension_scalar(TableLayout &layout, const TSValueTypeMetaData *schema,
                                          std::string, std::vector<std::size_t>, std::size_t)
    {
        ++extension_describes;
        layout.leaf_ts = schema;
        layout.value_col_start = layout.keys.size();
    }

    void emit_presence_only_extension_scalar(const TableLayout &, const TSInputView &, Int,
                                              DateTime now, DateTime as_of, bool,
                                              const TableRowSink &sink, bool)
    {
        ++extension_emits;
        const Value when{now};
        const Value revision{as_of};
        sink.cell(sink.context, 0, when.view());
        sink.cell(sink.context, 1, revision.view());
        sink.end_row(sink.context);
    }

    void apply_all_null_extension_scalar(const TableLayout &, const TableRowSource &source,
                                         const TSOutputView &out)
    {
        ++extension_applies;
        if (source.rows != 1)
        {
            throw std::logic_error("all-null extension expected one row");
        }
        const Value wrapped{ExtensionTableScalar{77}};
        apply_current_value(out, wrapped.view());
    }

    void apply_empty_extension_scalar(const TableLayout &, const TableRowSource &source,
                                      const TSOutputView &out)
    {
        ++extension_applies;
        if (source.rows != 1)
        {
            throw std::logic_error("empty extension expected one row");
        }
        const Value wrapped{ExtensionTableScalar{88}};
        apply_current_value(out, wrapped.view());
    }

    const TableTypeOps all_null_extension_table_ops{&describe_all_null_extension_scalar,
                                                     &emit_presence_only_extension_scalar,
                                                     &apply_all_null_extension_scalar};
    const TableTypeOps empty_extension_table_ops{&describe_empty_extension_scalar,
                                                  &emit_presence_only_extension_scalar,
                                                  &apply_empty_extension_scalar};

    [[nodiscard]] std::int64_t timestamp_at(const Frame &frame, const std::string &column,
                                            std::int64_t row)
    {
        const auto chunked = frame.table->GetColumnByName(column);
        REQUIRE(chunked != nullptr);
        return static_cast<const arrow::TimestampArray &>(*chunked->chunk(0)).Value(row);
    }
}  // namespace

namespace
{
    struct TableRoundTripGraph
    {
        [[maybe_unused]] static constexpr auto name = "table_round_trip_graph";

        static Port<TS<Float>> compose(Wiring &w, Port<TS<Float>> ts)
        {
            auto frame = wire<stdlib::to_table>(w, ts);
            return wire<stdlib::from_table, TS<Float>>(w, frame).as<TS<Float>>();
        }
    };

    struct ExtensionTableRoundTripGraph
    {
        [[maybe_unused]] static constexpr auto name = "extension_table_round_trip_graph";

        static Port<TS<ExtensionTableScalar>> compose(Wiring &w, Port<TS<ExtensionTableScalar>> ts)
        {
            auto rows = wire<stdlib::to_table>(w, ts);
            return wire<stdlib::from_table, TS<ExtensionTableScalar>>(w, rows)
                .as<TS<ExtensionTableScalar>>();
        }
    };

    struct ExtensionRecordGraph
    {
        [[maybe_unused]] static constexpr auto name = "extension_record_graph";

        static Port<TS<ExtensionTableScalar>> compose(Wiring &w, Port<TS<ExtensionTableScalar>> ts)
        {
            wire<stdlib::record>(w, ts, Str{"values"}, arg<"recordable_id">(Str{"extension"}));
            return ts;
        }
    };

    struct ExtensionReplayGraph
    {
        [[maybe_unused]] static constexpr auto name = "extension_replay_graph";

        static Port<TS<ExtensionTableScalar>> compose(Wiring &w)
        {
            return wire<stdlib::replay, TS<ExtensionTableScalar>>(
                       w, Str{"values"}, arg<"recordable_id">(Str{"extension"}))
                .as<TS<ExtensionTableScalar>>();
        }
    };

    using NestedExtensionBundle =
        UnNamedTSB<Field<"custom", TS<ExtensionTableScalar>>, Field<"regular", TS<Int>>>;
    using NestedExtensionDict = TSD<Str, TS<ExtensionTableScalar>>;
    using PolymorphicTableEvent = polymorphic_table_repro::Event;
    using PolymorphicTableDict = TSD<Str, TS<PolymorphicTableEvent>>;

    template <typename Schema>
    struct NestedExtensionTableRoundTripGraph
    {
        [[maybe_unused]] static constexpr auto name = "nested_extension_table_round_trip_graph";

        static Port<Schema> compose(Wiring &w, Port<Schema> ts)
        {
            auto rows = wire<stdlib::to_table>(w, ts);
            return wire<stdlib::from_table, Schema>(w, rows).template as<Schema>();
        }
    };

    struct PolymorphicTableRoundTripGraph
    {
        [[maybe_unused]] static constexpr auto name =
            "polymorphic_table_round_trip_graph";

        static Port<PolymorphicTableDict>
        compose(Wiring &w, Port<PolymorphicTableDict> ts)
        {
            auto rows = wire<stdlib::to_table>(w, ts);
            return wire<stdlib::from_table, PolymorphicTableDict>(w, rows)
                .as<PolymorphicTableDict>();
        }
    };

    template <typename Schema>
    struct NestedExtensionRecordGraph
    {
        [[maybe_unused]] static constexpr auto name = "nested_extension_record_graph";

        static Port<Schema> compose(Wiring &w, Port<Schema> ts)
        {
            wire<stdlib::record>(w, ts, Str{"values"},
                                 arg<"recordable_id">(Str{"nested_extension"}));
            return ts;
        }
    };

    template <typename Schema>
    struct NestedExtensionReplayGraph
    {
        [[maybe_unused]] static constexpr auto name = "nested_extension_replay_graph";

        static Port<Schema> compose(Wiring &w)
        {
            return wire<stdlib::replay, Schema>(
                       w, Str{"values"}, arg<"recordable_id">(Str{"nested_extension"}))
                .template as<Schema>();
        }
    };
}  // namespace

// The PERSISTED table-type-ops behaviours (RFC 0025 checkpoint 4): stored
// replay through the frame backend moved here with the store. The direct
// emit/apply extension-mechanism cases stay in core tests/cpp/test_table.cpp.
TEST_CASE("table type ops: stored replay dispatches through the extension "
          "operation")
{
    stdlib::register_standard_operators();
    GlobalContext context;
    record_replay::set_config(
        context.state().view(),
        record_replay::RecordReplayConfig{.backend = "hgraph.persistence.frame"});
    const auto *schema =
        TypeRegistry::instance().ts(scalar_descriptor<ExtensionTableScalar>::value_meta());
    extension_describes = 0;
    extension_emits = 0;
    extension_applies = 0;
    register_table_type_ops(schema, extension_table_ops);

    const auto expected =
        values<ExtensionTableScalar>(ExtensionTableScalar{2}, none, ExtensionTableScalar{5});
    (void)eval_node<ExtensionRecordGraph>(expected);
    CHECK_OUTPUT(eval_node<ExtensionReplayGraph>(), expected);

    CHECK(extension_describes == 1);
    CHECK(extension_emits == 2);
    CHECK(extension_applies == 2);
}

TEST_CASE("table type ops: a registered child records and replays beneath a TSD")
{
    stdlib::register_standard_operators();
    GlobalContext context;
    record_replay::set_config(
        context.state().view(),
        record_replay::RecordReplayConfig{.backend = "hgraph.persistence.frame"});
    const auto *child_schema =
        TypeRegistry::instance().ts(scalar_descriptor<ExtensionTableScalar>::value_meta());
    extension_describes = 0;
    extension_emits = 0;
    extension_applies = 0;
    register_table_type_ops(child_schema, extension_table_ops);

    const Value first = dict_delta<Str, TS<ExtensionTableScalar>>(
        {{Str{"one"}, ExtensionTableScalar{1}}, {Str{"two"}, ExtensionTableScalar{2}}});
    const Value second = dict_delta<Str, TS<ExtensionTableScalar>>(
        {{Str{"one"}, ExtensionTableScalar{4}}});
    const auto expected = values<Value>(first, second);

    (void)eval_node<NestedExtensionRecordGraph<NestedExtensionDict>>(expected);
    CHECK_OUTPUT(eval_node<NestedExtensionReplayGraph<NestedExtensionDict>>(), expected);

    CHECK(extension_describes == 1);
    CHECK(extension_emits == 3);
    CHECK(extension_applies == 3);
}

TEST_CASE("table type ops: a nested all-null child row survives persisted replay")
{
    stdlib::register_standard_operators();
    GlobalContext context;
    record_replay::set_config(
        context.state().view(),
        record_replay::RecordReplayConfig{.backend = "hgraph.persistence.frame"});
    const auto *child_schema =
        TypeRegistry::instance().ts(scalar_descriptor<ExtensionTableScalar>::value_meta());
    extension_describes = 0;
    extension_emits = 0;
    extension_applies = 0;
    register_table_type_ops(child_schema, all_null_extension_table_ops);

    const Value input = dict_delta<Str, TS<ExtensionTableScalar>>(
        {{Str{"one"}, ExtensionTableScalar{1}}, {Str{"two"}, ExtensionTableScalar{2}}});
    const Value expected = dict_delta<Str, TS<ExtensionTableScalar>>(
        {{Str{"one"}, ExtensionTableScalar{77}}, {Str{"two"}, ExtensionTableScalar{77}}});

    (void)eval_node<NestedExtensionRecordGraph<NestedExtensionDict>>(values<Value>(input));
    CHECK_OUTPUT(eval_node<NestedExtensionReplayGraph<NestedExtensionDict>>(),
                 values<Value>(expected));

    CHECK(extension_describes == 1);
    CHECK(extension_emits == 2);
    CHECK(extension_applies == 2);
}

