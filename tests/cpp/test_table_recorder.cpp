// RFC 0019 step 2 - the recorder driven by a column description.
//
// FrameRecorder is built from a TableConverter and so can only record what a
// converter describes: atomic leaves and depth-1 bundles. A partitioned
// time-series is described by its table layout instead - flattened column
// names plus leaf metadata, which already cover TSD key columns, removed flags
// and expanded frames. These cover what that difference requires: columns come
// from the description, rows arrive as already-resolved cells, and an unset
// cell is a null rather than a default.

#include <hgraph/types/value/table_codec.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/value/value.h>

#include <arrow/array.h>
#include <arrow/table.h>

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <vector>

using namespace hgraph;

namespace
{
    struct Columns
    {
        std::vector<std::string>              names;
        std::vector<const ValueTypeMetaData *> metas;
    };

    /** The shape a one-level TSD produces: bitemporal, then the level's
        removed flag and key, then the value. */
    [[nodiscard]] Columns partitioned_columns()
    {
        const auto *dt   = scalar_descriptor<DateTime>::value_meta();
        const auto *flag = scalar_descriptor<Bool>::value_meta();
        const auto *key  = scalar_descriptor<Str>::value_meta();
        const auto *val  = scalar_descriptor<Int>::value_meta();
        return Columns{
            {"__date_time__", "__as_of__", "__key_1_removed__", "__key_1__", "value"},
            {dt, dt, flag, key, val}};
    }

    [[nodiscard]] ValueView unset() { return ValueView{}; }
}  // namespace

TEST_CASE("table recorder: columns come from the description, not a value schema")
{
    const Columns columns = partitioned_columns();
    TableRecorder recorder{columns.names, columns.metas};

    const auto &schema = recorder.arrow_schema();
    REQUIRE(schema->num_fields() == 5);
    // A converter cannot describe these: they belong to the TSD level, not to
    // any field of the recorded value.
    CHECK(schema->field(2)->name() == "__key_1_removed__");
    CHECK(schema->field(3)->name() == "__key_1__");
    CHECK(schema->field(2)->type()->id() == arrow::Type::BOOL);
    CHECK(schema->field(3)->type()->id() == arrow::Type::STRING);
}

TEST_CASE("table recorder: an unset cell records as null, not as a default")
{
    const Columns columns = partitioned_columns();
    TableRecorder recorder{columns.names, columns.metas};

    const Value when{MIN_ST};
    const Value removed_false{Bool{false}};
    const Value removed_true{Bool{true}};
    const Value key_a{Str{"a"}};
    const Value seven{Int{7}};

    // A value row, then a removal row - which carries no value column at all.
    // Arrays, not vectors: ValueView's copy constructor is deleted, so an
    // initializer list will not do - array elements are copy-initialised from
    // prvalues and elided.
    const ValueView value_row[]{when.view(), when.view(), removed_false.view(), key_a.view(),
                                seven.view()};
    const ValueView removal_row[]{when.view(), when.view(), removed_true.view(), key_a.view(),
                                  unset()};
    recorder.append_row(value_row);
    recorder.append_row(removal_row);
    REQUIRE(recorder.rows() == 2);

    const Frame frame = recorder.finish();
    REQUIRE(frame.has_value());
    REQUIRE(frame.table->num_rows() == 2);

    const auto values = std::static_pointer_cast<arrow::Int64Array>(
        frame.table->GetColumnByName("value")->chunk(0));
    CHECK(values->IsValid(0));
    CHECK(values->Value(0) == 7);
    // The distinction the recorder exists to preserve: absent, not zero.
    CHECK(values->IsNull(1));

    const auto removed = std::static_pointer_cast<arrow::BooleanArray>(
        frame.table->GetColumnByName("__key_1_removed__")->chunk(0));
    CHECK(removed->Value(0) == false);
    CHECK(removed->Value(1) == true);
}

TEST_CASE("table recorder: finish leaves the recorder empty")
{
    const Columns columns = partitioned_columns();
    TableRecorder recorder{columns.names, columns.metas};

    const Value when{MIN_ST};
    const Value flag{Bool{false}};
    const Value key{Str{"a"}};
    const Value one{Int{1}};
    const ValueView row[]{when.view(), when.view(), flag.view(), key.view(), one.view()};
    recorder.append_row(row);

    CHECK(recorder.finish().table->num_rows() == 1);
    CHECK(recorder.rows() == 0);
    // A second finish is an empty frame rather than a repeat of the first.
    CHECK(recorder.finish().table->num_rows() == 0);
}

TEST_CASE("table recorder: a row of the wrong width is refused")
{
    const Columns columns = partitioned_columns();
    TableRecorder recorder{columns.names, columns.metas};
    const Value   when{MIN_ST};

    const ValueView short_row[]{when.view()};
    CHECK_THROWS_AS(recorder.append_row(short_row), std::invalid_argument);
}
