#include <hgraph/lib/std/standard_types.h>
#include <hgraph/types/frame.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/type_resolution.h>
#include <hgraph/types/metadata/type_record.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/type_pointer.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_builder.h>

#include <arrow/api.h>

#include <cstdint>
#include <stdexcept>
#include <type_traits>

int main()
{
    using namespace hgraph;

    static_assert(std::is_standard_layout_v<SchemaHeader>);
    static_assert(std::is_trivially_copyable_v<SchemaHeader>);
    static_assert(std::is_standard_layout_v<TypeRecord>);
    static_assert(std::is_trivially_copyable_v<TypeRecord>);
    static_assert(std::is_standard_layout_v<AnyPtr>);
    static_assert(std::is_trivially_copyable_v<AnyPtr>);
    using ConsumerScalar =
        Bundle<"consumer::Scalar", Field<"number", Int>, Field<"label", Str>>;
    using ConsumerBundle = TSBFromScalar<ConsumerScalar>;
    static_assert(std::is_same_v<ConsumerBundle,
                                 TSB<"consumer::Scalar", Field<"number", TS<Int>>,
                                     Field<"label", TS<Str>>>>);

    auto &registry = TypeRegistry::instance();
    registry.register_scalar<std::int32_t>("int32");

    Value value{std::int32_t{41}};
    auto view = value.view();
    const AnyPtr pointer = AnyPtr::read_only(*view.record(), view.data());
    if (!pointer.valid() || pointer.family() != TypeFamily::Value || !pointer.read_only_access())
    {
        throw std::runtime_error("installed target produced an invalid erased pointer");
    }

    value.begin_mutation().as<std::int32_t>() = 73;
    if (value.view().checked_as<std::int32_t>() != 73)
    {
        throw std::runtime_error("installed target mutation round-trip failed");
    }

    static_cast<void>(stdlib::register_standard_types(registry));

    using TypedFrame = FrameOf<Bundle<"consumer::Row", Field<"value", Int>>,
                               Bundle<"consumer::Metadata", Field<"revision", Int>>>;
    const auto *typed_frame = scalar_type<TypedFrame>();
    if (typed_frame == nullptr || typed_frame->key_type == nullptr)
    {
        throw std::runtime_error("installed typed frame metadata schema is unusable");
    }

    arrow::Int64Builder values;
    if (!values.Append(42).ok())
    {
        throw std::runtime_error("installed Arrow builder is unusable");
    }
    std::shared_ptr<arrow::Array> array;
    if (!values.Finish(&array).ok())
    {
        throw std::runtime_error("installed Arrow array construction failed");
    }
    Frame frame{arrow::Table::Make(
        arrow::schema({arrow::field("value", arrow::int64())}),
        {std::move(array)})};
    BundleBuilder metadata{ValuePlanFactory::instance().type_for(typed_frame->key_type)};
    metadata.set(0, Value{Int{7}});
    frame = with_frame_metadata(std::move(frame), metadata.build());
    if (!frame.has_metadata() ||
        frame_metadata(frame, typed_frame->key_type).as_bundle().at(0).checked_as<Int>() != 7)
    {
        throw std::runtime_error("installed Arrow frame metadata codec is unusable");
    }

    return 0;
}
