#include <hgraph/runtime/executor.h>
#include <hgraph/runtime/global_state.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/value/binary_compression.h>

#include <stdexcept>
#include <string>

namespace
{
    using namespace hgraph;

    struct Source
    {
        static constexpr bool schedule_on_start = true;
        static void eval(Out<TS<Int>> out) { out.set(41); }
    };

    struct AddOne
    {
        static void eval(In<"value", TS<Int>> input, Out<TS<Int>> out) { out.set(input.value() + 1); }
    };

    struct Sink
    {
        static void eval(In<"value", TS<Int>> input, GlobalStateView state) { state.set("result", Value{input.value()}); }
    };

    template <typename T> bool registration_requires_arrow()
    {
        try { static_cast<void>(scalar_descriptor<T>::value_meta()); }
        catch (const std::logic_error &error) { return std::string_view{error.what()}.contains("HGRAPH_WITH_ARROW"); }
        return false;
    }
}

int main()
{
    using namespace hgraph;
    Wiring wiring;
    wire<Sink>(wiring, wire<AddOne>(wiring, wire<Source>(wiring)));
    GraphExecutorBuilder builder;
    builder.graph_builder(std::move(wiring).finish()).start_time(MIN_ST).end_time(MIN_ST + MIN_TD);
    auto executor = builder.make_executor();
    executor.view().run();
    if (executor.view().graph().global_state().get_as<Int>("result") != 42) { return 1; }

    if (TypeRegistry::instance().value_type("frame") || TypeRegistry::instance().value_type("series")) { return 2; }
    if (!registration_requires_arrow<Frame>() || !registration_requires_arrow<Series>()) { return 2; }
    if (default_binary_compression() != BinaryCompression::None) { return 3; }
    const std::string bytes(1024, 'x');
    std::string encoded;
    write_compressed_block(bytes, BinaryCompression::Zstd, encoded);
    BinaryReader reader{encoded};
    std::string storage;
    if (read_compressed_block(reader, storage) != bytes) { return 4; }
    // A block written with a codec from the full SDK carries its encoding on the wire.
    BinaryReader compressed{std::string_view{"\x01\x01\x01x", 4}};
    try { static_cast<void>(read_compressed_block(compressed, storage)); }
    catch (const std::runtime_error &error) { return std::string_view{error.what()}.contains("zstd") ? 0 : 5; }
    return 6;
}
