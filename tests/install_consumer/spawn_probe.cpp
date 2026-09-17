#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/runtime/spawn.h>
#include <hgraph/types/static_node.h>

#include <array>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#endif

namespace
{
    using namespace hgraph;
    long long process_id()
    {
#ifdef _WIN32
        return _getpid();
#else
        return getpid();
#endif
    }
    struct Consume
    {
        static void eval(In<"value", TS<Int>> value, Scalar<"path", Str> path)
        {
            std::ofstream output{path.value(), std::ios::app};
            output << process_id() << ' ' << value.value() << '\n';
            if (!output) throw std::runtime_error("cannot write installed spawn trace");
        }
    };
    struct Offset
    {
        static void eval(In<"value", TS<Int>> value, In<"offset", TS<Int>> offset, Out<TS<Int>> out)
        { out.set(value.value() + offset.value()); }
    };
    struct Probe
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> value, Scalar<"path", Str> path)
        {
            auto offset = wire<stdlib::const_>(w, Int{10}).as<TS<Int>>();
            auto stage = process_stage(bind_(fn<Offset>(), {{"offset", offset.erased()}}), "installed-offset");
            auto sink = process_stage(spawn_fn<Consume>(arg<"path">(path.value())), "installed-consume", path.value());
            WiringArg input;
            input.port = value.erased();
            std::array arguments{input};
            wire_spawn(w, pipeline_({std::move(stage), std::move(sink)}), arguments);
            return value;
        }
    };
}

void register_spawn_consumer_recipes()
{
    hgraph::stdlib::register_standard_operators();
    hgraph::register_spawn_worker_recipe("installed-offset", +[](std::string_view) {
        const std::array inputs{schema_descriptor<TS<Int>>::ts_meta(), schema_descriptor<TS<Int>>::ts_meta()};
        return prepare_spawn_worker(fn<Offset>(), inputs);
    });
    hgraph::register_spawn_worker_recipe("installed-consume", +[](std::string_view path) {
        const std::array inputs{schema_descriptor<TS<Int>>::ts_meta()};
        const auto stage = spawn_fn<Consume>(arg<"path">(Str{path}));
        return prepare_spawn_worker(stage.function, inputs);
    });
}

void check_spawn_consumer()
{
    const auto path = std::filesystem::temp_directory_path() /
        ("installed-spawn-" + std::to_string(process_id()) + ".txt");
    std::filesystem::remove(path);
    (void)hgraph::testing::eval_node<Probe>(std::vector<std::optional<hgraph::Int>>{1, 2, 3},
                                           hgraph::arg<"path">(path.string()));
    std::ifstream trace{path};
    for (hgraph::Int expected : {11, 12, 13})
    {
        long long pid = 0;
        hgraph::Int value = 0;
        if (!(trace >> pid >> value) || pid == process_id() || value != expected)
            throw std::runtime_error("installed spawn pipeline did not run in a worker process");
    }
    trace.close();
    std::filesystem::remove(path);
}
