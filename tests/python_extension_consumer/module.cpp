#include <hgraph/lib/std/operators/io.h>
#include <hgraph/python/native_scalar_registration.h>
#include <hgraph/runtime/node_scheduler.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/record_replay.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/time_series/ts_delta.h>

#include <nanobind/nanobind.h>

#include <cstdint>

namespace nb = nanobind;

namespace
{
    struct ConsumerScalar
    {
        std::int64_t value{};

        friend bool operator==(const ConsumerScalar &,
                               const ConsumerScalar &) = default;
    };
}

namespace hgraph
{
    template <>
    struct python_conversion_traits<ConsumerScalar>
    {
        static nb::object to_python(const ConsumerScalar &value)
        {
            return nb::cast(value);
        }

        static ConsumerScalar from_python(nb::handle source)
        {
            return nb::cast<ConsumerScalar>(source);
        }
    };
}  // namespace hgraph


namespace
{
    // A trivial EXTERNAL record/replay backend (RFC 0025 checkpoint 3):
    // overloads of the core operator markers, guarded on this consumer's
    // own backend id, registered through public installed headers only.
    constexpr std::string_view kProbeBackend{"probe.mem"};

    [[nodiscard]] std::string probe_key(std::string_view key, std::string_view suffix)
    {
        return ":probe:" + std::string{key} + "." + std::string{suffix};
    }

    struct probe_record_impl
    {
        static constexpr auto name = "probe_record";

        static bool requires_(const hgraph::ResolutionMap &, hgraph::OperatorCallContext context)
        {
            return hgraph::record_replay::effective_backend_is(context, kProbeBackend);
        }

        static auto defaults()
        {
            return std::tuple{hgraph::arg<"key">(hgraph::Str{"out"}),
                              hgraph::arg<"recordable_id">(hgraph::Str{""}),
                              hgraph::arg<"model">(hgraph::Str{})};
        }

        static void eval(hgraph::In<"ts", hgraph::TsVar<"S">, hgraph::InputValidity::Unchecked> ts,
                         hgraph::Scalar<"key", hgraph::Str> key,
                         hgraph::Scalar<"recordable_id", hgraph::Str>,
                         hgraph::Scalar<"model", hgraph::Str>, hgraph::GlobalStateView gs)
        {
            if (!ts.modified()) { return; }
            const auto        count_key = probe_key(key.value(), "n");
            const auto        count     = gs.get(count_key);
            const hgraph::Int n = count.valid() ? count.checked_as<hgraph::Int>() : hgraph::Int{0};
            gs.set(probe_key(key.value(), std::to_string(n)), hgraph::Value{ts.value()});
            gs.set(count_key, hgraph::Value{hgraph::Int{n + 1}});
        }
    };

    struct probe_replay_impl
    {
        static constexpr auto name              = "probe_replay";
        static constexpr bool schedule_on_start = true;

        static bool requires_(const hgraph::ResolutionMap &, hgraph::OperatorCallContext context)
        {
            return hgraph::record_replay::effective_backend_is(context, kProbeBackend);
        }

        static auto defaults()
        {
            return std::tuple{hgraph::arg<"recordable_id">(hgraph::Str{""}),
                              hgraph::arg<"model">(hgraph::Str{})};
        }

        static void eval(hgraph::Scalar<"key", hgraph::Str> key,
                         hgraph::Scalar<"recordable_id", hgraph::Str>,
                         hgraph::Scalar<"model", hgraph::Str>, hgraph::GlobalStateView gs,
                         hgraph::NodeScheduler sched, hgraph::Out<hgraph::TsVar<"O">> out)
        {
            const auto        count = gs.get(probe_key(key.value(), "n"));
            const hgraph::Int n = count.valid() ? count.checked_as<hgraph::Int>() : hgraph::Int{0};
            const auto        cursor_key = probe_key(key.value(), "i");
            const auto        cursor     = gs.get(cursor_key);
            const hgraph::Int i = cursor.valid() ? cursor.checked_as<hgraph::Int>() : hgraph::Int{0};
            if (i >= n) { return; }
            hgraph::apply_delta(out, gs.get(probe_key(key.value(), std::to_string(i))));
            gs.set(cursor_key, hgraph::Value{hgraph::Int{i + 1}});
            if (i + 1 < n) { sched.schedule(hgraph::MIN_TD); }
        }
    };

    void install_probe_backend()
    {
        hgraph::register_overload<hgraph::stdlib::record, probe_record_impl>();
        hgraph::register_overload<hgraph::stdlib::replay, probe_replay_impl>();
    }
}  // namespace

NB_MODULE(_hgraph_consumer, module)
{
    auto consumer_scalar =
        nb::class_<ConsumerScalar>(module, "ConsumerScalar")
            .def(nb::init<std::int64_t>())
            .def_ro("value", &ConsumerScalar::value)
            .def("__eq__", [](const ConsumerScalar &lhs,
                              const ConsumerScalar &rhs) { return lhs == rhs; });

    hgraph::python_bridge::register_native_scalar_type<ConsumerScalar>(
        consumer_scalar, "hgraph.test.consumer_scalar");

    module.def("registry_address", [] {
        return reinterpret_cast<std::uintptr_t>(&hgraph::TypeRegistry::instance());
    });

    // Keyed installer (RFC 0025 checkpoint 3): the shared runtime replays
    // this registration after every reset_registries, exactly as core's.
    hgraph::OperatorRegistry::instance().register_installer("hgraph.test.probe",
                                                            &install_probe_backend);
    hgraph::OperatorRegistry::instance().run_installers();
}
