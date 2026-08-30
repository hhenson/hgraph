#include <hgraph/fabric/config.h>
#include <hgraph/fabric/history.h>
#include <hgraph/fabric/keys.h>
#include <hgraph/fabric/metadata_codec.h>
#include <hgraph/fabric/operators.h>
#include <hgraph/fabric/resolution.h>
#include <hgraph/fabric/service.h>
#include <hgraph/fabric/value_builders.h>

#include <hgraph/python/chrono.h>
#include <hgraph/python/native_scalar_registration.h>
#include <hgraph/types/operator_dispatch.h>

#include <nanobind/nanobind.h>
#include <nanobind/stl/optional.h>
#include <nanobind/stl/pair.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>

#include <arrow/builder.h>
#include <arrow/table.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <span>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace nb = nanobind;

namespace
{
    struct PythonFabricConfig
    {
        hgraph::fabric::FabricConfig value{};
    };

    [[nodiscard]] nb::bytes to_python_bytes(
        const hgraph::persistence::store::ObjectBytes &value)
    {
        return nb::bytes{reinterpret_cast<const char *>(value.data()), value.size()};
    }

    [[nodiscard]] std::span<const std::byte> bytes_view(const nb::bytes &value)
    {
        return {reinterpret_cast<const std::byte *>(value.c_str()), value.size()};
    }

    [[nodiscard]] hgraph::Frame fixture_frame(std::int64_t value)
    {
        arrow::Int64Builder builder;
        if (!builder.Append(value).ok())
        {
            throw std::runtime_error("failed to append resolver fixture value");
        }
        auto array = builder.Finish();
        if (!array.ok())
        {
            throw std::runtime_error("failed to finish resolver fixture value");
        }
        return hgraph::Frame{arrow::Table::Make(
            arrow::schema({arrow::field("value", arrow::int64())}),
            {std::move(array).ValueOrDie()})};
    }
}  // namespace

namespace hgraph::fabric
{
    namespace
    {
        struct RegisterMemoryFabricServiceOperator
            : Operator<"hgraph.fabric.register_memory_service",
                       Scalar<"prefix", Str>, Scalar<"path", Str>>
        {
        };

        struct RegisterMemoryFabricServiceGraph
        {
            static constexpr auto name =
                "hgraph.fabric.register_memory_service_impl";

            static void compose(Wiring &wiring,
                                Scalar<"prefix", Str> prefix,
                                Scalar<"path", Str> path)
            {
                if (fabric_config(wiring.global_state(), path.value()).has_value())
                {
                    throw std::invalid_argument(
                        "register_memory_fabric_service found an existing "
                        "FabricConfig");
                }
                set_fabric_config(
                    wiring.global_state(), path.value(),
                    make_memory_fabric_config(prefix.value()));
                register_service(wiring, service::path(path.value()));
            }
        };

        struct RegisterConfiguredFabricServiceOperator
            : Operator<"hgraph.fabric.register_configured_service",
                       Scalar<"path", Str>>
        {
        };

        struct RegisterConfiguredFabricServiceGraph
        {
            static constexpr auto name =
                "hgraph.fabric.register_configured_service_impl";

            static void compose(Wiring &wiring, Scalar<"path", Str> path)
            {
                if (!fabric_config(wiring.global_state(), path.value()).has_value())
                {
                    throw std::invalid_argument(
                        "register_fabric_service requires FabricConfig in the wiring state");
                }
                register_service(wiring, service::path(path.value()));
            }
        };
    }  // namespace
}  // namespace hgraph::fabric

NB_MODULE(_hgraph_fabric, module)
{
    using namespace hgraph;
    using namespace hgraph::fabric;

    nb::class_<PythonFabricConfig>(
        module, "FabricConfig",
        "Owning Fabric persistence configuration for graph hosting and standalone reads.");

    module.def(
        "_make_memory_fabric_config",
        [](Str prefix, std::size_t notification_request_limit) {
            return PythonFabricConfig{make_memory_fabric_config(
                std::move(prefix), notification_request_limit)};
        },
        nb::arg("prefix"), nb::arg("notification_request_limit"));

    module.def(
        "_set_fabric_config",
        [](nb::object state, Str path, const PythonFabricConfig &config) {
            set_fabric_config(nb::cast<GlobalState &>(state).view(), path, config.value);
        },
        nb::arg("state"), nb::arg("path"), nb::arg("config"));

    nb::enum_<ResolutionStatus>(module, "ResolutionStatus")
        .value("READY", ResolutionStatus::Ready)
        .value("UNCHANGED", ResolutionStatus::Unchanged)
        .value("PENDING", ResolutionStatus::Pending)
        .value("AMBIGUOUS", ResolutionStatus::Ambiguous)
        .value("CYCLIC", ResolutionStatus::Cyclic)
        .value("CORRUPT", ResolutionStatus::Corrupt);

    register_fabric_operators();
    OperatorRegistry::instance().register_installer(
        "hgraph.fabric.python_scalars", [] {
            register_graph_overload<RegisterMemoryFabricServiceOperator,
                                    RegisterMemoryFabricServiceGraph>();
            register_graph_overload<RegisterConfiguredFabricServiceOperator,
                                    RegisterConfiguredFabricServiceGraph>();
        });
    OperatorRegistry::instance().run_installers();

    module.def(
        "_encode_revision",
        [](Int format_version, Str data_id, RevisionId revision,
           DataVersion output_version,
           std::vector<std::pair<Str, DataVersion>> dependencies,
           std::optional<DataVersion> self_predecessor,
           DateTime as_of) {
            std::vector<DataDependencyInput> native_dependencies;
            native_dependencies.reserve(dependencies.size());
            for (auto &[dependency_id, version] : dependencies)
            {
                native_dependencies.push_back(DataDependencyInput{
                    .data_id = std::move(dependency_id),
                    .version = version,
                });
            }
            Value value = make_data_revision(DataRevisionInput{
                .format_version = format_version,
                .data_id = std::move(data_id),
                .revision = revision,
                .output_version = output_version,
                .dependencies = std::move(native_dependencies),
                .self_predecessor = self_predecessor,
                .as_of = as_of,
            });
            persistence::store::ObjectBytes encoded;
            notification_codec().encode(value.view(), encoded);
            return to_python_bytes(encoded);
        },
        nb::arg("format_version"), nb::arg("data_id"), nb::arg("revision"),
        nb::arg("output_version"), nb::arg("dependencies"),
        nb::arg("self_predecessor"), nb::arg("as_of"));

    module.def(
        "_decode_revision",
        [](const nb::bytes &encoded) {
            Value value =
                notification_codec().decode(data_revision_meta(), bytes_view(encoded));
            const DataRevisionInput revision = data_revision_input(value.view());
            nb::list dependencies;
            for (const auto &dependency : revision.dependencies)
            {
                dependencies.append(nb::make_tuple(dependency.data_id,
                                                   dependency.version));
            }
            nb::dict result;
            result["format_version"] = revision.format_version;
            result["data_id"] = revision.data_id;
            result["revision"] = revision.revision;
            result["output_version"] = revision.output_version;
            result["dependencies"] = std::move(dependencies);
            result["self_predecessor"] = revision.self_predecessor.has_value()
                                               ? nb::cast(*revision.self_predecessor)
                                               : nb::none();
            result["as_of"] = nb::cast(revision.as_of);
            return result;
        },
        nb::arg("encoded"));

    module.def(
        "_encode_revision_reference",
        [](std::uint8_t kind, RevisionId revision) {
            // A utility, not a store operation: the json codec directly, so the
            // Python surface needs no configuration to encode a reference.
            persistence::store::ObjectBytes encoded;
            notification_codec().encode(
                make_revision_reference(static_cast<MetadataObjectKind>(kind), revision)
                    .view(),
                encoded);
            return to_python_bytes(encoded);
        },
        nb::arg("kind"), nb::arg("revision"));

    module.def(
        "_decode_revision_reference",
        [](std::uint8_t kind, const nb::bytes &encoded) {
            return revision_reference_id(
                notification_codec()
                    .decode(revision_reference_meta(), bytes_view(encoded))
                    .view(),
                static_cast<MetadataObjectKind>(kind));
        },
        nb::arg("kind"), nb::arg("encoded"));

    module.def(
        "_load_data",
        [](const PythonFabricConfig &config, Str data_id,
           DateTime as_of) -> nb::object {
            auto frame = load_data(config.value, std::move(data_id), as_of);
            return frame.has_value()
                       ? python_conversion_traits<Frame>::to_python(*frame)
                       : nb::none();
        },
        nb::arg("config"), nb::arg("data_id"), nb::arg("as_of"));

    module.def(
        "_resolve_fixture",
        [](nb::iterable encoded_revisions, std::vector<Str> roots,
           std::vector<std::pair<Str, DataVersion>> exposed) {
            std::vector<DataRevisionInput> revisions;
            for (nb::handle item : encoded_revisions)
            {
                const nb::bytes encoded = nb::cast<nb::bytes>(item);
                revisions.push_back(data_revision_input(
                    notification_codec()
                        .decode(data_revision_meta(), bytes_view(encoded))
                        .view()));
            }
            std::ranges::sort(
                revisions,
                [](const DataRevisionInput &lhs, const DataRevisionInput &rhs) {
                    if (lhs.data_id == rhs.data_id)
                    {
                        return lhs.revision < rhs.revision;
                    }
                    return canonical_data_id_less(lhs.data_id, rhs.data_id);
                });

            FabricConfig config =
                make_memory_fabric_config("python/resolution-fixture");
            for (const auto &revision : revisions)
            {
                const std::string frame_key = data_version_key(
                    config.prefix, revision.data_id, revision.output_version);
                if (!config.frames.contains(frame_key))
                {
                    config.frames.write(
                        frame_key, fixture_frame(revision.output_version));
                }
                Value value = make_data_revision(revision);
                const auto stored = config.objects.put_immutable(
                    revision_key(config.prefix, revision.data_id,
                                 revision.revision),
                    config.values.encode(value.view()));
                if (stored.status !=
                    persistence::store::ImmutableWriteStatus::Created)
                {
                    throw std::invalid_argument(
                        "resolver fixture revisions must have unique slots");
                }
            }
            std::vector<ExposedRootVersion> native_exposed;
            native_exposed.reserve(exposed.size());
            for (auto &[data_id, version] : exposed)
            {
                native_exposed.push_back(
                    {.data_id = std::move(data_id), .output_version = version});
            }

            ConsistencyResolver resolver{std::move(config)};
            const ForestResolution result =
                resolver.resolve_forest(std::move(roots), native_exposed);
            nb::dict output;
            output["status"] = result.status;
            output["diagnostic"] = result.diagnostic;
            output["observed_data_ids"] = result.observed_data_ids;
            nb::list cut;
            if (result.cut.has_value())
            {
                for (const auto &revision : result.cut->revisions)
                {
                    cut.append(nb::make_tuple(
                        revision.data_id, revision.revision,
                        revision.output_version));
                }
            }
            output["cut"] = std::move(cut);
            nb::list changed;
            for (const auto &root : result.changed_roots)
            {
                changed.append(nb::make_tuple(
                    root.data_id, root.revision, root.output_version));
            }
            output["changed_roots"] = std::move(changed);
            nb::dict metrics;
            metrics["revision_cache_hits"] = result.metrics.revision_cache_hits;
            metrics["revision_cache_misses"] =
                result.metrics.revision_cache_misses;
            metrics["output_index_hits"] = result.metrics.output_index_hits;
            metrics["revisions_examined"] = result.metrics.revisions_examined;
            metrics["edges_examined"] = result.metrics.edges_examined;
            metrics["maximum_backtracking_depth"] =
                result.metrics.maximum_backtracking_depth;
            metrics["notice_to_ready_microseconds"] =
                result.metrics.notice_to_ready.has_value()
                    ? nb::cast(result.metrics.notice_to_ready->count())
                    : nb::none();
            output["metrics"] = std::move(metrics);
            return output;
        },
        nb::arg("encoded_revisions"), nb::arg("roots"),
        nb::arg("exposed") = std::vector<std::pair<Str, DataVersion>>{});

    module.doc() = "hgraph versioned dataflow fabric public contracts";
}
