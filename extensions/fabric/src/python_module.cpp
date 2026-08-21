#include <hgraph/fabric/metadata_codec.h>
#include <hgraph/fabric/operators.h>
#include <hgraph/fabric/value_builders.h>

#include <hgraph/python/native_scalar_registration.h>
#include <hgraph/types/operator_dispatch.h>

#include <nanobind/nanobind.h>
#include <nanobind/stl/optional.h>
#include <nanobind/stl/pair.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>

#include <cstddef>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace nb = nanobind;

namespace
{
    [[nodiscard]] nb::bytes to_python_bytes(
        const hgraph::persistence::store::ObjectBytes &value)
    {
        return nb::bytes{reinterpret_cast<const char *>(value.data()), value.size()};
    }

    [[nodiscard]] std::span<const std::byte> bytes_view(const nb::bytes &value)
    {
        return {reinterpret_cast<const std::byte *>(value.c_str()), value.size()};
    }
}  // namespace

NB_MODULE(_hgraph_fabric, module)
{
    using namespace hgraph;
    using namespace hgraph::fabric;

    auto mode = nb::enum_<SubscriptionMode>(module, "SubscriptionMode")
                    .value("LIVE", SubscriptionMode::Live)
                    .value("REPLAY", SubscriptionMode::Replay)
                    .value("SNAPSHOT", SubscriptionMode::Snapshot);

    register_fabric_operators();
    OperatorRegistry::instance().register_installer(
        "hgraph.fabric.python_scalars", [mode] {
            python_bridge::register_native_scalar_type<SubscriptionMode>(mode);
        });
    OperatorRegistry::instance().run_installers();

    module.def(
        "_encode_revision",
        [](Int format_version, Str data_id, RevisionId revision,
           DataVersion output_version,
           std::vector<std::pair<Str, DataVersion>> dependencies,
           std::optional<DataVersion> self_predecessor,
           std::int64_t as_of_microseconds) {
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
                .as_of = DateTime{TimeDelta{as_of_microseconds}},
            });
            return to_python_bytes(encode_revision(value.view()));
        },
        nb::arg("format_version"), nb::arg("data_id"), nb::arg("revision"),
        nb::arg("output_version"), nb::arg("dependencies"),
        nb::arg("self_predecessor"), nb::arg("as_of_microseconds"));

    module.def(
        "_decode_revision",
        [](const nb::bytes &encoded) {
            Value value = decode_revision(bytes_view(encoded));
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
            result["as_of_microseconds"] =
                revision.as_of.time_since_epoch().count();
            return result;
        },
        nb::arg("encoded"));

    module.def(
        "_encode_revision_reference",
        [](std::uint8_t kind, RevisionId revision) {
            return to_python_bytes(encode_revision_reference(
                static_cast<MetadataObjectKind>(kind), revision));
        },
        nb::arg("kind"), nb::arg("revision"));

    module.def(
        "_decode_revision_reference",
        [](std::uint8_t kind, const nb::bytes &encoded) {
            return decode_revision_reference(
                static_cast<MetadataObjectKind>(kind), bytes_view(encoded));
        },
        nb::arg("kind"), nb::arg("encoded"));

    module.attr("REVISION_MEDIA_TYPE") = std::string{REVISION_MEDIA_TYPE};
    module.attr("AS_OF_MEDIA_TYPE") = std::string{AS_OF_MEDIA_TYPE};
    module.attr("LATEST_MEDIA_TYPE") = std::string{LATEST_MEDIA_TYPE};
    module.doc() = "hgraph versioned dataflow fabric public contracts";
}
