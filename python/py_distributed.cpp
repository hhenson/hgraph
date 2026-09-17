#include "py_bindings.h"
#include "py_wiring.h"

#include <hgraph/runtime/distributed_map_wiring.h>
#include <hgraph/manifest/schema_descriptor.h>
#include <hgraph/types/value/binary_codec.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>

namespace hgraph::python_bridge
{
    namespace
    {
        nb::object scalar_recipe(const ValueTypeMetaData *schema)
        {
            auto annotation = nb::module_::import_("_hgraph").attr("python_type_for_value")(PyValueType{schema});
            auto recipe = nb::module_::import_("hgraph._distributed").attr("_describe_type")(annotation);
            // Python exports the underlying value class for Shared storage.
            // The process recipe must additionally preserve that storage type.
            if (schema->is_shared()) return nb::make_tuple("shared", recipe);
            return recipe;
        }
        const ValueTypeMetaData *load_scalar(nb::handle recipe)
        {
            auto annotation = nb::module_::import_("hgraph._distributed").attr("_load_type")(recipe);
            return nb::cast<PyValueType>(nb::module_::import_("hgraph._types").attr("_value_type")(annotation)).meta;
        }
        nb::object describe_ts(const TSValueTypeMetaData *schema)
        {
            nb::dict recipe;
            recipe["kind"] = static_cast<int>(schema->kind);
            switch (schema->kind)
            {
                case TSTypeKind::TS: case TSTypeKind::TSS: case TSTypeKind::TSW:
                    recipe["scalar"] = scalar_recipe(schema->kind == TSTypeKind::TSS ? schema->value_schema->element_type : schema->value_type);
                    if (schema->kind == TSTypeKind::TSW)
                    {
                        recipe["duration"] = schema->is_duration_based();
                        recipe["period"] = schema->is_duration_based() ? schema->time_range().count() : static_cast<Int>(schema->period());
                        recipe["minimum"] = schema->is_duration_based() ? schema->min_time_range().count() : static_cast<Int>(schema->min_period());
                    }
                    break;
                case TSTypeKind::TSD:
                    recipe["key"] = scalar_recipe(schema->key_type());
                    recipe["element"] = describe_ts(schema->element_ts());
                    break;
                case TSTypeKind::TSL:
                    recipe["element"] = describe_ts(schema->element_ts());
                    recipe["size"] = schema->is_unbounded_tsl() ? Int{-1} : static_cast<Int>(schema->fixed_size());
                    break;
                case TSTypeKind::TSB:
                {
                    recipe["name"] = schema->bundle_name() == nullptr ? "" : schema->bundle_name();
                    nb::list fields;
                    for (std::size_t i = 0; i < schema->field_count(); ++i)
                        fields.append(nb::make_tuple(schema->fields()[i].name, describe_ts(schema->fields()[i].type)));
                    recipe["fields"] = fields;
                    break;
                }
                case TSTypeKind::SIGNAL: break;
                default: throw nb::value_error("dmap_: boundary schema must be materialized before bootstrap");
            }
            return recipe;
        }
        const TSValueTypeMetaData *load_ts(nb::dict recipe)
        {
            auto &registry = TypeRegistry::instance();
            switch (static_cast<TSTypeKind>(nb::cast<int>(recipe["kind"])))
            {
                case TSTypeKind::TS: return registry.ts(load_scalar(recipe["scalar"]));
                case TSTypeKind::TSS: return registry.tss(load_scalar(recipe["scalar"]));
                case TSTypeKind::TSD: return registry.tsd(load_scalar(recipe["key"]), load_ts(nb::cast<nb::dict>(recipe["element"])));
                case TSTypeKind::TSL:
                {
                    const auto size = nb::cast<Int>(recipe["size"]);
                    return registry.tsl(load_ts(nb::cast<nb::dict>(recipe["element"])), size < 0 ? unbounded_tsl_size : static_cast<std::size_t>(size));
                }
                case TSTypeKind::TSW:
                {
                    auto scalar = load_scalar(recipe["scalar"]);
                    auto period = nb::cast<Int>(recipe["period"]), minimum = nb::cast<Int>(recipe["minimum"]);
                    return nb::cast<bool>(recipe["duration"]) ? registry.tsw_duration(scalar, TimeDelta{period}, TimeDelta{minimum})
                        : registry.tsw(scalar, static_cast<std::size_t>(period), static_cast<std::size_t>(minimum));
                }
                case TSTypeKind::TSB:
                {
                    std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
                    for (auto field : nb::cast<nb::list>(recipe["fields"]))
                    {
                        auto pair = nb::cast<nb::list>(field);
                        fields.emplace_back(nb::cast<std::string>(pair[0]), load_ts(nb::cast<nb::dict>(pair[1])));
                    }
                    auto name = nb::cast<std::string>(recipe["name"]);
                    return name.empty() ? registry.un_named_tsb(fields) : registry.tsb(name, fields);
                }
                case TSTypeKind::SIGNAL: return registry.signal();
                default: throw nb::value_error("dmap_: unknown bootstrap time-series schema");
            }
        }
        std::string output_identity(const TSValueTypeMetaData *schema)
        {
            if (schema == nullptr) return {};
            const auto descriptor = manifest::ts_descriptor(schema);
            return nb::cast<std::string>(nb::module_::import_("base64").attr("b64encode")(
                nb::bytes{reinterpret_cast<const char *>(descriptor.data()), descriptor.size()}).attr("decode")("ascii"));
        }
    }

    void bind_distributed(nb::module_ &m)
    {
        m.def("_distributed_describe_ts", [](PyTsType schema) { return describe_ts(schema.meta); });
        m.def("_distributed_load_ts", [](nb::dict recipe) { return PyTsType{load_ts(recipe)}; });
        m.def("_distributed_pack_scalar", [](nb::handle object) {
            auto value = py_to_value(object);
            const auto bytes = to_binary_string(value.view());
            return nb::make_tuple(scalar_recipe(value.schema()), nb::bytes{bytes.data(), bytes.size()});
        });
        m.def("_distributed_unpack_scalar", [](nb::handle recipe, nb::bytes bytes) {
            auto value = from_binary_string(load_scalar(recipe), std::string_view{bytes.c_str(), bytes.size()});
            return value_to_py(value.view());
        });
        m.def("distributed_map", [](PyWiring &wiring, PyWiredFn func, nb::tuple args, nb::dict kwargs,
                                    Int workers, bool in_process, const std::string &bootstrap,
                                    const std::string &program, const std::vector<std::string> &arguments) -> nb::object {
            if (workers <= 0) throw nb::value_error("dmap_ needs at least one worker");
            if (wiring.finished) throw nb::value_error("Wiring is already finished");
            std::vector<distributed::DistributedMapInput> descriptors;
            std::vector<WiringPortRef> inputs;
            std::optional<std::string> key_arg;
            for (auto &arg : build_args(args, kwargs))
            {
                if (arg.name == "__key_arg__") { key_arg = arg.scalar_value.view().checked_as<Str>(); continue; }
                if (arg.kind == WiringArg::Kind::Scalar)
                {
                    WiringArg scalar = arg;
                    scalar.name.clear();
                    arg.port = wire_operator(wiring.wiring_ref(), "const", {&scalar, 1}, true).output.erased();
                }
                descriptors.push_back({TypeRegistry::instance().dereference(arg.port.schema), arg.port.arg_tag, arg.name});
                inputs.push_back(arg.port);
            }
            distributed::WorkerPoolConfig config;
            config.workers = static_cast<std::size_t>(workers);
            config.hosting = in_process ? distributed::WorkerHosting::InProcess : distributed::WorkerHosting::Process;
            config.program = program;
            config.arguments = arguments;
            auto plan = distributed::prepare_distributed_map_pool(func.fn, descriptors, key_arg, config);
            plan.phase_runner = &py_run_executor_phase;
            if (!in_process)
            {
                nb::dict recipe = nb::cast<nb::dict>(nb::module_::import_("json").attr("loads")(bootstrap));
                nb::list descriptions;
                for (const auto &input : descriptors)
                    descriptions.append(nb::make_tuple(describe_ts(input.schema), static_cast<int>(input.tag), input.name));
                recipe["inputs"] = descriptions;
                recipe["key_arg"] = key_arg ? nb::cast(*key_arg) : nb::none();
                recipe["groups"] = workers;
                recipe["output"] = output_identity(plan.output);
                for (std::size_t group = 0; group < config.workers; ++group)
                {
                    recipe["group"] = group;
                    plan.recipes[group] = nb::cast<std::string>(nb::module_::import_("json").attr("dumps")(recipe));
                }
            }
            auto result = distributed::wire_distributed_map(wiring.wiring_ref(), inputs,
                std::make_shared<const distributed::DistributedMapPlan>(std::move(plan)));
            return result.erased().schema == nullptr ? nb::none() : nb::cast(PyPort{result.erased()});
        }, nb::arg("wiring"), nb::arg("func"), nb::arg("args"), nb::arg("kwargs"), nb::arg("workers"),
           nb::arg("in_process"), nb::arg("bootstrap"), nb::arg("program"), nb::arg("arguments"));

        m.def("serve_distributed_worker", [](PyWiredFn func, nb::dict recipe,
                    std::int64_t read_handle, std::int64_t write_handle, std::int64_t start, std::int64_t end) {
            std::vector<distributed::DistributedMapInput> inputs;
            for (auto item : nb::cast<nb::list>(recipe["inputs"]))
            {
                auto parts = nb::cast<nb::list>(item);
                inputs.push_back({load_ts(nb::cast<nb::dict>(parts[0])),
                    static_cast<WiringPortRef::ArgTag>(nb::cast<int>(parts[1])), nb::cast<std::string>(parts[2])});
            }
            std::optional<std::string> key_arg;
            if (!recipe["key_arg"].is_none()) key_arg = nb::cast<std::string>(recipe["key_arg"]);
            auto plan = distributed::prepare_distributed_map(func.fn, inputs, key_arg,
                nb::cast<std::size_t>(recipe["group"]), nb::cast<std::size_t>(recipe["groups"]));
            if (output_identity(plan.output) != nb::cast<std::string>(recipe["output"]))
                throw nb::value_error("dmap_: worker result schema differs from caller");
            auto channel = distributed::PipeEndpoint::adopt(read_handle, write_handle);
            nb::gil_scoped_release release;
            distributed::serve_worker(channel, std::move(plan.child), plan.slots,
                DateTime{TimeDelta{start}}, DateTime{TimeDelta{end}}, &py_run_executor_phase);
        });
    }
}
