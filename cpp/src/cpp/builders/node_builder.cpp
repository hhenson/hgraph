#include <hgraph/types/node.h>
#include <hgraph/types/time_series/ts_type_registry.h>
#include <hgraph/types/value/type_registry.h>

#include <hgraph/builders/builder.h>
#include <hgraph/builders/graph_builder.h>
#include <hgraph/builders/input_builder.h>
#include <hgraph/builders/node_builder.h>
#include <hgraph/builders/output_builder.h>

// Include all the extracted builder headers
#include <hgraph/builders/nodes/python_node_builder.h>
#include <hgraph/builders/nodes/python_generator_node_builder.h>
#include <hgraph/builders/nodes/tsd_map_node_builder.h>
#include <hgraph/builders/nodes/reduce_node_builder.h>
#include <hgraph/builders/nodes/context_node_builder.h>
#include <hgraph/builders/nodes/base_nested_graph_node_builder.h>
#include <hgraph/builders/nodes/nested_graph_node_builder.h>
#include <hgraph/builders/nodes/component_node_builder.h>
#include <hgraph/builders/nodes/try_except_node_builder.h>
#include <hgraph/builders/nodes/switch_node_builder.h>
#include <hgraph/builders/nodes/tsd_non_associative_reduce_node_builder.h>
#include <hgraph/builders/nodes/mesh_node_builder.h>
#include <hgraph/builders/nodes/last_value_pull_node_builder.h>

#include <unordered_set>
#include <utility>

namespace hgraph {
    namespace {
        const TSMeta *meta_from_ts_signature_object(const nb::object &obj) {
            if (!obj.is_valid() || obj.is_none()) {
                return nullptr;
            }

            try {
                if (nb::hasattr(obj, "cpp_type")) {
                    nb::object cpp_type = nb::getattr(obj, "cpp_type");
                    if (cpp_type.is_valid() && !cpp_type.is_none()) {
                        return nb::cast<const TSMeta *>(cpp_type);
                    }
                }

                if (nb::hasattr(obj, "ts_type")) {
                    return meta_from_ts_signature_object(nb::getattr(obj, "ts_type"));
                }

                if (nb::hasattr(obj, "tsb_type")) {
                    return meta_from_ts_signature_object(nb::getattr(obj, "tsb_type"));
                }
            } catch (...) {
                return nullptr;
            }

            return nullptr;
        }

        const TSMeta *input_meta_from_signature(const NodeSignature &signature) {
            if (!signature.time_series_inputs.has_value() || signature.time_series_inputs->empty()) {
                return nullptr;
            }

            std::vector<std::pair<std::string, const TSMeta *>> fields;
            fields.reserve(signature.time_series_inputs->size());
            std::unordered_set<std::string> seen;
            seen.reserve(signature.time_series_inputs->size());

            for (const auto &arg : signature.args) {
                auto it = signature.time_series_inputs->find(arg);
                if (it == signature.time_series_inputs->end()) {
                    continue;
                }

                const TSMeta *meta = meta_from_ts_signature_object(it->second);
                if (meta == nullptr) {
                    return nullptr;
                }

                fields.emplace_back(arg, meta);
                seen.insert(arg);
            }

            for (const auto &[name, ts_meta_obj] : *signature.time_series_inputs) {
                if (seen.contains(name)) {
                    continue;
                }

                const TSMeta *meta = meta_from_ts_signature_object(ts_meta_obj);
                if (meta == nullptr) {
                    return nullptr;
                }

                fields.emplace_back(name, meta);
            }

            if (fields.empty()) {
                return nullptr;
            }

            return TSTypeRegistry::instance().tsb(fields, fmt::format("__NodeInput_{}", signature.name));
        }

        const TSMeta *error_meta_from_signature(const NodeSignature &signature) {
            if (!signature.capture_exception) {
                return nullptr;
            }

            const value::TypeMeta *node_error_type = value::TypeRegistry::instance().get_by_name("NodeError");
            if (node_error_type == nullptr) {
                return nullptr;
            }
            return TSTypeRegistry::instance().ts(node_error_type);
        }

        const TSMeta *recordable_meta_from_signature(const NodeSignature &signature) {
            auto recordable = signature.recordable_state();
            if (!recordable.has_value()) {
                return nullptr;
            }
            return meta_from_ts_signature_object(*recordable);
        }
    }  // namespace

    NodeBuilder::NodeBuilder(node_signature_s_ptr signature_, nb::dict scalars_,
                             std::optional<input_builder_s_ptr> input_builder_,
                             std::optional<output_builder_s_ptr> output_builder_,
                             std::optional<output_builder_s_ptr> error_builder_,
                             std::optional<output_builder_s_ptr> recordable_state_builder_)
        : signature(std::move(signature_)), scalars(std::move(scalars_)), input_builder(std::move(input_builder_)),
          output_builder(std::move(output_builder_)), error_builder(std::move(error_builder_)),
          recordable_state_builder(std::move(recordable_state_builder_)) {
        if (signature) {
            _input_meta = input_meta_from_signature(*signature);
            _output_meta = signature->time_series_output.has_value()
                ? meta_from_ts_signature_object(signature->time_series_output.value())
                : nullptr;
            _error_meta = error_meta_from_signature(*signature);
            _recordable_state_meta = recordable_meta_from_signature(*signature);
        }
    }

    NodeBuilder::NodeBuilder(NodeBuilder &&other) noexcept
        : signature(other.signature), scalars(std::move(other.scalars)), input_builder(other.input_builder),
          output_builder(other.output_builder), error_builder(other.error_builder),
          recordable_state_builder(other.recordable_state_builder), _input_meta(other._input_meta),
          _output_meta(other._output_meta), _error_meta(other._error_meta),
          _recordable_state_meta(other._recordable_state_meta) {
    }

    NodeBuilder &NodeBuilder::operator=(NodeBuilder &&other) noexcept {
        if (this != &other) {
            // Copy nanobind::ref members (inc_ref) instead of moving them, so both sides stay valid
            signature = other.signature;
            scalars = std::move(other.scalars);
            input_builder = other.input_builder;
            output_builder = other.output_builder;
            error_builder = other.error_builder;
            recordable_state_builder = other.recordable_state_builder;
            _input_meta = other._input_meta;
            _output_meta = other._output_meta;
            _error_meta = other._error_meta;
            _recordable_state_meta = other._recordable_state_meta;
        }
        return *this;
    }

    const TSMeta* NodeBuilder::input_meta() const {
        return _input_meta;
    }

    const TSMeta* NodeBuilder::output_meta() const {
        return _output_meta;
    }

    const TSMeta* NodeBuilder::error_output_meta() const {
        return _error_meta;
    }

    const TSMeta* NodeBuilder::recordable_state_meta() const {
        return _recordable_state_meta;
    }

    void NodeBuilder::release_instance(const node_s_ptr &item) const {
        (void)item;
        // Clean switch: TS endpoints are value-owned on Node; legacy builder instances are no longer created.
        dispose_component(*item);
    }

    size_t NodeBuilder::node_type_size() const {
        // Default implementation returns sizeof(Node)
        // Concrete builders should override this to return the size of their specific node type
        return sizeof(Node);
    }

    size_t NodeBuilder::node_type_alignment() const {
        // Default implementation returns alignof(Node)
        // Concrete builders should override this to return the alignment of their specific node type
        return alignof(Node);
    }

    size_t NodeBuilder::type_alignment() const {
        return node_type_alignment();
    }

    size_t NodeBuilder::memory_size() const {
        // Clean switch: builders now carry schema/contracts only; endpoint storage is embedded in Node.
        return add_canary_size(node_type_size());
    }

    void NodeBuilder::register_with_nanobind(nb::module_ &m) {
        nb::class_ < NodeBuilder, Builder > (m, "NodeBuilder")
                .def("make_instance", &NodeBuilder::make_instance, "owning_graph_id"_a, "node_ndx"_a)
                .def("release_instance", &NodeBuilder::release_instance, "node"_a)
                .def_ro("signature", &NodeBuilder::signature)
                .def_ro("scalars", &NodeBuilder::scalars)
                .def_ro("input_builder", &NodeBuilder::input_builder)
                .def_ro("output_builder", &NodeBuilder::output_builder)
                .def_ro("error_builder", &NodeBuilder::error_builder)
                .def_ro("recordable_state_builder", &NodeBuilder::recordable_state_builder)
                .def("__str__", [](const NodeBuilder &self) {
                    return fmt::format("NodeBuilder@{:p}[sig={}]",
                                       static_cast<const void *>(&self), self.signature->name);
                })
                .def("__repr__", [](const NodeBuilder &self) {
                    return fmt::format("NodeBuilder@{:p}[sig={}]",
                                       static_cast<const void *>(&self), self.signature->name);
                });

        nb::class_<BaseNodeBuilder, NodeBuilder>(m, "BaseNodeBuilder");

        // Register all the extracted builder classes
        python_node_builder_register_with_nanobind(m);
        python_generator_node_builder_register_with_nanobind(m);
        tsd_map_node_builder_register_with_nanobind(m);
        reduce_node_builder_register_with_nanobind(m);
        context_node_builder_register_with_nanobind(m);
        base_nested_graph_node_builder_register_with_nanobind(m);
        nested_graph_node_builder_register_with_nanobind(m);
        component_node_builder_register_with_nanobind(m);
        try_except_node_builder_register_with_nanobind(m);
        switch_node_builder_register_with_nanobind(m);
        tsd_non_associative_reduce_node_builder_register_with_nanobind(m);
        mesh_node_builder_register_with_nanobind(m);
        last_value_pull_node_builder_register_with_nanobind(m);
    }
} // namespace hgraph
