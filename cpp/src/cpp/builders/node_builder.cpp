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

#include <cstdio>
#include <cstdlib>
#include <unordered_set>
#include <utility>

namespace hgraph {
    namespace {
        const value::TypeMeta *scalar_meta_from_signature_object(const nb::object &obj) {
            if (!obj.is_valid() || obj.is_none()) {
                return nullptr;
            }

            if (nb::hasattr(obj, "cpp_type")) {
                try {
                    nb::object cpp_type = nb::getattr(obj, "cpp_type");
                    if (cpp_type.is_valid() && !cpp_type.is_none()) {
                        return nb::cast<const value::TypeMeta *>(cpp_type);
                    }
                } catch (...) {
                    // Fall through to py_type/object fallback.
                }
            }

            if (nb::hasattr(obj, "py_type")) {
                try {
                    nb::object py_type = nb::getattr(obj, "py_type");
                    if (py_type.is_valid() && !py_type.is_none()) {
                        if (const auto *meta = value::TypeRegistry::instance().from_python_type(py_type); meta != nullptr) {
                            return meta;
                        }

                        try {
                            auto value_mod = nb::module_::import_("hgraph._hgraph").attr("value");
                            nb::object resolved = value_mod.attr("get_scalar_type_meta")(py_type);
                            if (resolved.is_valid() && !resolved.is_none()) {
                                return nb::cast<const value::TypeMeta *>(resolved);
                            }
                        } catch (...) {
                            // Fallback below.
                        }
                    }
                } catch (...) {
                    // Fall through to object fallback.
                }
            }

            // Python parity: unknown scalar schemas are represented as object.
            return value::TypeRegistry::instance().get_scalar<nb::object>();
        }

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

                // TSL fallback when element schema can be synthesized but cpp_type on the
                // list metadata is unavailable (for example TSL[TS[type[...]], Size[N]]).
                if (nb::hasattr(obj, "value_tp") && nb::hasattr(obj, "size_tp")) {
                    const TSMeta *element_meta = meta_from_ts_signature_object(nb::getattr(obj, "value_tp"));
                    if (element_meta != nullptr) {
                        size_t fixed_size = 0;
                        nb::object size_tp = nb::getattr(obj, "size_tp");
                        if (size_tp.is_valid() && !size_tp.is_none() && nb::hasattr(size_tp, "py_type")) {
                            nb::object size_type = nb::getattr(size_tp, "py_type");
                            if (size_type.is_valid() && !size_type.is_none() &&
                                nb::hasattr(size_type, "FIXED_SIZE") &&
                                nb::cast<bool>(nb::getattr(size_type, "FIXED_SIZE")) &&
                                nb::hasattr(size_type, "SIZE")) {
                                fixed_size = nb::cast<size_t>(nb::getattr(size_type, "SIZE"));
                            }
                        }
                        return TSTypeRegistry::instance().tsl(element_meta, fixed_size);
                    }
                }

                // TSB fallback for schemas that include fields whose Python metadata
                // does not expose cpp_type (for example TS[Frame[...]] lanes).
                if (nb::hasattr(obj, "bundle_schema_tp")) {
                    nb::object bundle_schema = nb::getattr(obj, "bundle_schema_tp");
                    if (bundle_schema.is_valid() && !bundle_schema.is_none() &&
                        nb::hasattr(bundle_schema, "meta_data_schema")) {
                        std::vector<std::pair<std::string, const TSMeta *>> fields;
                        nb::dict meta_schema = nb::cast<nb::dict>(nb::getattr(bundle_schema, "meta_data_schema"));
                        fields.reserve(meta_schema.size());

                        for (auto item : meta_schema) {
                            std::string name = nb::cast<std::string>(item.first);
                            nb::object child_obj = nb::cast<nb::object>(item.second);
                            const TSMeta *child_meta = meta_from_ts_signature_object(child_obj);
                            if (child_meta == nullptr) {
                                return nullptr;
                            }
                            fields.emplace_back(std::move(name), child_meta);
                        }

                        std::string schema_name = "__AnonymousTSB";
                        nb::object python_type = nb::none();
                        if (nb::hasattr(bundle_schema, "py_type")) {
                            python_type = nb::getattr(bundle_schema, "py_type");
                            if (python_type.is_valid() && !python_type.is_none() && nb::hasattr(python_type, "__name__")) {
                                schema_name = nb::cast<std::string>(nb::getattr(python_type, "__name__"));
                            }
                        }
                        return TSTypeRegistry::instance().tsb(fields, schema_name, python_type);
                    }
                }

                if (nb::hasattr(obj, "value_scalar_tp")) {
                    nb::object scalar_meta_obj = nb::getattr(obj, "value_scalar_tp");
                    if (const auto *scalar_meta = scalar_meta_from_signature_object(scalar_meta_obj); scalar_meta != nullptr) {
                        return TSTypeRegistry::instance().ts(scalar_meta);
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

        const TSMeta *error_meta_from_signature(const NodeSignature &signature, const TSMeta *output_meta) {
            if (!signature.capture_exception) {
                return nullptr;
            }

            const value::TypeMeta *node_error_type = value::TypeRegistry::instance().get_by_name("NodeError");
            if (node_error_type == nullptr) {
                try {
                    auto hgraph_mod = nb::module_::import_("hgraph");
                    nb::object node_error_py_type = hgraph_mod.attr("NodeError");
                    if (node_error_py_type.is_valid() && !node_error_py_type.is_none()) {
                        node_error_type = value::TypeRegistry::instance().from_python_type(node_error_py_type);
                        if (node_error_type == nullptr) {
                            auto value_mod = nb::module_::import_("hgraph._hgraph").attr("value");
                            nb::object resolved = value_mod.attr("get_scalar_type_meta")(node_error_py_type);
                            if (resolved.is_valid() && !resolved.is_none()) {
                                node_error_type = nb::cast<const value::TypeMeta *>(resolved);
                            }
                        }
                    }
                } catch (...) {
                    node_error_type = nullptr;
                }
            }

            if (node_error_type == nullptr) {
                return nullptr;
            }

            const TSMeta *node_error_ts = TSTypeRegistry::instance().ts(node_error_type);
            if (output_meta != nullptr && output_meta->kind == TSKind::TSD) {
                const value::TypeMeta *key_type = output_meta->key_type();
                if (key_type != nullptr) {
                    return TSTypeRegistry::instance().tsd(key_type, node_error_ts);
                }
            }
            return node_error_ts;
        }

        const TSMeta *recordable_meta_from_signature(const NodeSignature &signature) {
            auto recordable = signature.recordable_state();
            if (!recordable.has_value()) {
                return nullptr;
            }
            return meta_from_ts_signature_object(*recordable);
        }

        bool signal_input_has_impl_from_signature_object(const nb::object& obj) {
            if (!obj.is_valid() || obj.is_none()) {
                return false;
            }

            try {
                if (!nb::hasattr(obj, "cpp_type")) {
                    return false;
                }

                nb::object cpp_type = nb::getattr(obj, "cpp_type");
                if (!cpp_type.is_valid() || cpp_type.is_none()) {
                    return false;
                }

                const auto* meta = nb::cast<const TSMeta*>(cpp_type);
                if (meta == nullptr || meta->kind != TSKind::SIGNAL) {
                    return false;
                }

                if (!nb::hasattr(obj, "value_tp")) {
                    return false;
                }

                nb::object value_tp = nb::getattr(obj, "value_tp");
                return value_tp.is_valid() && !value_tp.is_none();
            } catch (...) {
                return false;
            }
        }

        std::vector<bool> signal_input_impl_flags_from_signature(const NodeSignature& signature) {
            std::vector<bool> flags;
            if (!signature.time_series_inputs.has_value() || signature.time_series_inputs->empty()) {
                return flags;
            }

            flags.reserve(signature.time_series_inputs->size());
            std::unordered_set<std::string> seen;
            seen.reserve(signature.time_series_inputs->size());

            for (const auto& arg : signature.args) {
                auto it = signature.time_series_inputs->find(arg);
                if (it == signature.time_series_inputs->end()) {
                    continue;
                }
                flags.push_back(signal_input_has_impl_from_signature_object(it->second));
                seen.insert(arg);
            }

            for (const auto& [name, ts_meta_obj] : *signature.time_series_inputs) {
                if (seen.contains(name)) {
                    continue;
                }
                flags.push_back(signal_input_has_impl_from_signature_object(ts_meta_obj));
            }

            return flags;
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
            _error_meta = error_meta_from_signature(*signature, _output_meta);
            _recordable_state_meta = recordable_meta_from_signature(*signature);
            _signal_input_impl_flags = signal_input_impl_flags_from_signature(*signature);
        }
    }

    NodeBuilder::NodeBuilder(NodeBuilder &&other) noexcept
        : signature(other.signature), scalars(std::move(other.scalars)), input_builder(other.input_builder),
          output_builder(other.output_builder), error_builder(other.error_builder),
          recordable_state_builder(other.recordable_state_builder), _input_meta(other._input_meta),
          _output_meta(other._output_meta), _error_meta(other._error_meta),
          _recordable_state_meta(other._recordable_state_meta),
          _signal_input_impl_flags(other._signal_input_impl_flags) {
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
            _signal_input_impl_flags = other._signal_input_impl_flags;
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

    void NodeBuilder::configure_node_instance(const node_s_ptr& node) const {
        if (!node) {
            return;
        }
        if (std::getenv("HGRAPH_DEBUG_SIGNAL_IMPL") != nullptr && !_signal_input_impl_flags.empty()) {
            std::fprintf(stderr,
                         "[signal_impl] node=%s flags=",
                         signature ? signature->name.c_str() : "<null>");
            for (bool flag : _signal_input_impl_flags) {
                std::fprintf(stderr, "%d", flag ? 1 : 0);
            }
            std::fprintf(stderr, "\n");
        }
        node->set_signal_input_impl_flags(_signal_input_impl_flags);
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
