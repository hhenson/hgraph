/**
 * Wiring-domain implementation + bindings: the leaked graph-fn/WiredFn/node
 * registries, erased WiringArg assembly, the python graph-fn WiredFn ops,
 * and bind_wiring() (Wiring/Run, node_ref/graph_fn, switch/dispatch cases,
 * feedback, component, _evaluate_const).
 */
#include "py_wiring.h"
#include "py_bindings.h"

#include <hgraph/runtime/registry_snapshot.h>

#include <spdlog/sinks/base_sink.h>

#include <mutex>

namespace nb = nanobind;
using namespace hgraph;
using namespace hgraph::python_bridge;

namespace
{
    constexpr char retained_run_capsule_name[] = "hgraph.failed_run";

    [[nodiscard]] std::string retained_error_message(
        const std::exception_ptr &error)
    {
        try
        {
            if (error != nullptr) { std::rethrow_exception(error); }
        }
        catch (const std::exception &caught)
        {
            return caught.what();
        }
        catch (...)
        {
            return "unknown graph run error";
        }
        return "graph run failed";
    }

    void release_retained_run(PyObject *capsule) noexcept
    {
        auto *run = static_cast<std::shared_ptr<PyRun> *>(
            PyCapsule_GetPointer(capsule, retained_run_capsule_name));
        if (run == nullptr)
        {
            PyErr_Clear();
            return;
        }
        delete run;
    }

    [[nodiscard]] spdlog::level::level_enum python_to_spd_level(int level) noexcept
    {
        if (level >= 50) { return spdlog::level::critical; }
        if (level >= 40) { return spdlog::level::err; }
        if (level >= 30) { return spdlog::level::warn; }
        if (level >= 20) { return spdlog::level::info; }
        if (level >= 10) { return spdlog::level::debug; }
        return spdlog::level::trace;
    }

    [[nodiscard]] int spd_to_python_level(spdlog::level::level_enum level) noexcept
    {
        switch (level)
        {
        case spdlog::level::trace: return 5;
        case spdlog::level::debug: return 10;
        case spdlog::level::info: return 20;
        case spdlog::level::warn: return 30;
        case spdlog::level::err: return 40;
        case spdlog::level::critical: return 50;
        case spdlog::level::off: return 100;
        case spdlog::level::n_levels: break;
        }
        return 20;
    }

    class PythonLoggingSink final : public spdlog::sinks::base_sink<std::mutex>
    {
      public:
        PythonLoggingSink(nb::object logger, nb::object formatter)
            : logger_(std::move(logger)), formatter_(std::move(formatter))
        {
        }

        void log_with_context(spdlog::level::level_enum level,
                              std::string_view message,
                              std::string_view node_path)
        {
            nb::gil_scoped_acquire gil;
            nb::object child = logger_.attr("getChild")(nb::str(node_path.data(), node_path.size()));
            const int python_level = spd_to_python_level(level);
            nb::str text{message.data(), message.size()};
            if (formatter_.is_none())
            {
                child.attr("log")(python_level, text);
                return;
            }
            formatter_(python_level, text, nb::tuple(),
                       nb::arg("node_path") = nb::str(node_path.data(), node_path.size()),
                       nb::arg("__orig_log__") = child.attr("_log"));
        }

      protected:
        void sink_it_(const spdlog::details::log_msg &message) override
        {
            nb::gil_scoped_acquire gil;
            logger_.attr("log")(
                spd_to_python_level(message.level),
                nb::str(message.payload.data(), message.payload.size()));
        }

        void flush_() override {}

      private:
        nb::object logger_;
        nb::object formatter_;
    };

    class PythonRunLogger final : public spdlog::logger
    {
      public:
        explicit PythonRunLogger(std::shared_ptr<PythonLoggingSink> sink)
            : spdlog::logger("hgraph.python.run", sink), sink_(std::move(sink))
        {
        }

        void log_with_context(spdlog::level::level_enum level,
                              std::string_view message,
                              NodePtr node)
        {
            if (!node.has_value())
            {
                spdlog::logger::log(
                    spdlog::source_loc{}, level,
                    spdlog::string_view_t{message.data(), message.size()});
                return;
            }
            sink_->log_with_context(level, message,
                                    diagnostic::node_path(NodeView{node}));
        }

      private:
        std::shared_ptr<PythonLoggingSink> sink_;
    };

    namespace
    {
        void emit_python_run_log(spdlog::logger &logger,
                                 spdlog::level::level_enum level,
                                 std::string_view message,
                                 NodePtr node)
        {
            static_cast<PythonRunLogger &>(logger).log_with_context(level, message, node);
        }
    }

    class PythonLifecycleObserver final : public LifecycleObserver
    {
      public:
        explicit PythonLifecycleObserver(nb::object observer)
            : observer_(observer.release().ptr())
        {
        }

        ~PythonLifecycleObserver() override
        {
            nb::gil_scoped_acquire gil;
            nb::steal(nb::handle(observer_));
        }

        void on_before_start_graph(const GraphView &graph) override
        {
            invoke_graph("on_before_start_graph", graph);
        }
        void on_after_start_graph(const GraphView &graph) override
        {
            invoke_graph("on_after_start_graph", graph);
        }
        void on_start_graph_failed(const GraphView &graph) override
        {
            invoke_graph("on_start_graph_failed", graph);
        }
        void on_before_start_node(const NodeView &node) override
        {
            invoke_node("on_before_start_node", node);
        }
        void on_after_start_node(const NodeView &node) override
        {
            invoke_node("on_after_start_node", node);
        }
        void on_start_node_failed(const NodeView &node) override
        {
            invoke_node("on_start_node_failed", node);
        }
        void on_before_graph_evaluation(const GraphView &graph) override
        {
            invoke_graph("on_before_graph_evaluation", graph);
        }
        void on_after_graph_evaluation(const GraphView &graph) override
        {
            invoke_graph("on_after_graph_evaluation", graph);
        }
        void on_before_node_evaluation(const NodeView &node) override
        {
            invoke_node("on_before_node_evaluation", node);
        }
        void on_after_node_evaluation(const NodeView &node) override
        {
            invoke_node("on_after_node_evaluation", node);
        }
        void on_after_graph_push_nodes_evaluation(const GraphView &graph) override
        {
            invoke_graph("on_after_graph_push_nodes_evaluation", graph);
        }
        void on_before_stop_node(const NodeView &node) override
        {
            invoke_node("on_before_stop_node", node);
        }
        void on_after_stop_node(const NodeView &node) override
        {
            invoke_node("on_after_stop_node", node);
        }
        void on_stop_node_failed(const NodeView &node) override
        {
            invoke_node("on_stop_node_failed", node);
        }
        void on_before_stop_graph(const GraphView &graph) override
        {
            invoke_graph("on_before_stop_graph", graph);
        }
        void on_after_stop_graph(const GraphView &graph) override
        {
            invoke_graph("on_after_stop_graph", graph);
        }
        void on_stop_graph_failed(const GraphView &graph) override
        {
            invoke_graph("on_stop_graph_failed", graph);
        }

      private:
        template <typename Subject>
        void invoke(std::string_view method, Subject subject)
        {
            try
            {
                const nb::handle observer{observer_};
                if (!nb::hasattr(observer, method.data())) { return; }
                auto guard = std::make_shared<PyTsGuard>();
                PyTsLease lease{
                    .guard = guard,
                    .generation = ++guard->generation,
                    .owns_guard_lifetime = true,
                };
                auto invalidate = UnwindCleanupGuard([&] { lease.invalidate(); });
                observer.attr(method.data())(nb::cast(subject(lease)));
                invalidate.release();
                lease.invalidate();
            }
            catch (const nb::python_error &error)
            {
                throw std::runtime_error(error.what());
            }
        }

        void invoke_graph(std::string_view method, const GraphView &graph)
        {
            invoke(method, [&](const PyTsLease &lease) {
                return PyGraph{graph.pointer(), lease};
            });
        }

        void invoke_node(std::string_view method, const NodeView &node)
        {
            invoke(method, [&](const PyTsLease &lease) {
                return PyNode{node.pointer(), NodeScheduler{}, lease};
            });
        }

        PyObject *observer_{nullptr};
    };

    /** Immortal per-function records (stable context pointers; keyed by the
        user function object per the identity ruling). */
    [[nodiscard]] std::unordered_map<PyObject *, PyGraphFnRecord *> &py_graph_fn_registry()
    {
        static auto *registry = new std::unordered_map<PyObject *, PyGraphFnRecord *>{};
        return *registry;
    }

    // fn<X>() erases at a template instantiation point, so runtime names
    // resolve through a pre-instantiated table of the stdlib markers usable
    // as higher-order callables.
    [[nodiscard]] const std::unordered_map<std::string_view, WiredFn> &wired_fn_table()
    {
        static const auto *table = new std::unordered_map<std::string_view, WiredFn>{
            {"add_", fn<stdlib::add_>()},   {"sub_", fn<stdlib::sub_>()},
            {"mul_", fn<stdlib::mul_>()},   {"div_", fn<stdlib::div_>()},
            {"min_", fn<stdlib::min_>()},   {"max_", fn<stdlib::max_>()},
            {"bit_and", fn<stdlib::bit_and>()}, {"bit_or", fn<stdlib::bit_or>()},
            {"bit_xor", fn<stdlib::bit_xor>()}, {"union", fn<stdlib::union_>()},
            {"merge", fn<stdlib::merge>()}, {"eq_", fn<stdlib::eq_>()},
            {"not_", fn<stdlib::not_>()},   {"neg_", fn<stdlib::neg_>()},
            {"abs_", fn<stdlib::abs_>()},   {"str_", fn<stdlib::str_>()},
        };
        return *table;
    }

    struct RuntimeOperatorRecord
    {
        std::string name;
        std::vector<std::string> names;
        std::vector<std::string_view> name_views;
        std::size_t arity{0};
        bool variadic{false};
        bool has_output{false};
        const TSValueTypeMetaData *expected_output{nullptr};
    };

    [[nodiscard]] std::unordered_map<std::string, RuntimeOperatorRecord *> &runtime_operator_registry()
    {
        static auto *registry = new std::unordered_map<std::string, RuntimeOperatorRecord *>{};
        return *registry;
    }

    [[nodiscard]] WiringPortRef runtime_operator_wire(const void *context, Wiring &w,
                                                      std::span<const WiringPortRef> args)
    {
        const auto &record = *static_cast<const RuntimeOperatorRecord *>(context);
        return wire_erased_operator(
            w, record.name, args, record.has_output, record.expected_output);
    }

    [[nodiscard]] CompiledSubGraph runtime_operator_compile(
        const void *context, Wiring *parent,
        std::span<const TSValueTypeMetaData *const> input_schemas)
    {
        Wiring child = parent != nullptr ? parent->child_wiring()
                                         : Wiring{WiringKind::SubGraph};
        std::vector<const TSValueTypeMetaData *> schemas{input_schemas.begin(), input_schemas.end()};
        std::vector<WiringPortRef> boundary;
        boundary.reserve(input_schemas.size());
        for (std::size_t index = 0; index < input_schemas.size(); ++index)
        {
            boundary.push_back(WiringPortRef::boundary_source(index, {}, input_schemas[index]));
        }
        const auto &record = *static_cast<const RuntimeOperatorRecord *>(context);
        auto compile = [&] {
            WiringPortRef out = runtime_operator_wire(context, child, boundary);
            if (record.has_output)
            {
                return std::move(child).finish_subgraph(out, std::move(schemas));
            }
            return std::move(child).finish_subgraph(std::nullopt,
                                                    std::move(schemas));
        };
        if (!child.has_wiring_observers()) { return compile(); }
        return child.observe(
            WiringScopeEvent{
                .kind = WiringScopeKind::NestedGraph,
                .label = record.name,
                .signature = record.name,
            },
            compile);
    }

    [[nodiscard]] const WiredFnOps &runtime_operator_ops()
    {
        static constexpr WiredFnOps ops{
            &runtime_operator_wire,
            &runtime_operator_compile,
            [](const void *context) {
                const auto &record = *static_cast<const RuntimeOperatorRecord *>(context);
                return std::span<const std::string_view>{
                    record.name_views.data(), record.name_views.size()};
            },
            [](const void *, std::size_t) -> const TSValueTypeMetaData * { return nullptr; },
            nullptr,
            [](const void *context) -> const TSValueTypeMetaData * {
                return static_cast<const RuntimeOperatorRecord *>(context)->expected_output;
            },
            [](const void *context) -> std::string_view {
                return static_cast<const RuntimeOperatorRecord *>(context)->name;
            },
        };
        return ops;
    }

    [[nodiscard]] WiredFn runtime_operator_fn(
        const std::string &name, const TSValueTypeMetaData *expected_output = nullptr)
    {
        const auto shape = OperatorRegistry::instance().callable_shape(name);
        if (!shape.has_value())
        {
            throw std::invalid_argument(
                "operator '" + name +
                "' has no single time-series-only callable shape for higher-order use");
        }

        if (expected_output != nullptr)
        {
            auto *record = new RuntimeOperatorRecord{};
            record->name = name;
            record->names = shape->parameter_names;
            record->name_views.reserve(record->names.size());
            for (const std::string &parameter : record->names)
            {
                record->name_views.push_back(parameter);
            }
            record->arity = shape->arity;
            record->variadic = shape->variadic;
            record->has_output = shape->has_output;
            record->expected_output = expected_output;
            return WiredFn{
                .ops           = &runtime_operator_ops(),
                .context       = record,
                .identity      = &typeid(RuntimeOperatorRecord),
                .operator_name = record->name,
                .arity         = record->arity,
                .variadic      = record->variadic,
                .has_output    = record->has_output,
            };
        }

        auto &registry = runtime_operator_registry();
        auto  found    = registry.find(name);
        if (found == registry.end())
        {
            auto *record = new RuntimeOperatorRecord{};
            record->name = name;
            record->names = shape->parameter_names;
            record->name_views.reserve(record->names.size());
            for (const std::string &parameter : record->names)
            {
                record->name_views.push_back(parameter);
            }
            record->arity = shape->arity;
            record->variadic = shape->variadic;
            record->has_output = shape->has_output;
            found = registry.emplace(name, record).first;
        }
        return WiredFn{
            .ops           = &runtime_operator_ops(),
            .context       = found->second,
            .identity      = &typeid(RuntimeOperatorRecord),
            .operator_name = found->second->name,
            .arity         = found->second->arity,
            .variadic      = found->second->variadic,
            .has_output    = found->second->has_output,
        };
    }

    /** Immortal callable records (stable scalar identity by pointer). */
    [[nodiscard]] std::unordered_map<PyObject *, PyNodeRecord *> &py_node_registry()
    {
        static auto *registry = new std::unordered_map<PyObject *, PyNodeRecord *>{};
        return *registry;
    }

    class PyWiringCleanup final
    {
      public:
        explicit PyWiringCleanup(nb::object callback)
            : callback_(callback.release().ptr())
        {
        }

        PyWiringCleanup(const PyWiringCleanup &) = delete;
        PyWiringCleanup &operator=(const PyWiringCleanup &) = delete;

        ~PyWiringCleanup() noexcept
        {
            if (callback_ == nullptr) { return; }
            nb::gil_scoped_acquire gil;
            try
            {
                nb::borrow<nb::object>(nb::handle(callback_))();
            }
            catch (nb::python_error &error)
            {
                error.discard_as_unraisable("Wiring extension cleanup");
            }
            Py_DECREF(callback_);
        }

      private:
        PyObject *callback_{nullptr};
    };

    class PyWiringExtensionState final
    {
      public:
        PyWiringExtensionState() = default;
        PyWiringExtensionState(const PyWiringExtensionState &) = delete;
        PyWiringExtensionState &operator=(const PyWiringExtensionState &) = delete;

        ~PyWiringExtensionState() noexcept
        {
            nb::gil_scoped_acquire gil;
            for (const auto &[key, value] : values_)
            {
                static_cast<void>(key);
                Py_DECREF(value);
            }
        }

        [[nodiscard]] nb::object acquire(const std::string &key, const nb::object &factory)
        {
            if (const auto found = values_.find(key); found != values_.end())
            {
                return nb::borrow<nb::object>(nb::handle(found->second));
            }
            nb::object value = factory();
            PyObject  *raw   = value.ptr();
            values_.emplace(key, raw);
            static_cast<void>(value.release());
            return nb::borrow<nb::object>(nb::handle(raw));
        }

      private:
        std::unordered_map<std::string, PyObject *> values_{};
    };

    [[nodiscard]] WiringPortRef py_graph_fn_wire(const void *context, Wiring &w,
                                                 std::span<const WiringPortRef> args)
    {
        const auto &record = *static_cast<const PyGraphFnRecord *>(context);
        nb::gil_scoped_acquire gil;
        nb::list ports;
        for (std::size_t index = 0; index < args.size(); ++index)
        {
            const auto *expected = index < record.input_schemas.size()
                                       ? record.input_schemas[index]
                                       : nullptr;
            ports.append(nb::cast(PyPort{
                graph_wiring_detail::adapt_source_for_input(w, expected, args[index])}));
        }
        nb::object borrowed = nb::cast(PyWiring::borrow(w));
        nb::object result   = record.wrapper(borrowed, nb::tuple(ports));
        if (result.is_none()) { return {}; }
        return nb::cast<PyPort &>(result).ref;
    }

    [[nodiscard]] CompiledSubGraph py_graph_fn_compile(const void *context,
                                                       Wiring *parent,
                                                       std::span<const TSValueTypeMetaData *const> input_schemas)
    {
        const auto &record = *static_cast<const PyGraphFnRecord *>(context);
        if (input_schemas.size() != record.arity)
        {
            throw std::invalid_argument("python graph fn: compiled input schema count does not match its inputs");
        }
        Wiring child = parent != nullptr ? parent->child_wiring()
                                         : Wiring{WiringKind::SubGraph};
        std::vector<const TSValueTypeMetaData *> schemas{input_schemas.begin(), input_schemas.end()};
        std::vector<WiringPortRef> boundary;
        boundary.reserve(input_schemas.size());
        for (std::size_t index = 0; index < input_schemas.size(); ++index)
        {
            boundary.push_back(WiringPortRef::boundary_source(index, {}, input_schemas[index]));
        }
        WiringPortRef out = py_graph_fn_wire(
            context, child, {boundary.data(), boundary.size()});
        // The Python wrapper emits the nested graph scope while invoking the
        // callable. The call result is authoritative: an unannotated lambda
        // is provisionally output-producing but may compile to an actual sink.
        if (out.schema != nullptr)
        {
            return std::move(child).finish_subgraph(out, std::move(schemas));
        }
        return std::move(child).finish_subgraph(std::nullopt,
                                                std::move(schemas));
    }

    [[nodiscard]] const WiredFnOps &py_graph_fn_ops()
    {
        static constexpr WiredFnOps ops{
            &py_graph_fn_wire,
            &py_graph_fn_compile,
            [](const void *context) {
                const auto &record = *static_cast<const PyGraphFnRecord *>(context);
                return std::span<const std::string_view>{record.names.data(), record.names.size()};
            },
            [](const void *context, std::size_t index) -> const TSValueTypeMetaData * {
                const auto &record = *static_cast<const PyGraphFnRecord *>(context);
                return index < record.input_schemas.size() ? record.input_schemas[index] : nullptr;
            },
            [](const void *context, std::size_t index) -> std::optional<TypePattern> {
                const auto &record = *static_cast<const PyGraphFnRecord *>(context);
                return index < record.input_patterns.size() ? record.input_patterns[index]
                                                            : std::nullopt;
            },
            [](const void *context) {
                // Known when the python fn carries a TS return annotation
                // (mesh_ learns its element type this way); else null.
                return static_cast<const PyGraphFnRecord *>(context)->output_schema;
            },
            [](const void *context) -> std::string_view {
                return static_cast<const PyGraphFnRecord *>(context)->diagnostic_label;
            },
        };
        return ops;
    }
}  // namespace

namespace hgraph::python_bridge
{
    const LoggerOps &python_run_logger_ops() noexcept
    {
        static const LoggerOps ops{.emit_impl = &emit_python_run_log};
        return ops;
    }

    RetainedGraphRunError::RetainedGraphRunError(
        std::exception_ptr error, std::shared_ptr<PyRun> run)
        : std::runtime_error(retained_error_message(error)),
          run_(std::move(run))
    {
    }

    void translate_retained_graph_run_error(
        const RetainedGraphRunError &error)
    {
        auto *owner = new std::shared_ptr<PyRun>{error.run()};
        PyObject *capsule = PyCapsule_New(
            owner, retained_run_capsule_name, &release_retained_run);
        if (capsule == nullptr)
        {
            delete owner;
            return;
        }

        PyObject *message = PyUnicode_FromString(error.what());
        PyObject *instance = message != nullptr
                                 ? PyObject_CallFunctionObjArgs(
                                       PyExc_RuntimeError, message, nullptr)
                                 : nullptr;
        Py_XDECREF(message);
        if (instance == nullptr)
        {
            Py_DECREF(capsule);
            return;
        }
        if (PyObject_SetAttrString(instance, "_hgraph_failed_run", capsule) != 0)
        {
            Py_DECREF(capsule);
            Py_DECREF(instance);
            return;
        }
        Py_DECREF(capsule);
        PyErr_SetObject(PyExc_RuntimeError, instance);
        Py_DECREF(instance);
    }

    std::shared_ptr<spdlog::logger> make_python_run_logger(
        nb::object logger, int python_level, nb::object formatter)
    {
        if (logger.is_none())
        {
            logger = nb::module_::import_("logging").attr("getLogger")("hgraph");
        }
        auto sink = std::make_shared<PythonLoggingSink>(std::move(logger),
                                                        std::move(formatter));
        auto result = std::make_shared<PythonRunLogger>(std::move(sink));
        result->set_level(python_to_spd_level(python_level));
        return result;
    }

    void add_python_lifecycle_observers(
        GraphExecutorBuilder &builder,
        std::vector<std::unique_ptr<LifecycleObserver>> &owned,
        nb::tuple observers)
    {
        owned.reserve(nb::len(observers));
        for (nb::handle observer : observers)
        {
            if (observer.is_none())
            {
                throw nb::type_error("life_cycle_observers cannot contain None");
            }
            if (nb::isinstance<EvaluationTrace>(observer))
            {
                owned.push_back(std::make_unique<EvaluationTrace>(
                    nb::cast<const EvaluationTrace &>(observer)));
            }
            else if (nb::isinstance<EvaluationProfiler>(observer))
            {
                owned.push_back(std::make_unique<EvaluationProfiler>(
                    nb::cast<const EvaluationProfiler &>(observer)));
            }
            else if (nb::isinstance<GraphDiagnostics>(observer))
            {
                owned.push_back(std::make_unique<GraphDiagnostics>(
                    nb::cast<const GraphDiagnostics &>(observer)));
            }
            else
            {
                owned.push_back(std::make_unique<PythonLifecycleObserver>(
                    nb::borrow<nb::object>(observer)));
            }
            builder.add_lifecycle_observer(owned.back().get());
        }
    }

    void configure_python_wiring_observers(
        Wiring &wiring,
        std::vector<nb::object> &borrowed,
        std::unique_ptr<WiringTracer> &tracer,
        nb::object trace_wiring,
        nb::tuple observers)
    {
        if (tracer != nullptr || !borrowed.empty())
        {
            throw std::logic_error("wiring observers are already configured");
        }

        if (!trace_wiring.is_none())
        {
            if (nb::isinstance<nb::bool_>(trace_wiring))
            {
                if (nb::cast<bool>(trace_wiring))
                {
                    tracer = std::make_unique<WiringTracer>();
                }
            }
            else if (nb::isinstance<nb::dict>(trace_wiring))
            {
                nb::dict options = nb::cast<nb::dict>(trace_wiring);
                const std::string filter = options.contains("filter") &&
                                                   !options["filter"].is_none()
                                               ? nb::cast<std::string>(options["filter"])
                                               : std::string{};
                const bool graph = !options.contains("graph") ||
                                   nb::cast<bool>(options["graph"]);
                const bool node = !options.contains("node") ||
                                  nb::cast<bool>(options["node"]);
                tracer = std::make_unique<WiringTracer>(filter, graph, node);
            }
            else
            {
                throw nb::type_error(
                    "trace_wiring must be bool, dict, or None");
            }
            if (tracer != nullptr) { wiring.add_wiring_observer(tracer.get()); }
        }

        borrowed.reserve(nb::len(observers));
        for (nb::handle observer : observers)
        {
            if (observer.is_none())
            {
                throw nb::type_error("wiring_observers cannot contain None");
            }
            if (!nb::isinstance<WiringTracer>(observer))
            {
                throw nb::type_error(
                    "wiring_observers currently accepts native WiringTracer instances only");
            }
            borrowed.push_back(nb::borrow<nb::object>(observer));
            wiring.add_wiring_observer(nb::cast<WiringTracer *>(observer));
        }
    }

    [[nodiscard]] std::vector<WiringArg> build_args(nb::tuple args, nb::dict kwargs)
    {
        std::vector<WiringArg> out;
        out.reserve(nb::len(args) + nb::len(kwargs));
        const auto push = [&](nb::handle object, std::string name) {
            WiringArg arg;
            arg.name = std::move(name);
            if (object.is_none())
            {
                // None is an absent wiring-time scalar. The overload matcher
                // deliberately permits it for scalar variables without
                // binding their type, then forwards it to Python as None.
                arg.kind = WiringArg::Kind::Scalar;
            }
            else if (nb::isinstance<PyPort>(object))
            {
                arg.kind = WiringArg::Kind::TimeSeries;
                arg.port = nb::cast<PyPort &>(object).ref;
            }
            else if (nb::isinstance<PyTsType>(object))
            {
                arg.kind         = WiringArg::Kind::Scalar;
                arg.scalar_value = Value{PyTsMetaRef{nb::cast<PyTsType &>(object).meta}};
                arg.scalar_meta  = arg.scalar_value.schema();
            }
            else if (nb::isinstance<PyWiredFn>(object))
            {
                arg.kind         = WiringArg::Kind::Scalar;
                arg.scalar_value = Value{nb::cast<PyWiredFn &>(object).fn};
                arg.scalar_meta  = arg.scalar_value.schema();
            }
            else if (nb::isinstance<PyScalarValue>(object))
            {
                arg.kind         = WiringArg::Kind::Scalar;
                arg.scalar_value = nb::cast<PyScalarValue &>(object).value;
                arg.scalar_meta  = arg.scalar_value.schema();
            }
            else if (nb::isinstance<PyNodeHandle>(object))
            {
                arg.kind         = WiringArg::Kind::Scalar;
                arg.scalar_value = Value{PyNodeRef{nb::cast<PyNodeHandle &>(object).record}};
                arg.scalar_meta  = arg.scalar_value.schema();
            }
            else if (nb::isinstance<PySwitchCases>(object))
            {
                arg.kind         = WiringArg::Kind::Scalar;
                arg.scalar_value = Value{nb::cast<PySwitchCases &>(object).cases};
                arg.scalar_meta  = arg.scalar_value.schema();
            }
            else if (nb::isinstance<PyDispatchCases>(object))
            {
                arg.kind         = WiringArg::Kind::Scalar;
                arg.scalar_value = Value{nb::cast<PyDispatchCases &>(object).cases};
                arg.scalar_meta  = arg.scalar_value.schema();
            }
            else
            {
                arg.kind         = WiringArg::Kind::Scalar;
                arg.scalar_value = py_to_value(object);
                arg.scalar_meta  = arg.scalar_value.schema();
            }
            out.push_back(std::move(arg));
        };
        for (nb::handle object : args) { push(object, {}); }
        for (auto [key, object] : kwargs) { push(object, nb::cast<std::string>(key)); }
        return out;
    }

    void bind_wiring(nb::module_ &m)
    {
    python_conversion_traits<WiredFn>::to_python_hook() = [](const WiredFn &value) {
        return nb::cast(PyWiredFn{value});
    };
    python_conversion_traits<WiredFn>::from_python_hook() = [](nb::handle source) {
        if (!nb::isinstance<PyWiredFn>(source))
        {
            throw nb::type_error("expected a WiredFn value");
        }
        return nb::cast<PyWiredFn &>(source).fn;
    };

    nb::class_<PyRun>(
        m, "Run",
        "The completed result of a native graph execution.\n\n"
        "Use recorded() to retrieve values captured through the runner's "
        "record/replay support.")
        .def("recorded", &PyRun::recorded, nb::arg("key"),
             nb::arg("sparse") = false,
             "Return values recorded under key. Sparse mode preserves only "
             "ticks; dense mode aligns values to evaluation cycles.");
    m.def("operator_names", [] { return OperatorRegistry::instance().registered_names(); },
          "Return the names currently registered in the native operator registry.");

    nb::class_<EvaluationTrace>(
        m, "EvaluationTrace",
        "Configure textual tracing for graph and node lifecycle events.\n\n"
        "Pass an instance as GraphConfiguration.trace. The optional filter "
        "limits output to matching graph and node paths.")
        .def(nb::init<std::optional<std::string>, bool, bool, bool, bool, bool>(),
             nb::arg("filter").none() = nb::none(), nb::arg("start") = true,
             nb::arg("eval") = true, nb::arg("stop") = true,
             nb::arg("node") = true, nb::arg("graph") = true)
        .def_static("set_print_all_values", &EvaluationTrace::set_print_all_values,
                    nb::arg("value"),
                    "Globally include or omit valid input values that did not "
                    "tick in node trace output.")
        .def_static("set_use_logger", &EvaluationTrace::set_use_logger,
                    nb::arg("value"),
                    "Globally select logging instead of direct trace output.");

    nb::class_<EvaluationProfilePhase>(
        m, "EvaluationProfilePhase",
        "Aggregated timing and failure counts for one lifecycle phase.")
        .def_ro("count", &EvaluationProfilePhase::count)
        .def_ro("failures", &EvaluationProfilePhase::failures)
        .def_ro("total_time", &EvaluationProfilePhase::total_time)
        .def_ro("max_time", &EvaluationProfilePhase::max_time)
        .def_ro("recent_time", &EvaluationProfilePhase::recent_time);
    nb::class_<EvaluationProfileEntry>(
        m, "EvaluationProfileEntry",
        "A profiler entry for one graph or node path, split into start, "
        "evaluation, and stop phases.")
        .def_ro("path", &EvaluationProfileEntry::path)
        .def_ro("label", &EvaluationProfileEntry::label)
        .def_ro("graph", &EvaluationProfileEntry::graph)
        .def_ro("start", &EvaluationProfileEntry::start)
        .def_ro("evaluation", &EvaluationProfileEntry::evaluation)
        .def_ro("stop", &EvaluationProfileEntry::stop);
    nb::class_<EvaluationProfileSnapshot>(
        m, "EvaluationProfileSnapshot",
        "An immutable point-in-time summary captured by EvaluationProfiler.")
        .def_ro("graph_cycles", &EvaluationProfileSnapshot::graph_cycles)
        .def_ro("wall_time", &EvaluationProfileSnapshot::wall_time)
        .def_ro("root_evaluation_time", &EvaluationProfileSnapshot::root_evaluation_time)
        .def_ro("scheduling_lag_total", &EvaluationProfileSnapshot::scheduling_lag_total)
        .def_ro("scheduling_lag_max", &EvaluationProfileSnapshot::scheduling_lag_max)
        .def_ro("scheduling_lag_samples", &EvaluationProfileSnapshot::scheduling_lag_samples)
        .def_ro("runtime_load", &EvaluationProfileSnapshot::runtime_load)
        .def_ro("entries", &EvaluationProfileSnapshot::entries);
    nb::class_<EvaluationProfiler>(
        m, "EvaluationProfiler",
        "Collect graph and node lifecycle timing without retaining runtime "
        "objects.\n\n"
        "Pass an instance as GraphConfiguration.profile. snapshot() is safe "
        "after the run completes.")
        .def(nb::init<bool, bool, bool, bool, bool, std::size_t>(),
             nb::arg("start") = true, nb::arg("eval") = true,
             nb::arg("stop") = true, nb::arg("node") = true,
             nb::arg("graph") = true, nb::arg("recent_window") = 100)
        .def("snapshot", &EvaluationProfiler::snapshot,
             "Return an immutable snapshot of the currently collected metrics.")
        .def("reset", &EvaluationProfiler::reset,
             "Clear all collected counts and timings.");

    nb::enum_<GraphDiagnosticEntityKind>(
        m, "GraphDiagnosticEntityKind",
        "Whether a diagnostics entry describes a graph or a node.")
        .value("GRAPH", GraphDiagnosticEntityKind::Graph)
        .value("NODE", GraphDiagnosticEntityKind::Node);
    nb::class_<NodeStorageMetrics>(
        m, "NodeStorageMetrics",
        "Static, nested-graph, and dynamic storage metrics for one node.")
        .def_ro("static_bytes", &NodeStorageMetrics::static_bytes)
        .def_ro("nested_graph_count", &NodeStorageMetrics::nested_graph_count)
        .def_ro("nested_graph_capacity", &NodeStorageMetrics::nested_graph_capacity)
        .def_ro("nested_graph_blocks", &NodeStorageMetrics::nested_graph_blocks)
        .def_ro("dynamic_live_bytes", &NodeStorageMetrics::dynamic_live_bytes)
        .def_ro("dynamic_reserved_bytes", &NodeStorageMetrics::dynamic_reserved_bytes);
    nb::class_<NodeInspectionMetrics>(
        m, "NodeInspectionMetrics",
        "Optional cold-path inspection metrics reported by a node.")
        .def_ro("pending_items", &NodeInspectionMetrics::pending_items);
    nb::class_<GraphDiagnosticTarget>(
        m, "GraphDiagnosticTarget",
        "A resolved connection from a source time-series path to a target "
        "node input path.")
        .def_ro("source_path", &GraphDiagnosticTarget::source_path)
        .def_ro("node_id", &GraphDiagnosticTarget::node_id)
        .def_ro("target_path", &GraphDiagnosticTarget::target_path);
    nb::class_<GraphDiagnosticValue>(
        m, "GraphDiagnosticValue",
        "A serialized diagnostic view of an input, output, or scalar value.\n\n"
        "Value capture is optional. Check available before reading json or "
        "frame; error and table_error explain failed conversions.")
        .def_ro("available", &GraphDiagnosticValue::available)
        .def_ro("valid", &GraphDiagnosticValue::valid)
        .def_ro("last_modified", &GraphDiagnosticValue::last_modified)
        .def_ro("schema_label", &GraphDiagnosticValue::schema_label)
        .def_ro("json", &GraphDiagnosticValue::json)
        .def_ro("error", &GraphDiagnosticValue::error)
        .def_ro("table_error", &GraphDiagnosticValue::table_error)
        .def_prop_ro("has_frame", [](const GraphDiagnosticValue &value) {
            return value.frame.has_value();
        })
        .def_prop_ro("frame", [](const GraphDiagnosticValue &value) {
            return value.frame.has_value()
                       ? frame_to_py(value.frame)
                       : nb::none();
        })
        .def_ro("target_node_ids", &GraphDiagnosticValue::target_node_ids)
        .def_ro("targets", &GraphDiagnosticValue::targets)
        .def_ro("bound_targets", &GraphDiagnosticValue::bound_targets);
    nb::class_<GraphDiagnosticEntry>(
        m, "GraphDiagnosticEntry",
        "One graph or node in a GraphDiagnosticsSnapshot, including "
        "lifecycle, storage, connection, and optional value information.")
        .def_ro("id", &GraphDiagnosticEntry::id)
        .def_ro("parent_id", &GraphDiagnosticEntry::parent_id)
        .def_ro("children", &GraphDiagnosticEntry::children)
        .def_ro("path", &GraphDiagnosticEntry::path)
        .def_ro("label", &GraphDiagnosticEntry::label)
        .def_ro("schema_label", &GraphDiagnosticEntry::schema_label)
        .def_ro("implementation_label", &GraphDiagnosticEntry::implementation_label)
        .def_ro("kind", &GraphDiagnosticEntry::kind)
        .def_ro("node_kind", &GraphDiagnosticEntry::node_kind)
        .def_ro("node_index", &GraphDiagnosticEntry::node_index)
        .def_ro("started", &GraphDiagnosticEntry::started)
        .def_ro("stopped", &GraphDiagnosticEntry::stopped)
        .def_ro("evaluation_time", &GraphDiagnosticEntry::evaluation_time)
        .def_ro("scheduled_time", &GraphDiagnosticEntry::scheduled_time)
        .def_ro("storage", &GraphDiagnosticEntry::storage)
        .def_ro("peak_storage", &GraphDiagnosticEntry::peak_storage)
        .def_ro("inspection", &GraphDiagnosticEntry::inspection)
        .def_ro("input", &GraphDiagnosticEntry::input)
        .def_ro("output", &GraphDiagnosticEntry::output)
        .def_ro("scalars", &GraphDiagnosticEntry::scalars)
        .def_ro("start", &GraphDiagnosticEntry::start)
        .def_ro("evaluation", &GraphDiagnosticEntry::evaluation)
        .def_ro("stop", &GraphDiagnosticEntry::stop);
    nb::class_<GraphDiagnosticsSnapshot>(
        m, "GraphDiagnosticsSnapshot",
        "An immutable graph-wide diagnostics snapshot.\n\n"
        "Entries use stable numeric identifiers and do not retain live graph "
        "or node objects, so a snapshot remains safe after execution.")
        .def_ro("graph_cycles", &GraphDiagnosticsSnapshot::graph_cycles)
        .def_ro("wall_time", &GraphDiagnosticsSnapshot::wall_time)
        .def_ro("root_evaluation_time", &GraphDiagnosticsSnapshot::root_evaluation_time)
        .def_ro("scheduling_lag_total", &GraphDiagnosticsSnapshot::scheduling_lag_total)
        .def_ro("scheduling_lag_max", &GraphDiagnosticsSnapshot::scheduling_lag_max)
        .def_ro("scheduling_lag_samples", &GraphDiagnosticsSnapshot::scheduling_lag_samples)
        .def_ro("runtime_load", &GraphDiagnosticsSnapshot::runtime_load)
        .def_ro("planned_bytes", &GraphDiagnosticsSnapshot::planned_bytes)
        .def_ro("dynamic_live_bytes", &GraphDiagnosticsSnapshot::dynamic_live_bytes)
        .def_ro("dynamic_reserved_bytes", &GraphDiagnosticsSnapshot::dynamic_reserved_bytes)
        .def_ro("peak_dynamic_live_bytes", &GraphDiagnosticsSnapshot::peak_dynamic_live_bytes)
        .def_ro("peak_dynamic_reserved_bytes", &GraphDiagnosticsSnapshot::peak_dynamic_reserved_bytes)
        .def_ro("entries", &GraphDiagnosticsSnapshot::entries);
    nb::class_<GraphDiagnostics>(
        m, "GraphDiagnostics",
        "Collect graph structure, lifecycle, storage, and optional values.\n\n"
        "Pass this observer through GraphConfiguration.life_cycle_observers. "
        "capture_values=True serializes runtime values and may add "
        "substantial diagnostic cost.")
        .def("__init__",
             [](nb::pointer_and_handle<GraphDiagnostics> self,
                std::size_t recent_window, bool capture_values) {
                 new (self.p) GraphDiagnostics{GraphDiagnosticsOptions{
                     .recent_window = recent_window,
                     .capture_values = capture_values,
                 }};
             },
             nb::arg("recent_window") = 100,
             nb::arg("capture_values") = false)
        .def("snapshot", &GraphDiagnostics::snapshot,
             "Return an immutable snapshot of the collected diagnostics.")
        .def("reset", &GraphDiagnostics::reset,
             "Clear collected diagnostics and peak counters.");
    nb::class_<RuntimeRegistrySnapshot>(
        m, "RuntimeRegistrySnapshot",
        "Process-lifetime cardinalities for native runtime registries.")
        .def_ro("node_runtime_types", &RuntimeRegistrySnapshot::node_runtime_types)
        .def_ro("graph_programs", &RuntimeRegistrySnapshot::graph_programs)
        .def_ro("graph_runtime_types", &RuntimeRegistrySnapshot::graph_runtime_types)
        .def_ro("executor_runtime_types", &RuntimeRegistrySnapshot::executor_runtime_types)
        .def_ro("type_records", &RuntimeRegistrySnapshot::type_records)
        .def_ro("type_system_lock_acquisitions",
                &RuntimeRegistrySnapshot::type_system_lock_acquisitions);
    m.def("runtime_registry_snapshot", &runtime_registry_snapshot,
          "Capture cold-path process-lifetime runtime registry cardinalities.");

    nb::class_<WiringTracer>(
        m, "WiringTracer",
        "Capture graph and node wiring events as formatted text lines.\n\n"
        "Supply the tracer through GraphConfiguration.wiring_observers or "
        "trace_wiring. The optional filter matches wiring paths.")
        .def(nb::init<std::string, bool, bool>(), nb::arg("filter") = "",
             nb::arg("graph") = true, nb::arg("node") = true)
        .def_prop_ro("lines", [](const WiringTracer &tracer) {
            return std::vector<std::string>{tracer.lines().begin(),
                                            tracer.lines().end()};
        })
        .def("clear", &WiringTracer::clear,
             "Remove every captured wiring line.");
    nb::class_<WiringObservationScope>(m, "_WiringObservationScope")
        .def("__enter__", [](WiringObservationScope &self) -> WiringObservationScope & {
            return self;
        }, nb::rv_policy::reference_internal)
        .def("__exit__", [](WiringObservationScope &self, nb::object,
                            nb::object error, nb::object) {
            if (error.is_none()) { self.complete(); }
            else { self.fail(nb::cast<std::string>(nb::str(error))); }
            return false;
        }, nb::arg("exception_type").none(), nb::arg("exception").none(),
           nb::arg("traceback").none());

    nb::class_<PyWiredFn>(m, "WiredFn")
        .def_prop_ro("arity", [](const PyWiredFn &self) { return self.fn.arity; })
        .def_prop_ro("variadic", [](const PyWiredFn &self) { return self.fn.variadic; })
        .def_prop_ro("has_output", [](const PyWiredFn &self) { return self.fn.has_output; })
        .def_prop_ro("_python_callable", [](const PyWiredFn &self) -> nb::object {
            if (self.fn.identity != nullptr && *self.fn.identity == typeid(PyGraphFnRecord) &&
                self.fn.context != nullptr)
            {
                return static_cast<const PyGraphFnRecord *>(self.fn.context)->user_fn;
            }
            return nb::none();
        });
    nb::class_<PyNodeHandle>(m, "NodeRef");
    nb::class_<PyScalarValue>(m, "ScalarValue");
    nb::class_<PySender>(m, "Sender").def("send", &PySender::send, nb::arg("value"));

    m.def("node_ref", [](nb::object fn) {
        auto &registry = py_node_registry();
        auto  found    = registry.find(fn.ptr());
        if (found == registry.end())
        {
            auto *record = new PyNodeRecord{fn};   // immortal: scalar identity by pointer
            found        = registry.emplace(fn.ptr(), record).first;
        }
        return PyNodeHandle{found->second};
    });

    m.def("graph_fn", [](nb::object wrapper, nb::object identity, nb::list param_names, bool has_output,
                         std::optional<PyTsType> output_type, nb::list input_types,
                         nb::list input_patterns,
                         nb::object user_callable) {
        auto &registry = py_graph_fn_registry();
        auto  found    = registry.find(identity.ptr());
        if (found == registry.end())
        {
            auto *record = new PyGraphFnRecord{};   // immortal: WiredFn contexts must outlive every value
            record->wrapper    = wrapper;
            record->user_fn    = user_callable.is_none() ? identity : user_callable;
            record->identity   = identity;
            const nb::handle diagnostic_source = user_callable.is_none()
                                                     ? nb::handle{identity}
                                                     : nb::handle{user_callable};
            record->diagnostic_label = nb::hasattr(diagnostic_source, "__name__")
                                           ? nb::cast<std::string>(
                                                 diagnostic_source.attr("__name__"))
                                           : std::string{"<python-graph>"};
            record->has_output = has_output;
            record->arity      = nb::len(param_names);
            if (output_type.has_value()) { record->output_schema = output_type->meta; }
            record->input_schemas.reserve(record->arity);
            for (nb::handle input_type : input_types)
            {
                record->input_schemas.push_back(
                    input_type.is_none() ? nullptr : nb::cast<PyTsType &>(input_type).meta);
            }
            record->input_patterns.reserve(record->arity);
            for (nb::handle input_pattern : input_patterns)
            {
                record->input_patterns.push_back(
                    input_pattern.is_none()
                        ? std::nullopt
                        : std::optional<TypePattern>{nb::cast<PyTypePattern &>(input_pattern).pattern});
            }
            record->name_storage.reserve(record->arity);
            for (nb::handle name : param_names) { record->name_storage.push_back(nb::cast<std::string>(name)); }
            for (const auto &name : record->name_storage) { record->names.emplace_back(name); }
            found = registry.emplace(identity.ptr(), record).first;
        }
        const PyGraphFnRecord *record = found->second;
        return PyWiredFn{WiredFn{
            .ops        = &py_graph_fn_ops(),
            .context    = record,
            .identity   = &typeid(PyGraphFnRecord),
            .arity      = record->arity,
            .has_output = record->has_output,
        }};
    }, nb::arg("wrapper"), nb::arg("identity"), nb::arg("param_names"), nb::arg("has_output"),
       nb::arg("output_type") = nb::none(), nb::arg("input_types") = nb::list(),
       nb::arg("input_patterns") = nb::list(),
       nb::arg("user_callable") = nb::none());

    nb::class_<PySwitchCases>(m, "SwitchCases");
    nb::class_<PyDispatchCases>(m, "DispatchCases");
    nb::class_<PyFeedback>(m, "Feedback")
        .def_prop_ro("port", [](const PyFeedback &fb) { return PyPort{fb.delegate}; })
        .def_prop_ro("bound", [](const PyFeedback &fb) { return fb.bound; });
    nb::class_<PyDelayedBinding>(m, "DelayedBinding")
        .def_prop_ro("port", [](const PyDelayedBinding &delayed) {
            return PyPort{delayed.binding.port()};
        })
        .def_prop_ro("bound", [](const PyDelayedBinding &delayed) {
            return delayed.binding.bound();
        });

    m.def("switch_cases", [](nb::dict cases, bool reload,
                              std::optional<PyTsType> key_type) {
        stdlib::SwitchCases result;
        result.reload_on_ticked = reload;
        const auto *key_schema = key_type.has_value()
                                     ? TypeRegistry::instance()
                                           .dereference(key_type->meta)
                                           ->value_schema
                                     : nullptr;
        for (auto [key, branch] : cases)
        {
            WiredFn fn;
            if (nb::isinstance<PyWiredFn>(branch)) { fn = nb::cast<PyWiredFn &>(branch).fn; }
            else
            {
                const auto &table = wired_fn_table();
                const auto  found = table.find(nb::cast<std::string>(branch));
                if (found == table.end()) { throw nb::value_error("no wired-fn erasure for switch branch"); }
                fn = found->second;
            }
            if (key.is_none()) { result.default_branch = fn; }
            else
            {
                result.cases.push_back(stdlib::SwitchCase{
                    key_schema != nullptr ? py_to_value_as(key, key_schema)
                                          : py_to_value(key),
                    fn});
            }
        }
        return PySwitchCases{std::move(result)};
    }, nb::arg("cases"), nb::arg("reload") = false,
       nb::arg("key_type") = nb::none());
    m.def("dispatch_cases", [](nb::list entries, nb::list on, nb::object default_branch) {
        const auto as_wired_fn = [](nb::handle branch) {
            if (nb::isinstance<PyWiredFn>(branch))
            {
                return nb::cast<PyWiredFn &>(branch).fn;
            }
            const auto &table = wired_fn_table();
            const auto found = table.find(nb::cast<std::string>(branch));
            if (found == table.end())
            {
                throw nb::value_error("no wired-fn erasure for dispatch branch");
            }
            return found->second;
        };

        stdlib::DispatchCases result;
        result.dispatch_args.clear();
        for (nb::handle index : on)
        {
            result.dispatch_args.push_back(nb::cast<std::size_t>(index));
        }
        for (nb::handle item : entries)
        {
            nb::tuple pair = nb::cast<nb::tuple>(item);
            nb::tuple types = nb::cast<nb::tuple>(pair[0]);
            stdlib::DispatchCase entry;
            entry.types.reserve(nb::len(types));
            for (nb::handle type : types)
            {
                entry.types.push_back(nb::cast<PyValueType &>(type).meta);
            }
            entry.branch = as_wired_fn(pair[1]);
            result.cases.push_back(std::move(entry));
        }
        if (!default_branch.is_none()) { result.default_branch = as_wired_fn(default_branch); }
        return PyDispatchCases{std::move(result)};
    }, nb::arg("entries"), nb::arg("on"), nb::arg("default_branch").none() = nb::none());
    m.def("wired_op", [](const std::string &name, std::optional<PyTsType> expected_output) {
        const auto &table = wired_fn_table();
        const auto  found = table.find(name);
        if (found != table.end() && !expected_output.has_value())
        {
            return PyWiredFn{found->second};
        }
        return PyWiredFn{runtime_operator_fn(
            name, expected_output.has_value() ? expected_output->meta : nullptr)};
    }, nb::arg("name"), nb::arg("expected_output").none() = nb::none());

    // Conversion-layer round trip (test/debug aid): Python -> Value -> Python.
    m.def("_roundtrip_value", [](nb::handle object) { return value_to_py(py_to_value(object).view()); });

    nb::class_<PyWiring>(m, "Wiring", nb::is_weak_referenceable())
        .def(nb::init<bool>(), nb::arg("is_realtime") = false)
        .def(nb::init<GlobalState &, bool>(), nb::arg("state"),
             nb::arg("is_realtime") = false)
        .def("identity", [](const PyWiring &wiring) {
            return wiring.raw->identity();
        }, "Return the stable identity of the underlying C++ Wiring.")
        .def_prop_ro("is_realtime", [](const PyWiring &wiring) {
            return wiring.raw->is_realtime();
        }, "Whether this context is wiring a real-time graph.")
        .def("_acquire_extension_state", [](PyWiring &wiring, const std::string &key,
                                             const nb::object &factory) {
            auto state = std::static_pointer_cast<PyWiringExtensionState>(
                wiring.raw->acquire_extension_state(
                    std::type_index(typeid(PyWiringExtensionState)),
                    [] { return std::make_shared<PyWiringExtensionState>(); }));
            return state->acquire(key, factory);
        }, nb::arg("key"), nb::arg("factory"),
        "Return state owned by this wiring, creating it once with factory.")
        .def("_retain_cleanup", [](PyWiring &wiring, nb::object callback) {
            wiring.raw->retain_extension_state(
                std::make_shared<PyWiringCleanup>(std::move(callback)));
        })
        .def("exception_time_series", &PyWiring::exception_time_series,
             nb::arg("port"), nb::arg("trace_back_depth") = 1,
             nb::arg("capture_values") = false)
        .def("wire", &PyWiring::wire, nb::arg("name"), nb::arg("args") = nb::tuple(),
             nb::arg("kwargs") = nb::dict(), nb::arg("output_type") = nb::none(),
             nb::arg("sizes") = nb::none(), nb::arg("initial_resolution").none() = nb::none(),
             nb::arg("node_label") = nb::none())
        .def("set_replay", &PyWiring::set_replay, nb::arg("key"), nb::arg("values"),
             nb::arg("ts_type") = nb::none())
        .def("feedback", &PyWiring::feedback, nb::arg("ts_type"), nb::arg("initial") = nb::none())
        .def("feedback_bind", &PyWiring::feedback_bind, nb::arg("feedback"), nb::arg("port"))
        .def("delayed_binding", &PyWiring::delayed_binding, nb::arg("ts_type"))
        .def("delayed_binding_bind", &PyWiring::delayed_binding_bind,
             nb::arg("delayed"), nb::arg("port"))
        .def("configure_wiring_observers",
             &PyWiring::configure_wiring_observers,
             nb::arg("trace_wiring") = false,
             nb::arg("observers") = nb::tuple())
        .def("add_lifecycle_observer", &PyWiring::add_lifecycle_observer,
             nb::arg("observer"))
        .def("wiring_trace_lines", &PyWiring::wiring_trace_lines)
        .def("build_services", [](PyWiring &wiring) { wiring.raw->build_services(); })
        .def("service_client_paths", [](PyWiring &wiring) {
            return wiring.raw->service_client_paths();
        })
        .def("service_client_records", [](PyWiring &wiring) {
            nb::list records;
            for (const auto &record : wiring.raw->service_client_records())
            {
                records.append(nb::make_tuple(
                    record.base_path, record.endpoint_path, record.kind,
                    record.interface_name, record.specialization, record.receive));
            }
            return records;
        })
        .def("built_service_paths", [](PyWiring &wiring) {
            return wiring.raw->built_service_paths();
        })
        .def("service_materialization_path", [](PyWiring &wiring) {
            return std::string{wiring.raw->service_materialization_path()};
        })
        .def("_graph_wiring_scope", &PyWiring::graph_wiring_scope,
             nb::arg("label"))
        .def("_release_seed_context", &PyWiring::release_seed_context)
        .def("run", &PyWiring::run, nb::arg("start_time") = nb::none(), nb::arg("end_time") = nb::none(),
             nb::arg("realtime") = false, nb::arg("trace").none() = nb::none(),
             nb::arg("profiler").none() = nb::none(),
             nb::arg("logger").none() = nb::none(), nb::arg("logger_level") = 10,
             nb::arg("logger_formatter").none() = nb::none(),
             nb::arg("observers") = nb::tuple(),
             nb::arg("trace_back_depth") = 1,
             nb::arg("capture_values") = false,
             nb::arg("cleanup_on_error") = true,
             nb::arg("snapshot") = false,
             nb::arg("_on_prepared") = nb::none())
        .def("push_source", &PyWiring::push_source, nb::arg("ts_type"), nb::arg("conflate") = false,
             nb::arg("burst") = false, nb::arg("max_pending") = nb::none(),
             nb::arg("fn"), nb::arg("config"), nb::arg("stop_fn"),
             nb::arg("stop_enabled"), nb::arg("stop_config"),
             nb::arg("stop_scalar_offset"), nb::arg("scalars"),
             nb::arg("node_label") = nb::none());

    m.def(
        "component",
        [](PyWiring &wiring, const std::string &recordable_id,
           nb::list names, nb::list ports, nb::object compose) -> nb::object {
            if (nb::len(names) != nb::len(ports))
            {
                throw nb::value_error("component names and ports must have the same length");
            }

            std::vector<WiringNamedPortRef> inputs;
            inputs.reserve(nb::len(names));
            for (std::size_t index = 0; index < nb::len(names); ++index)
            {
                inputs.emplace_back(
                    nb::cast<std::string>(names[index]),
                    nb::cast<PyPort &>(ports[index]).ref);
            }

            try
            {
                WiringPortRef out = stdlib::component(
                    wiring.wiring_ref(), recordable_id,
                    std::span<const WiringNamedPortRef>{inputs.data(), inputs.size()},
                    [&](std::span<const WiringPortRef> wrapped) {
                        nb::list args;
                        for (const WiringPortRef &port : wrapped)
                        {
                            args.append(nb::cast(PyPort{port}));
                        }
                        nb::object result = compose(nb::tuple(args));
                        return result.is_none() ? WiringPortRef{}
                                                : nb::cast<PyPort &>(result).ref;
                    });
                return out.is_unbound_source() ? nb::none()
                                               : nb::cast(PyPort{std::move(out)});
            }
            catch (const std::invalid_argument &error)
            {
                if (std::string_view{error.what()}.starts_with(
                        "component: duplicate recordable id"))
                {
                    throw std::runtime_error(error.what());
                }
                throw;
            }
        },
        nb::arg("wiring"), nb::arg("recordable_id"), nb::arg("names"),
        nb::arg("ports"), nb::arg("compose"));

    m.def(
        "_evaluate_const",
        [](GlobalState &state, const std::string &name, nb::tuple args, nb::dict kwargs,
           std::optional<PyTsType> output_type) {
            const auto wiring_args = build_args(args, kwargs);
            Value      value       = OperatorRegistry::instance().evaluate_const(
                name, std::span<const WiringArg>{wiring_args.data(), wiring_args.size()},
                output_type.has_value() ? output_type->meta : nullptr, state.view());
            return value.has_value() ? value_to_py(value.view()) : nb::none();
        },
        nb::arg("state"), nb::arg("name"), nb::arg("args") = nb::tuple(), nb::arg("kwargs") = nb::dict(),
        nb::arg("output_type") = nb::none());

    m.def(
        "_lower",
        [](GlobalState &state, const PyWiredFn &function, nb::list input_frames,
           const std::string &date_column, const std::string &as_of_column,
           bool no_as_of_support, std::optional<DateTime> start_time,
           std::optional<DateTime> end_time, EvaluationTrace *trace) -> nb::object {
            std::vector<Frame> frames;
            frames.reserve(nb::len(input_frames));
            for (nb::handle object : input_frames)
            {
                Value converted = py_arrow_to_frame(object);
                frames.push_back(converted.view().checked_as<Frame>());
            }

            GlobalContext context{state};
            stdlib::LowerOptions options;
            options.date_column      = date_column;
            options.as_of_column     = as_of_column;
            options.no_as_of_support = no_as_of_support;
            options.start_time       = start_time.value_or(MIN_ST);
            options.end_time         = end_time.value_or(MAX_ET);
            options.observer         = trace;
            options.phase_runner     = &py_run_executor_phase;
            options.phase_runner_requires_graph_opt_in = true;
            stdlib::LowerExecution execution = stdlib::prepare_lower(
                function.fn, std::span<const Frame>{frames.data(), frames.size()},
                std::move(options));

            nb::object runtime = nb::module_::import_("hgraph._wiring._state");
            runtime.attr("_enter_runtime")();
            auto clear_runtime_state = UnwindCleanupGuard([&] {
                runtime.attr("_exit_runtime")();
            });
            {
                nb::gil_scoped_release release;
                execution.run();
            }
            clear_runtime_state.complete();
            return execution.result().has_value()
                       ? frame_to_py(*execution.result())
                       : nb::none();
        },
        nb::arg("state"), nb::arg("function"), nb::arg("input_frames"),
        nb::arg("date_column") = "date", nb::arg("as_of_column") = "as_of",
        nb::arg("no_as_of_support") = true,
        nb::arg("start_time").none() = nb::none(),
        nb::arg("end_time").none() = nb::none(),
        nb::arg("trace").none() = nb::none());
    }
}  // namespace hgraph::python_bridge
