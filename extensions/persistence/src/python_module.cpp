/**
 * The hgraph-persistence Python bridge (RFC 0025, checkpoint 4): the frame
 * store surface — reads, the graph-scoped Python compatibility store, and
 * recording-session installation — over the shared hgraph runtime.
 * Importing the module registers the durable backend (the keyed installer),
 * so selecting ``"hgraph.persistence.frame"`` resolves the frame overloads
 * from unchanged core imports.
 */
#include <hgraph/persistence/frame_store.h>
#include <hgraph/persistence/recording_store.h>

#include <hgraph/runtime/global_state.h>
#include <hgraph/types/frame.h>
#include <hgraph/types/metadata/type_registry.h>

#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>

#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>

#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace nb = nanobind;
using namespace hgraph;
using namespace hgraph::persistence;

namespace
{
    /** GIL-safe owner for a Python object stored inside GlobalState values:
        graph/state teardown may destroy it from pure C++ context. */
    struct GilObject
    {
        PyObject *object{nullptr};

        GilObject() noexcept = default;
        explicit GilObject(nb::object value) noexcept : object(value.release().ptr()) {}
        GilObject(const GilObject &other) noexcept : object(other.object)
        {
            if (object != nullptr)
            {
                const nb::gil_scoped_acquire gil;
                Py_INCREF(object);
            }
        }
        GilObject(GilObject &&other) noexcept : object(other.object) { other.object = nullptr; }
        GilObject &operator=(const GilObject &other) noexcept
        {
            if (this != &other)
            {
                GilObject copy{other};
                std::swap(object, copy.object);
            }
            return *this;
        }
        GilObject &operator=(GilObject &&other) noexcept
        {
            std::swap(object, other.object);
            return *this;
        }
        ~GilObject()
        {
            if (object != nullptr)
            {
                const nb::gil_scoped_acquire gil;
                Py_DECREF(object);
            }
        }

        [[nodiscard]] nb::object get() const { return nb::borrow<nb::object>(object); }
    };

    /** Export a Frame through the Arrow C stream protocol. */
    struct FrameStream
    {
        Frame frame;

        [[nodiscard]] nb::object capsule() const
        {
            auto reader = std::make_shared<arrow::TableBatchReader>(*frame.table);
            auto *c_stream = new ArrowArrayStream{};
            const auto status = arrow::ExportRecordBatchReader(reader, c_stream);
            if (!status.ok())
            {
                delete c_stream;
                throw std::runtime_error("arrow stream export failed: " + status.ToString());
            }
            const auto release = [](PyObject *object) {
                if (!PyCapsule_IsValid(object, "arrow_array_stream")) { return; }
                auto *raw = static_cast<ArrowArrayStream *>(
                    PyCapsule_GetPointer(object, "arrow_array_stream"));
                if (raw != nullptr)
                {
                    if (raw->release != nullptr) { raw->release(raw); }
                    delete raw;
                }
            };
            return nb::steal(PyCapsule_New(c_stream, "arrow_array_stream", release));
        }
    };

    /** Arrow table in the Python STORE presentation (naive-UTC timestamps,
        schema metadata retained, never the Polars user presentation). */
    [[nodiscard]] nb::object frame_to_store_py(const Frame &frame)
    {
        if (!frame.has_value()) { return nb::none(); }
        nb::object stream = nb::cast(FrameStream{frame});
        nb::object table = nb::module_::import_("pyarrow").attr("table")(stream);
        return nb::module_::import_("hgraph._frame").attr("_present_store_frame")(table);
    }

    [[nodiscard]] Frame py_arrow_to_frame(nb::handle object)
    {
        nb::object capsule = object.attr("__arrow_c_stream__")();
        auto *stream = static_cast<ArrowArrayStream *>(
            PyCapsule_GetPointer(capsule.ptr(), "arrow_array_stream"));
        if (stream == nullptr) { throw nb::type_error("expected an arrow_array_stream capsule"); }
        auto reader = arrow::ImportRecordBatchReader(stream);
        if (!reader.ok())
        {
            throw std::runtime_error("arrow import failed: " + reader.status().ToString());
        }
        auto table = arrow::Table::FromRecordBatchReader(reader->get());
        if (!table.ok())
        {
            throw std::runtime_error("arrow read failed: " + table.status().ToString());
        }
        return Frame{.table = std::move(*table)};
    }

    /** State-owned compatibility store delegated to Python.
     *
     * This intentionally exposes only store/load/has. Native storage policy
     * (immutability, compression and segmentation) belongs to the C++
     * memory/file/S3 implementations; a Python bridge decides its own
     * overwrite behaviour and is never asked to manage segments.
     */
    struct PythonFrameStore
    {
        explicit PythonFrameStore(nb::object impl) : impl(std::move(impl)) {}
        GilObject impl;
    };

    [[nodiscard]] const store::FrameStoreOps &python_frame_store_ops() noexcept
    {
        static const store::FrameStoreOps ops{
            [](void *context, std::string_view key, Frame frame,
               std::optional<store::Compression>) {
                const nb::gil_scoped_acquire gil;
                static_cast<PythonFrameStore *>(context)->impl.get().attr("store")(
                    std::string{key}, frame_to_store_py(frame));
            },
            [](void *context, std::string_view key) {
                const nb::gil_scoped_acquire gil;
                nb::object out = static_cast<PythonFrameStore *>(context)->impl.get().attr("load")(
                    std::string{key});
                if (out.is_none()) { return Frame{}; }
                return py_arrow_to_frame(out);
            },
            [](void *context, std::string_view key) {
                const nb::gil_scoped_acquire gil;
                return nb::cast<bool>(static_cast<PythonFrameStore *>(context)->impl.get().attr(
                    "has")(std::string{key}));
            },
            // The deliberately small Python protocol has no bulk-delete
            // operation. Removing the adapter from GlobalState releases it;
            // deleting persisted user data remains its own API.
            [](void *) {},
        };
        return ops;
    }

    struct PythonFrameStoreStack
    {
        std::vector<store::FrameStore> previous;
    };

    inline constexpr std::string_view PYTHON_FRAME_STORE_STACK_KEY{
        "__hgraph.python.frame_store_stack__"};

    [[nodiscard]] PythonFrameStoreStack python_frame_store_stack(GlobalStateView state)
    {
        const ValueView value = state.get(PYTHON_FRAME_STORE_STACK_KEY);
        return value ? value.checked_as<PythonFrameStoreStack>() : PythonFrameStoreStack{};
    }

    [[nodiscard]] bool python_frame_store_active(GlobalStateView state)
    {
        const ValueView value = state.get(PYTHON_FRAME_STORE_STACK_KEY);
        return value && !value.checked_as<PythonFrameStoreStack>().previous.empty();
    }

    void install_python_frame_store(GlobalStateView state, nb::object impl)
    {
        auto previous = persistence::frame_store(state);
        auto stack    = python_frame_store_stack(state);
        stack.previous.push_back(previous);

        auto adapter = store::FrameStore{
            std::make_shared<PythonFrameStore>(std::move(impl)), python_frame_store_ops()};
        persistence::set_frame_store(state, std::move(adapter));
        try
        {
            (void)TypeRegistry::instance().register_scalar<PythonFrameStoreStack>(
                "__python_frame_store_stack__");
            state.set(PYTHON_FRAME_STORE_STACK_KEY, Value{std::move(stack)});
        }
        catch (...)
        {
            if (previous) { persistence::set_frame_store(state, std::move(previous)); }
            else { persistence::clear_frame_store(state); }
            throw;
        }
    }

    void restore_python_frame_store(GlobalStateView state)
    {
        auto current = persistence::frame_store(state);
        auto stack   = python_frame_store_stack(state);
        if (!current || stack.previous.empty())
        {
            throw std::logic_error("the active graph store is not a Python frame store");
        }

        auto previous = std::move(stack.previous.back());
        stack.previous.pop_back();
        if (previous) { persistence::set_frame_store(state, std::move(previous)); }
        else { persistence::clear_frame_store(state); }

        try
        {
            if (stack.previous.empty())
            {
                static_cast<void>(state.erase(PYTHON_FRAME_STORE_STACK_KEY));
            }
            else { state.set(PYTHON_FRAME_STORE_STACK_KEY, Value{std::move(stack)}); }
        }
        catch (...)
        {
            persistence::set_frame_store(state, std::move(current));
            throw;
        }
    }
}  // namespace

NB_MODULE(_hgraph_persistence, module)
{
    module.doc() = "hgraph durable persistence bridge (RFC 0025)";

    nb::class_<FrameStream>(module, "_FrameStream")
        .def("__arrow_c_stream__",
             [](const FrameStream &self, nb::handle) { return self.capsule(); },
             nb::arg("requested_schema") = nb::none());

    module.def("_frame_store_contains", [](nb::object state, const std::string &key) {
        return persistence::store_contains(nb::cast<GlobalState &>(state).view(), key);
    });
    module.def("_frame_store_read", [](nb::object state, const std::string &key) {
        return frame_to_store_py(persistence::store_read(nb::cast<GlobalState &>(state).view(), key));
    });

    // Install/restore a graph-scoped Python compatibility store. Nesting is
    // lossless: graph state retains each native or Python handle it replaced.
    module.def("_set_python_frame_store", [](nb::object state, nb::object impl) {
        install_python_frame_store(nb::cast<GlobalState &>(state).view(), std::move(impl));
    });
    module.def("_restore_python_frame_store", [](nb::object state) {
        restore_python_frame_store(nb::cast<GlobalState &>(state).view());
    });

    // Selecting the frame backend starts a NEW recording session: install a
    // fresh native store unless a user-owned Python compatibility store is
    // active (`with DataFrameStorage(): set_record_replay_model(...)` keeps
    // the legacy ordering contract).
    module.def("_start_recording_session", [](nb::object state) {
        const auto view = nb::cast<GlobalState &>(state).view();
        if (!python_frame_store_active(view)) { install_fresh_frame_store(view); }
    });
    module.def("_python_frame_store_active", [](nb::object state) {
        return python_frame_store_active(nb::cast<GlobalState &>(state).view());
    });

    // The durable backend: registered on import through the keyed installer,
    // so every registry rebuild replays it exactly as core's registration.
    register_frame_backend();
}
