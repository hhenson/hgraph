/**
 * Persistence bindings (RFC 0025, checkpoint 3): the frame-store surface
 * split out of the core state/services unit so the checkpoint-4/5 move to
 * ``_hgraph_persistence`` relocates one translation unit. Everything here
 * is extension-bound: frame-store reads, the graph-scoped Python
 * compatibility store, and the durable recording-option enum slots.
 */
#include "py_runtime.h"
#include "py_wiring.h"
#include "py_bindings.h"

#include <hgraph/python/bridge_state.h>
#include <hgraph/types/frame_store.h>
#include <hgraph/types/metadata/type_registry.h>

namespace nb = nanobind;
using namespace hgraph;

namespace hgraph::python_bridge
{
    namespace
    {
        /** State-owned compatibility store delegated to Python.
         *
         * This intentionally exposes only store/load/has. Native storage
         * policy (immutability, compression and future segmentation) belongs
         * to the C++ memory/file/S3 implementations; a Python bridge decides
         * its own overwrite behaviour and is never asked to manage segments.
         */
        struct PythonFrameStore
        {
            explicit PythonFrameStore(nb::object impl) : impl(std::move(impl)) {}
            PyObj impl;
        };

        [[nodiscard]] const store::FrameStoreOps &python_frame_store_ops() noexcept
        {
            static const store::FrameStoreOps ops{
                [](void *context, std::string_view key, Frame frame, std::optional<store::Compression>) {
                    const nb::gil_scoped_acquire gil;
                    static_cast<PythonFrameStore *>(context)->impl.get().attr("store")(
                        std::string{key}, frame_to_store_py(frame));
                },
                [](void *context, std::string_view key) {
                    const nb::gil_scoped_acquire gil;
                    nb::object out = static_cast<PythonFrameStore *>(context)->impl.get().attr("load")(
                        std::string{key});
                    if (out.is_none()) { return Frame{}; }
                    const Value frame = py_arrow_to_frame(out);
                    return Frame{frame.view().checked_as<Frame>()};
                },
                [](void *context, std::string_view key) {
                    const nb::gil_scoped_acquire gil;
                    return nb::cast<bool>(static_cast<PythonFrameStore *>(context)->impl.get().attr("has")(
                        std::string{key}));
                },
                // The deliberately small Python protocol has no bulk-delete
                // operation. Removing the adapter from GlobalState releases
                // it; deleting persisted user data remains its own API.
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

        void install_python_frame_store(GlobalStateView state, nb::object impl)
        {
            auto previous = record_replay::frame_store(state);
            auto stack    = python_frame_store_stack(state);
            stack.previous.push_back(previous);

            auto adapter = store::FrameStore{
                std::make_shared<PythonFrameStore>(std::move(impl)), python_frame_store_ops()};
            record_replay::set_frame_store(state, std::move(adapter));
            try
            {
                (void)TypeRegistry::instance().register_scalar<PythonFrameStoreStack>(
                    "__python_frame_store_stack__");
                state.set(PYTHON_FRAME_STORE_STACK_KEY, Value{std::move(stack)});
            }
            catch (...)
            {
                if (previous) { record_replay::set_frame_store(state, std::move(previous)); }
                else { record_replay::clear_frame_store(state); }
                throw;
            }
        }

        void restore_python_frame_store(GlobalStateView state)
        {
            auto current = record_replay::frame_store(state);
            auto stack   = python_frame_store_stack(state);
            if (!current || stack.previous.empty())
            {
                throw std::logic_error("the active graph store is not a Python frame store");
            }

            auto previous = std::move(stack.previous.back());
            stack.previous.pop_back();
            if (previous) { record_replay::set_frame_store(state, std::move(previous)); }
            else { record_replay::clear_frame_store(state); }

            try
            {
                if (stack.previous.empty()) { static_cast<void>(state.erase(PYTHON_FRAME_STORE_STACK_KEY)); }
                else { state.set(PYTHON_FRAME_STORE_STACK_KEY, Value{std::move(stack)}); }
            }
            catch (...)
            {
                record_replay::set_frame_store(state, std::move(current));
                throw;
            }
        }
    }  // namespace

    bool python_frame_store_active(GlobalStateView state)
    {
        const ValueView value = state.get(PYTHON_FRAME_STORE_STACK_KEY);
        return value && !value.checked_as<PythonFrameStoreStack>().previous.empty();
    }

    void bind_persistence(nb::module_ &m)
    {
    m.def("_frame_store_contains", [](GlobalState &state, const std::string &key) {
        return record_replay::store_contains(state.view(), key);
    });
    m.def("_frame_store_read", [](GlobalState &state, const std::string &key) {
        return frame_to_py(record_replay::store_read(state.view(), key));
    });

    // Install/restore a graph-scoped Python compatibility store. Nesting is
    // lossless: graph state retains each native or Python handle it replaced.
    m.def("_set_python_frame_store", [](GlobalState &state, nb::object impl) {
        install_python_frame_store(state.view(), std::move(impl));
    });
    m.def("_restore_python_frame_store", [](GlobalState &state) {
        restore_python_frame_store(state.view());
    });
    m.def("_set_record_as_of_enum",
          [](nb::object enum_class) { record_as_of_enum_slot() = std::move(enum_class); });
    m.def("_set_record_removes_enum",
          [](nb::object enum_class) { record_removes_enum_slot() = std::move(enum_class); });
    }
}  // namespace hgraph::python_bridge
