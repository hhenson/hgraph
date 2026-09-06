#include <hgraph/types/python_ops.h>

#include <atomic>
#include <stdexcept>
#include <string>

namespace hgraph
{
    namespace
    {
        std::atomic<const PythonOps *> &python_ops_slot() noexcept
        {
            static std::atomic<const PythonOps *> slot{nullptr};
            return slot;
        }
    }  // namespace

    void set_python_ops(const PythonOps *ops) noexcept
    {
        python_ops_slot().store(ops, std::memory_order_release);
    }

    const PythonOps *python_ops() noexcept
    {
        return python_ops_slot().load(std::memory_order_acquire);
    }

    namespace python_ops_detail
    {
        const PythonOps &require_python_ops(const char *what)
        {
            const auto *ops = python_ops();
            if (ops == nullptr) { throw_unregistered(what); }
            return *ops;
        }

        void throw_unregistered(const char *what)
        {
            throw std::logic_error(std::string{"no Python conversion is registered for "} + what +
                                   " (the Python bridge is not loaded)");
        }

        void throw_unregistered_scalar(const std::type_info &type)
        {
            throw std::logic_error(std::string{"no Python conversion is registered for the scalar type "} +
                                   type.name());
        }

        const PythonScalarSlots *scalar_slots(const std::type_info &type) noexcept
        {
            const auto *ops = python_ops();
            if (ops == nullptr || ops->scalars.conversion_for == nullptr) { return nullptr; }
            return ops->scalars.conversion_for(type);
        }
    }  // namespace python_ops_detail
}  // namespace hgraph
