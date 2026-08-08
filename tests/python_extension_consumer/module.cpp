#include <hgraph/python/native_scalar_registration.h>
#include <hgraph/types/metadata/type_registry.h>

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
}
