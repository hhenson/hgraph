#include <catch2/catch_test_macros.hpp>

#include <hg/typing/scalar_value.h>
#include <pybind11/embed.h>


TEST_CASE("Basic scalar value behaviour", "[graph_typing]") {
    py::scoped_interpreter guard{};
    using namespace hg;

    // Test the raw type
    ScalarValue v1{create_scalar_value<hg_int>((hg_int)1)};

    REQUIRE_FALSE(v1.is_reference());

    REQUIRE(v1.is<hg_int>());
    REQUIRE_FALSE(v1.is<hg_string>());

    REQUIRE(v1.as<hg_int>() == 1);
    REQUIRE_FALSE(v1.as<hg_int>() == 2);

    REQUIRE_THROWS(v1.as<hg_string>());

    REQUIRE(v1.py_object().equal(py::int_(1)));

    REQUIRE(std::hash<ScalarValue>()(v1));

    // Test the reference type
    ScalarValue ref{&v1};

    REQUIRE(ref.is_reference());

    REQUIRE(ref.is<hg_int>());
    REQUIRE_FALSE(ref.is<hg_string>());

    REQUIRE(ref.as<hg_int>() == 1);
    REQUIRE_FALSE(ref.as<hg_int>() == 2);

    REQUIRE_THROWS(ref.as<hg_string>());

    REQUIRE(ref.py_object().equal(py::int_(1)));

    REQUIRE(std::hash<ScalarValue>()(ref));
}