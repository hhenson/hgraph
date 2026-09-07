/**
 * RFC 0035 acceptance criterion 3: a standalone ``python-user-nodes`` build
 * (``HGRAPH_ENABLE_PYTHON_USER_NODES=ON``, ``HGRAPH_BUILD_PYTHON_BINDINGS=OFF``)
 * converts a scalar, a compact list, a TSB and a TSD through the provider
 * table the bridge unit registers at load, with no ``_hgraph`` module.
 *
 * The executable embeds its own interpreter the way a standalone host
 * does: an isolated configuration (no environment, no ``site``), the
 * configured interpreter's site-packages appended for the third-party
 * modules the bridge's Python surface uses (``numpy`` for the dense list
 * export), then ``attach_embedded_interpreter``. Every case ends by
 * checking that neither ``hgraph`` nor ``_hgraph`` was imported. This file
 * is compiled only under the preset (``tests/cpp/CMakeLists.txt``); see
 * ``python_integration.rst``, "Standalone conversions".
 */
#include <hgraph/lib/std/operators/arithmetic.h>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/python/bridge_state.h>
#include <hgraph/python/conversion.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/python_ops.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/value/value.h>
#include <hgraph/util/date_time.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>

#include <chrono>
#include <cstdint>
#include <stdexcept>
#include <string>

namespace
{
    namespace nb = nanobind;
    using hgraph::DateTime;
    using hgraph::Int;
    using hgraph::MIN_ST;
    using hgraph::Str;
    using hgraph::TSOutput;
    using hgraph::TypeRegistry;
    using hgraph::Value;
    using hgraph::ValuePlanFactory;
    using hgraph::python_bridge::delta_value_to_python;
    using hgraph::python_bridge::from_python;
    using hgraph::python_bridge::to_python;
    using hgraph::python_bridge::value_to_python;

    /**
     * The process's one embedded interpreter. Isolated: no environment
     * variables, no ``site``, the stdlib located through the home the build
     * configured from the interpreter it links; the configured interpreter's
     * site-packages appended afterwards. Never finalised -- the runtime's
     * registries outlive every test case.
     */
    void ensure_interpreter()
    {
        static const bool started = [] {
            if (Py_IsInitialized() == 0)
            {
                PyConfig config;
                PyConfig_InitIsolatedConfig(&config);
                config.site_import             = 0;
                config.install_signal_handlers = 0;
#ifdef HGRAPH_TEST_PYTHON_HOME
                PyConfig_SetBytesString(&config, &config.home, HGRAPH_TEST_PYTHON_HOME);
#endif
                const PyStatus status = Py_InitializeFromConfig(&config);
                PyConfig_Clear(&config);
                if (PyStatus_Exception(status) != 0)
                {
                    throw std::runtime_error(std::string{"the embedded interpreter failed to start: "} +
                                             (status.err_msg != nullptr ? status.err_msg : "unknown error"));
                }
#ifdef HGRAPH_TEST_PYTHON_SITE
                nb::module_::import_("sys").attr("path").attr("append")(HGRAPH_TEST_PYTHON_SITE);
#endif
            }
            hgraph::python_bridge::attach_embedded_interpreter();
            return true;
        }();
        REQUIRE(started);
    }

    bool module_loaded(const char *name) { return PyDict_GetItemString(PyImport_GetModuleDict(), name) != nullptr; }

    bool importable(const char *name)
    {
        const nb::object module = nb::steal(PyImport_ImportModule(name));
        if (module.is_valid()) { return true; }
        PyErr_Clear();
        return false;
    }

    DateTime tick(int count) { return MIN_ST + std::chrono::microseconds{count}; }

    /** ``key in container`` for a dict, set or frozenset. */
    bool contains(nb::handle container, nb::handle key)
    {
        const int result = PySequence_Contains(container.ptr(), key.ptr());
        if (result < 0) { throw nb::python_error(); }
        return result == 1;
    }

    const hgraph::ValueTypeMetaData *int_meta() { return TypeRegistry::instance().register_scalar<Int>("int"); }
    const hgraph::ValueTypeMetaData *str_meta() { return TypeRegistry::instance().register_scalar<Str>("str"); }
}  // namespace

TEST_CASE("python-user-nodes: the provider is registered at load and the process has no _hgraph module",
          "[python_user_nodes][rfc0035]")
{
    ensure_interpreter();

    // Linking the bridge unit registered the table (RFC 0035, "Registered
    // whenever, before the first conversion"); no module initializer ran.
    REQUIRE(hgraph::python_ops() != nullptr);
    CHECK_FALSE(module_loaded("_hgraph"));
    CHECK_FALSE(module_loaded("hgraph"));
    // Attaching again is harmless (a host cannot know whether it is first).
    CHECK_NOTHROW(hgraph::python_bridge::attach_embedded_interpreter());
    CHECK_FALSE(module_loaded("_hgraph"));
    // The bridge's Python surface for a dense scalar list is a numpy array
    // (same surface as the module); the host supplies numpy.
    REQUIRE(importable("numpy"));
}

TEST_CASE("python-user-nodes: a scalar round-trips through the registered table",
          "[python_user_nodes][rfc0035]")
{
    ensure_interpreter();

    Value count{ValuePlanFactory::instance().type_for(int_meta())};
    from_python(count, nb::int_{42});
    CHECK(nb::cast<std::int64_t>(to_python(count)) == 42);

    Value label{ValuePlanFactory::instance().type_for(str_meta())};
    from_python(label, nb::str{"forty-two"});
    CHECK(nb::cast<std::string>(to_python(label)) == "forty-two");

    // ``None`` resets: an empty value reads back as ``None``.
    from_python(count, nb::none());
    CHECK(to_python(count).is_none());

    CHECK_FALSE(module_loaded("_hgraph"));
}

TEST_CASE("python-user-nodes: a compact list round-trips through the registered table",
          "[python_user_nodes][rfc0035]")
{
    ensure_interpreter();

    Value numbers{ValuePlanFactory::instance().type_for(TypeRegistry::instance().list(int_meta()))};
    nb::list source;
    source.append(1);
    source.append(2);
    source.append(3);
    from_python(numbers, source);

    // Dense, buffer-compatible elements export as the array the module
    // exports (nanobind's ndarray: the path that needs the attached state).
    const nb::object exported = to_python(numbers);
    CHECK(nb::cast<std::string>(exported.type().attr("__module__")) == "numpy");
    CHECK(nb::cast<std::string>(exported.type().attr("__name__")) == "ndarray");
    REQUIRE(nb::len(exported) == 3);
    CHECK(nb::cast<std::int64_t>(exported[0]) == 1);
    CHECK(nb::cast<std::int64_t>(exported[2]) == 3);

    // Elements without a buffer form export element by element.
    Value labels{ValuePlanFactory::instance().type_for(TypeRegistry::instance().list(str_meta()))};
    nb::list words;
    words.append("one");
    words.append("two");
    from_python(labels, words);
    const nb::object exported_words = to_python(labels);
    REQUIRE(nb::isinstance<nb::list>(exported_words));
    REQUIRE(nb::len(exported_words) == 2);
    CHECK(nb::cast<std::string>(exported_words[1]) == "two");

    CHECK_FALSE(module_loaded("_hgraph"));
}

TEST_CASE("python-user-nodes: a TSB output converts through the registered table",
          "[python_user_nodes][rfc0035]")
{
    ensure_interpreter();
    auto       &registry = TypeRegistry::instance();
    const auto *bundle =
        registry.tsb("PythonUserNodesProbe", {{"count", registry.ts(int_meta())}, {"label", registry.ts(str_meta())}});
    TSOutput output{*bundle};

    const DateTime t1 = tick(1);
    nb::dict       first;
    first["count"] = 3;
    first["label"] = "three";
    {
        auto mutation = output.view(t1).begin_mutation(t1);
        REQUIRE(from_python(mutation, first));
    }
    // No CompoundScalar class is registered for the schema (that is the
    // module's doing), so the value is the plain field mapping.
    nb::object value = value_to_python(output.view(t1).data_view());
    REQUIRE(nb::isinstance<nb::dict>(value));
    CHECK(nb::cast<std::int64_t>(value["count"]) == 3);
    CHECK(nb::cast<std::string>(value["label"]) == "three");

    const DateTime t2 = tick(2);
    nb::dict       partial;
    partial["count"] = 4;
    {
        auto mutation = output.view(t2).begin_mutation(t2);
        REQUIRE(from_python(mutation, partial));
    }
    const nb::object delta = delta_value_to_python(output.view(t2).data_view(), t2);
    REQUIRE(nb::isinstance<nb::dict>(delta));
    CHECK(nb::len(delta) == 1);
    CHECK(nb::cast<std::int64_t>(delta["count"]) == 4);
    value = value_to_python(output.view(t2).data_view());
    CHECK(nb::cast<std::int64_t>(value["count"]) == 4);
    CHECK(nb::cast<std::string>(value["label"]) == "three");

    CHECK_FALSE(module_loaded("_hgraph"));
}

TEST_CASE("python-user-nodes: a TSD output converts through the registered table",
          "[python_user_nodes][rfc0035]")
{
    ensure_interpreter();
    auto    &registry = TypeRegistry::instance();
    TSOutput output{*registry.tsd(int_meta(), registry.ts(str_meta()))};

    const DateTime t1 = tick(1);
    nb::dict       first;
    first[nb::int_{1}] = "one";
    first[nb::int_{2}] = "two";
    {
        auto mutation = output.view(t1).begin_mutation(t1);
        REQUIRE(from_python(mutation, first));
    }
    nb::object value = value_to_python(output.view(t1).data_view());
    REQUIRE(nb::isinstance<nb::dict>(value));
    CHECK(nb::len(value) == 2);
    CHECK(nb::cast<std::string>(value[nb::int_{2}]) == "two");

    // A plain mapping is a replacement: key 1 leaves, key 3 arrives.
    const DateTime t2 = tick(2);
    nb::dict       replacement;
    replacement[nb::int_{2}] = "two";
    replacement[nb::int_{3}] = "three";
    {
        auto mutation = output.view(t2).begin_mutation(t2);
        REQUIRE(from_python(mutation, replacement));
    }
    value = value_to_python(output.view(t2).data_view());
    CHECK(nb::len(value) == 2);
    CHECK(nb::cast<std::string>(value[nb::int_{3}]) == "three");
    CHECK_FALSE(contains(value, nb::int_{1}));

    const nb::object delta = delta_value_to_python(output.view(t2).data_view(), t2);
    REQUIRE(nb::isinstance<nb::dict>(delta));
    const nb::object removed  = delta["removed"];
    const nb::object modified = delta["modified"];
    CHECK(nb::len(removed) == 1);
    CHECK(contains(removed, nb::int_{1}));
    // A replacement writes every listed child, so both keys are modified.
    CHECK(nb::len(modified) == 2);
    CHECK(nb::cast<std::string>(modified[nb::int_{3}]) == "three");
    CHECK(nb::cast<std::string>(modified[nb::int_{2}]) == "two");

    CHECK_FALSE(module_loaded("_hgraph"));
}

TEST_CASE("python-user-nodes: the stdlib enums' conversions stay the module's", "[python_user_nodes][rfc0035]")
{
    ensure_interpreter();

    // Their Python values are the hgraph package's Enum classes, which this
    // process does not have, so the standalone table has no conversion for
    // them and the forwarder says so (RFC 0035, "Unresolved questions").
    // The operators register the enum scalars, as the module does.
    hgraph::stdlib::register_standard_operators();
    const Value policy{hgraph::stdlib::DivideByZero::Nan};
    CHECK_THROWS_WITH(to_python(policy), Catch::Matchers::ContainsSubstring("no Python conversion is registered"));

    CHECK_FALSE(module_loaded("_hgraph"));
}
