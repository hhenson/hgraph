#include "descriptor/module_descriptor.h"

#include <catch2/catch_test_macros.hpp>

#include <string>

namespace descriptor = hgl::descriptor;
namespace gir        = hgl::hgraph_ir;

TEST_CASE("module descriptors normalize public and provider inventories", "[descriptor]") {
    gir::Module module;
    module.path       = "acme.prices";
    module.structures = {
        gir::StructContract{.identity = "acme.prices.Hidden"},
        gir::StructContract{.identity = "acme.prices.Quote", .exported = true},
        gir::StructContract{.identity = "acme.prices.Value", .exported = true, .abstract = true},
    };
    module.operators = {
        gir::OperatorContract{.identity = "hgraph.std.add", .registry_name = "add_", .imported = true},
        gir::OperatorContract{.identity = "acme.prices.summarize", .registry_name = "acme.prices.summarize"},
    };
    module.callables = {
        gir::Callable{.identity = "acme.prices.helper"},
        gir::Callable{
            .identity = "acme.prices.total", .visibility = gir::CallableVisibility::Export, .kind = gir::CallableKind::Composition},
        gir::Callable{.identity               = "acme.prices.summarize#3",
                      .operator_identity      = "acme.prices.summarize",
                      .operator_registry_name = "acme.prices.summarize",
                      .visibility             = gir::CallableVisibility::Implementation,
                      .kind                   = gir::CallableKind::RuntimeNode},
    };
    module.provider_requirements = {"zeta", "alpha", "zeta"};

    descriptor::DescribeOptions options;
    options.language_version    = "0.1-test";
    options.public_headers      = {"prices.h", "prices.h"};
    options.cmake_packages      = {"hgraph"};
    options.imported_targets    = {"zeta::native", "hgraph::core", "zeta::native"};
    options.registration_symbol = "acme::prices::register_operators";

    const descriptor::ModuleDescriptor result = descriptor::describe_module(module, std::move(options));

    REQUIRE(result.format_version == descriptor::module_descriptor_format_version);
    CHECK(result.module_identity == "acme.prices");
    CHECK(result.provider_identity == "acme.prices");
    CHECK(result.provider_requirements == std::vector<std::string>{"alpha", "zeta"});
    REQUIRE(result.interface.size() == 4U);
    CHECK(result.interface[0].identity == "acme.prices.Quote");
    CHECK(result.interface[1].identity == "acme.prices.Value");
    CHECK(result.interface[1].abstract);
    CHECK(result.interface[2].identity == "acme.prices.summarize");
    CHECK(result.interface[3].identity == "acme.prices.total");
    REQUIRE(result.implementations.size() == 1U);
    CHECK(result.implementations.front().execution == descriptor::ExecutionKind::RuntimeNode);
    CHECK(result.build.public_headers == std::vector<std::string>{"prices.h"});
    CHECK(result.build.imported_targets == std::vector<std::string>{"hgraph::core", "zeta::native"});
}

TEST_CASE("module descriptor JSON is canonical and reviewable", "[descriptor]") {
    descriptor::ModuleDescriptor module;
    module.module_identity   = "acme.\"prices\"";
    module.language_version  = "test\nversion";
    module.provider_identity = "acme.prices";
    module.interface         = {
        {.category = descriptor::DeclarationCategory::Operator, .identity = "acme.prices.value", .registry_name = "value"}};
    module.provider_requirements     = {"hgraph.stdlib"};
    module.build.public_headers      = {"prices.h"};
    module.build.cmake_packages      = {"hgraph"};
    module.build.imported_targets    = {"hgraph::core"};
    module.build.registration_symbol = "acme::prices::register_operators";

    CHECK(descriptor::to_json(module) == R"json({
  "format": "hgl.module",
  "format_version": 1,
  "module": {
    "identity": "acme.\"prices\"",
    "language_version": "test\nversion"
  },
  "interface": [
    {
      "category": "operator",
      "identity": "acme.prices.value",
      "registry_name": "value"
    }
  ],
  "provider": {
    "identity": "acme.prices",
    "implementations": [],
    "requires": [
      "hgraph.stdlib"
    ]
  },
  "build": {
    "public_headers": [
      "prices.h"
    ],
    "cmake_packages": [
      "hgraph"
    ],
    "imported_targets": [
      "hgraph::core"
    ],
    "registration": {
      "kind": "cpp",
      "symbol": "acme::prices::register_operators"
    }
  }
}
)json");
}
