#include "descriptor/module_descriptor.h"
#include "descriptor/sha256.h"

#include <catch2/catch_test_macros.hpp>

#include <limits>
#include <span>
#include <string>
#include <string_view>

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
    CHECK(result.descriptor_fingerprint.starts_with("sha256:"));
    CHECK(result.descriptor_fingerprint.size() == 71U);
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

TEST_CASE("source native functions become importable exact declarations", "[descriptor][native]") {
    gir::Module module;
    module.path     = "checks.native";
    module.bindings = {
        gir::Binding{.name = "T", .kind = gir::BindingKind::TypeParameter, .owner_identity = "checks.native::len#0"},
    };
    module.types = {
        gir::Type{.kind = hgl::ir::hir::TypeKind::Scalar, .scalar = hgl::ir::hir::ScalarType::I64},
        gir::Type{.kind = hgl::ir::hir::TypeKind::Symbol, .nominal_identity = "T", .binding = gir::BindingId{0U}},
        gir::Type{.kind = hgl::ir::hir::TypeKind::Set, .children = {gir::TypeId{1U}}},
    };
    module.native_functions = {
        gir::NativeFunction{
            .module_identity    = "checks.native",
            .identity           = "checks.native::len",
            .candidate_identity = "checks.native::len#0",
            .generics           = {gir::GenericParameter{.name = "T", .binding = gir::BindingId{0U}}},
            .parameters         = {gir::NativeParameter{
                .name = "value", .type = gir::TypeId{2U}, .access = hgl::ir::hir::NativeParameterAccess::InputView}},
            .result             = gir::TypeId{0U},
            .phases             = {hgl::ir::hir::NativePhase::Evaluation},
            .source_defined     = true,
        },
    };

    descriptor::DescribeOptions options;
    options.language_version = "0.1-test";
    options.public_headers   = {"native.h"};
    options.source_native_symbols.emplace_back("checks.native::len#0", "checks::native::native::len");
    const descriptor::ModuleDescriptor result = descriptor::describe_module(module, std::move(options));

    REQUIRE(result.native_declarations.size() == 1U);
    const descriptor::NativeDeclaration &native = result.native_declarations.front();
    CHECK(native.identity == "checks.native::len");
    CHECK(native.cpp_symbol == "checks::native::native::len");
    REQUIRE(native.signature.generics.size() == 1U);
    CHECK(native.signature.generics.front().name == "T");
    REQUIRE(native.parameters.size() == 1U);
    CHECK(native.parameters.front().access == descriptor::NativeParameterAccess::InputView);
    CHECK(native.exception_policy == descriptor::NativeExceptionPolicy::NoThrow);
}

TEST_CASE("module descriptors preserve type and parameter pack shape", "[descriptor][parameter-pack]") {
    gir::Module module;
    module.path     = "checks.packs";
    module.bindings = {
        gir::Binding{.name = "Fields", .kind = gir::BindingKind::TypeParameter, .owner_identity = "checks.packs.named"},
        gir::Binding{.name = "values", .kind = gir::BindingKind::SignalParameter, .owner_identity = "checks.packs.named"},
    };
    module.types = {
        gir::Type{.kind = hgl::ir::hir::TypeKind::Scalar, .scalar = hgl::ir::hir::ScalarType::I64},
        gir::Type{.kind = hgl::ir::hir::TypeKind::Symbol, .nominal_identity = "Fields", .binding = gir::BindingId{0U}},
    };
    module.operators = {gir::OperatorContract{
        .identity   = "checks.packs.named",
        .generics   = {gir::GenericParameter{.name = "Fields", .binding = gir::BindingId{0U}, .is_pack = true}},
        .parameters = {gir::Parameter{
            .name = "values", .type = gir::TypeId{1U}, .binding = gir::BindingId{1U}, .pack = gir::ParameterPack::Keyword}},
        .result     = gir::TypeId{0U},
    }};

    const descriptor::ModuleDescriptor result = descriptor::describe_module(module, {});
    REQUIRE(result.interface.size() == 1U);
    const descriptor::Signature &signature = result.interface.front().signature;
    REQUIRE(signature.generics.size() == 1U);
    CHECK(signature.generics.front().is_pack);
    REQUIRE(signature.parameters.size() == 1U);
    CHECK(signature.parameters.front().pack == descriptor::ParameterPack::Keyword);
    const std::string json = descriptor::to_json(result);
    CHECK(json.find("\"kind\": \"type_pack\"") != std::string::npos);
    CHECK(json.find("\"pack\": \"keyword\"") != std::string::npos);
}

TEST_CASE("module descriptors retain structured signatures layouts and constraints", "[descriptor][schema]") {
    gir::Module module;
    module.path     = "checks.schema";
    module.bindings = {
        gir::Binding{.name = "T", .kind = gir::BindingKind::TypeParameter, .owner_identity = "checks.schema.Range"},
        gir::Binding{.name = "T", .kind = gir::BindingKind::TypeParameter, .owner_identity = "checks.schema.summarize"},
        gir::Binding{.name = "max_size", .kind = gir::BindingKind::ConstParameter, .owner_identity = "checks.schema.summarize"},
        gir::Binding{.name = "window", .kind = gir::BindingKind::SignalParameter, .owner_identity = "checks.schema.summarize"},
    };
    module.types = {
        gir::Type{.kind = hgl::ir::hir::TypeKind::Scalar, .scalar = hgl::ir::hir::ScalarType::I64},
        gir::Type{.kind = hgl::ir::hir::TypeKind::Scalar, .scalar = hgl::ir::hir::ScalarType::F64},
        gir::Type{.kind = hgl::ir::hir::TypeKind::Symbol, .nominal_identity = "T", .binding = gir::BindingId{0}},
        gir::Type{.kind = hgl::ir::hir::TypeKind::Symbol, .nominal_identity = "T", .binding = gir::BindingId{1}},
        gir::Type{.kind = hgl::ir::hir::TypeKind::Rolling, .children = {gir::TypeId{3}}, .size = gir::ConstExprId{0}},
    };
    module.const_exprs = {
        gir::ConstExpr{.kind = gir::ConstExprKind::Parameter, .parameter = "max_size", .parameter_binding = gir::BindingId{2}},
        gir::ConstExpr{.kind = gir::ConstExprKind::Literal, .literal = hgl::ir::hir::Constant{true}},
    };
    module.constraints = {
        gir::Constraint{.node = gir::ConstraintSymbol{"T"}},
        gir::Constraint{.node = gir::ConstraintType{gir::TypeId{0}}},
        gir::Constraint{.node = gir::ConstraintType{gir::TypeId{1}}},
        gir::Constraint{.node = gir::ConstraintSet{{gir::ConstraintId{1}, gir::ConstraintId{2}}}},
        gir::Constraint{.node = gir::ConstraintRelation{gir::ConstraintRelationOp::In, gir::ConstraintId{0}, gir::ConstraintId{3}}},
        gir::Constraint{.node = gir::ConstraintValue{gir::ConstExprId{1}}},
        gir::Constraint{.node = gir::ConstraintCall{"schema", {gir::ConstraintId{0}}}},
        gir::Constraint{.node = gir::OperatorRequirement{"checks.schema.add", "add_", {gir::ConstraintId{0}}, gir::TypeId{3}}},
        gir::Constraint{.node = gir::ConstraintRelation{gir::ConstraintRelationOp::Equal, gir::ConstraintId{6},
                                                        gir::ConstraintId{7}, "compatibility"}},
        gir::Constraint{.node = gir::ConstraintNot{gir::ConstraintId{8}}},
        gir::Constraint{.node = gir::ConstraintLogic{gir::ConstraintLogicOp::And, gir::ConstraintId{9}, gir::ConstraintId{5}}},
    };
    module.structures = {
        gir::StructContract{
            .identity     = "checks.schema.Range",
            .exported     = true,
            .generics     = {gir::GenericParameter{"T", false, {}, gir::BindingId{0}}},
            .requirements = gir::ConstraintId{4},
            .fields       = {gir::StructField{.name = "lower", .type = gir::TypeId{2}, .origin_identity = "checks.schema.Range"}},
        },
    };
    module.operators = {
        gir::OperatorContract{
            .identity     = "checks.schema.summarize",
            .generics     = {gir::GenericParameter{"T", false, {}, gir::BindingId{1}},
                             gir::GenericParameter{"max_size", true, gir::TypeId{0}, gir::BindingId{2}}},
            .parameters   = {gir::Parameter{"window", false, gir::TypeId{4}, {}, gir::BindingId{3}}},
            .result       = gir::TypeId{3},
            .requirements = gir::ConstraintId{10},
        },
    };

    const descriptor::ModuleDescriptor result = descriptor::describe_module(module, {});

    REQUIRE(result.interface.size() == 2U);
    const descriptor::InterfaceDeclaration &range = result.interface[0];
    CHECK(range.identity == "checks.schema.Range");
    CHECK(range.signature.requirements == 0U);
    REQUIRE(range.fields.size() == 1U);
    CHECK(range.fields.front().origin_identity == "checks.schema.Range");

    const descriptor::InterfaceDeclaration &summarize = result.interface[1];
    REQUIRE(summarize.signature.generics.size() == 2U);
    CHECK(summarize.signature.generics[1].binding_identity == "checks.schema.summarize::max_size");
    REQUIRE(summarize.signature.parameters.size() == 1U);
    CHECK(summarize.signature.parameters.front().binding_identity == "checks.schema.summarize::window");
    REQUIRE(result.constant_expressions.size() == 2U);
    CHECK(result.constant_expressions.front().parameter_identity == "checks.schema.summarize::max_size");
    REQUIRE(result.constraints.size() == 11U);
    CHECK(result.constraints.front().operator_spelling == "in");
    CHECK(result.constraints[2].category == descriptor::ConstraintCategory::Set);
    CHECK(result.constraints[5].category == descriptor::ConstraintCategory::Logic);
    CHECK(result.constraints[6].category == descriptor::ConstraintCategory::Not);
    CHECK(result.constraints[8].category == descriptor::ConstraintCategory::Call);
    CHECK(result.constraints[9].registry_name == "add_");
    CHECK(result.constraints[10].category == descriptor::ConstraintCategory::Value);

    const std::string json = descriptor::to_json(result);
    CHECK(json.find("\"binding\": \"checks.schema.Range::T\"") != std::string::npos);
    CHECK(json.find("\"kind\": \"rolling\"") != std::string::npos);
    CHECK(json.find("\"operator\": \"in\"") != std::string::npos);
    CHECK(json.find("\"category\": \"compatibility\"") != std::string::npos);
}

TEST_CASE("module descriptors advertise concrete implementation materializations", "[descriptor][generics]") {
    gir::Module module;
    module.path     = "checks.materialized";
    module.bindings = {
        gir::Binding{.name = "T", .kind = gir::BindingKind::TypeParameter, .owner_identity = "checks.materialized.choose#1"},
        gir::Binding{.name = "value", .kind = gir::BindingKind::SignalParameter, .owner_identity = "checks.materialized.choose#1"},
    };
    module.types = {
        gir::Type{.kind = hgl::ir::hir::TypeKind::Scalar, .scalar = hgl::ir::hir::ScalarType::I64},
        gir::Type{.kind = hgl::ir::hir::TypeKind::Scalar, .scalar = hgl::ir::hir::ScalarType::F64},
        gir::Type{.kind = hgl::ir::hir::TypeKind::Symbol, .nominal_identity = "T", .binding = gir::BindingId{0}},
    };
    module.callables = {
        gir::Callable{
            .identity               = "checks.materialized.choose#1",
            .operator_identity      = "checks.materialized.choose",
            .operator_registry_name = "checks.materialized.choose",
            .visibility             = gir::CallableVisibility::Implementation,
            .kind                   = gir::CallableKind::Composition,
            .generics               = {gir::GenericParameter{"T", false, {}, gir::BindingId{0}}},
            .parameters             = {gir::Parameter{"value", false, gir::TypeId{2}, {}, gir::BindingId{1}}},
            .result                 = gir::TypeId{2},
        },
    };
    module.materializations = {
        gir::Materialization{
            .identity       = "checks.materialized.choose#1@instantiate:0",
            .implementation = gir::CallableId{0},
            .substitutions  = {gir::Substitution{.parameter = gir::BindingId{0}, .type = gir::TypeId{0}}},
        },
        gir::Materialization{
            .identity       = "checks.materialized.choose#1@instantiate:1",
            .implementation = gir::CallableId{0},
            .substitutions  = {gir::Substitution{.parameter = gir::BindingId{0}, .type = gir::TypeId{1}}},
        },
    };

    const descriptor::ModuleDescriptor result = descriptor::describe_module(module, {});
    REQUIRE(result.implementations.size() == 2);
    CHECK(result.implementations[0].identity == "checks.materialized.choose#1@instantiate:0");
    CHECK(result.implementations[1].identity == "checks.materialized.choose#1@instantiate:1");
    for (const descriptor::Implementation &implementation : result.implementations) {
        CHECK(implementation.signature.generics.empty());
        REQUIRE(implementation.signature.parameters.size() == 1);
        CHECK(implementation.signature.parameters.front().type == implementation.signature.result);
    }
    CHECK(result.types[result.implementations[0].signature.result].scalar_name == "i64");
    CHECK(result.types[result.implementations[1].signature.result].scalar_name == "f64");
}

TEST_CASE("module descriptors retain residual materialization generics", "[descriptor][generics]") {
    gir::Module module;
    module.path     = "checks.partial";
    module.bindings = {
        gir::Binding{.name = "T", .kind = gir::BindingKind::TypeParameter, .owner_identity = "checks.partial.keep#1"},
        gir::Binding{.name = "N", .kind = gir::BindingKind::ConstParameter, .owner_identity = "checks.partial.keep#1"},
        gir::Binding{.name = "value", .kind = gir::BindingKind::SignalParameter, .owner_identity = "checks.partial.keep#1"},
    };
    module.const_exprs = {
        gir::ConstExpr{.kind = gir::ConstExprKind::Parameter, .parameter = "N", .parameter_binding = gir::BindingId{1}},
    };
    module.types = {
        gir::Type{.kind = hgl::ir::hir::TypeKind::Scalar, .scalar = hgl::ir::hir::ScalarType::I64},
        gir::Type{.kind = hgl::ir::hir::TypeKind::Symbol, .nominal_identity = "T", .binding = gir::BindingId{0}},
        gir::Type{.kind = hgl::ir::hir::TypeKind::List, .children = {gir::TypeId{1}}, .size = gir::ConstExprId{0}},
    };
    module.callables = {
        gir::Callable{
            .identity               = "checks.partial.keep#1",
            .operator_identity      = "checks.partial.keep",
            .operator_registry_name = "checks.partial.keep",
            .visibility             = gir::CallableVisibility::Implementation,
            .kind                   = gir::CallableKind::Composition,
            .generics               = {gir::GenericParameter{"T", false, {}, gir::BindingId{0}},
                                       gir::GenericParameter{"N", true, gir::TypeId{0}, gir::BindingId{1}}},
            .parameters             = {gir::Parameter{"value", false, gir::TypeId{2}, {}, gir::BindingId{2}}},
            .result                 = gir::TypeId{2},
        },
    };
    module.materializations = {
        gir::Materialization{
            .identity       = "checks.partial.keep#1@instantiate:0",
            .implementation = gir::CallableId{0},
            .substitutions  = {gir::Substitution{.parameter = gir::BindingId{0}, .type = gir::TypeId{0}},
                               gir::Substitution{.parameter = gir::BindingId{1}, .retained = true}},
        },
    };

    const descriptor::ModuleDescriptor result = descriptor::describe_module(module, {});
    REQUIRE(result.implementations.size() == 1);
    const descriptor::Signature &signature = result.implementations.front().signature;
    REQUIRE(signature.generics.size() == 1);
    CHECK(signature.generics.front().name == "N");
    CHECK(signature.generics.front().is_const);
    REQUIRE(signature.parameters.size() == 1);
    CHECK(signature.parameters.front().type == signature.result);
    CHECK(result.types[signature.result].size != descriptor::no_schema_id);
    CHECK(result.constant_expressions[result.types[signature.result].size].parameter_identity == "checks.partial.keep#1::N");
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
  "format_version": 2,
  "module": {
    "identity": "acme.\"prices\"",
    "language_version": "test\nversion",
    "descriptor_fingerprint": ""
  },
  "interface": [
    {
      "category": "operator",
      "identity": "acme.prices.value",
      "registry_name": "value",
      "signature": {
        "generic_parameters": [],
        "parameters": [],
        "result": null,
        "requires": null
      }
    }
  ],
  "provider": {
    "identity": "acme.prices",
    "implementations": [],
    "requires": [
      "hgraph.stdlib"
    ]
  },
  "schema": {
    "types": [],
    "constant_expressions": [],
    "constraints": []
  },
  "native": {
    "types": [],
    "declarations": []
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
    "runtime_images": [],
    "registration": {
      "kind": "cpp",
      "symbol": "acme::prices::register_operators"
    },
    "lifecycle": {
      "abi_version": 0,
      "query_symbol": ""
    }
  }
}
)json");
}

TEST_CASE("module descriptor literals preserve values outside JSON's numeric model", "[descriptor][schema]") {
    descriptor::ModuleDescriptor module;
    module.constant_expressions = {
        descriptor::ConstantExpressionRecord{.literal = hgl::ir::hir::Constant{std::numeric_limits<std::int64_t>::max()}},
        descriptor::ConstantExpressionRecord{.literal = hgl::ir::hir::Constant{std::numeric_limits<double>::infinity()}},
        descriptor::ConstantExpressionRecord{.literal = hgl::ir::hir::Constant{-std::numeric_limits<double>::infinity()}},
    };

    const std::string json = descriptor::to_json(module);
    CHECK(json.find("\"value\": \"9223372036854775807\"") != std::string::npos);
    CHECK(json.find("\"value\": \"inf\"") != std::string::npos);
    CHECK(json.find("\"value\": \"-inf\"") != std::string::npos);
}

TEST_CASE("module descriptor fingerprints cover canonical contents", "[descriptor][fingerprint]") {
    descriptor::ModuleDescriptor module;
    module.module_identity   = "checks.fingerprint";
    module.provider_identity = "checks.fingerprint";

    descriptor::seal(module);
    const std::string original = module.descriptor_fingerprint;
    CHECK(original == descriptor::fingerprint(module));
    descriptor::seal(module);
    CHECK(module.descriptor_fingerprint == original);

    module.build.public_headers.push_back("changed.h");
    CHECK(descriptor::fingerprint(module) != original);
}

TEST_CASE("descriptor SHA-256 uses the standard digest", "[descriptor][fingerprint]") {
    const auto digest = [](std::string_view input) {
        const auto bytes = std::as_bytes(std::span{input.data(), input.size()});
        const auto value = descriptor::detail::sha256_hex(bytes);
        return std::string{value.data(), value.size()};
    };

    CHECK(digest("") == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    CHECK(digest("abc") == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    CHECK(digest("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq") ==
          "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1");
}

TEST_CASE("module descriptors preserve structured compile-time expressions", "[descriptor][schema]") {
    gir::Module module;
    module.path        = "checks.constants";
    module.types       = {gir::Type{.kind = hgl::ir::hir::TypeKind::Scalar, .scalar = hgl::ir::hir::ScalarType::I64}};
    module.const_exprs = {
        gir::ConstExpr{.kind = gir::ConstExprKind::Literal, .literal = hgl::ir::hir::Constant{std::int64_t{1}}},
        gir::ConstExpr{.kind = gir::ConstExprKind::Literal, .literal = hgl::ir::hir::Constant{std::int64_t{2}}},
        gir::ConstExpr{.kind = gir::ConstExprKind::Unary, .unary = hgl::ir::hir::UnaryOp::Negate, .lhs = gir::ConstExprId{0}},
        gir::ConstExpr{.kind   = gir::ConstExprKind::Binary,
                       .binary = hgl::ir::hir::BinaryOp::Add,
                       .lhs    = gir::ConstExprId{0},
                       .rhs    = gir::ConstExprId{1}},
        gir::ConstExpr{.kind     = gir::ConstExprKind::Sequence,
                       .elements = {{.value = gir::ConstExprId{0}}, {.key = gir::ConstExprId{0}, .value = gir::ConstExprId{1}}}},
        gir::ConstExpr{.kind = gir::ConstExprKind::Index, .lhs = gir::ConstExprId{4}, .rhs = gir::ConstExprId{0}},
        gir::ConstExpr{.kind = gir::ConstExprKind::Field, .lhs = gir::ConstExprId{4}, .member = "value"},
        gir::ConstExpr{.kind = gir::ConstExprKind::Tuple, .items = {gir::ConstExprId{0}, gir::ConstExprId{1}}},
        gir::ConstExpr{.kind             = gir::ConstExprKind::Construct,
                       .constructed_type = gir::TypeId{0},
                       .arguments        = {{"value", gir::ConstExprId{0}}},
                       .delta            = true},
    };
    module.callables = {
        gir::Callable{
            .identity   = "checks.constants.public_defaults",
            .visibility = gir::CallableVisibility::Export,
            .parameters = {gir::Parameter{"unary", false, gir::TypeId{0}, gir::ConstExprId{2}},
                           gir::Parameter{"binary", false, gir::TypeId{0}, gir::ConstExprId{3}},
                           gir::Parameter{"index", false, gir::TypeId{0}, gir::ConstExprId{5}},
                           gir::Parameter{"field", false, gir::TypeId{0}, gir::ConstExprId{6}},
                           gir::Parameter{"tuple", false, gir::TypeId{0}, gir::ConstExprId{7}},
                           gir::Parameter{"construct", false, gir::TypeId{0}, gir::ConstExprId{8}}},
            .result     = gir::TypeId{0},
        },
    };

    const descriptor::ModuleDescriptor result = descriptor::describe_module(module, {});

    REQUIRE(result.constant_expressions.size() == 9U);
    CHECK(result.constant_expressions[0].category == descriptor::ConstantExpressionCategory::Unary);
    CHECK(result.constant_expressions[2].category == descriptor::ConstantExpressionCategory::Binary);
    CHECK(result.constant_expressions[4].category == descriptor::ConstantExpressionCategory::Index);
    CHECK(result.constant_expressions[5].category == descriptor::ConstantExpressionCategory::Sequence);
    CHECK(result.constant_expressions[6].category == descriptor::ConstantExpressionCategory::Field);
    CHECK(result.constant_expressions[7].category == descriptor::ConstantExpressionCategory::Tuple);
    CHECK(result.constant_expressions[8].category == descriptor::ConstantExpressionCategory::Construct);

    const std::string json = descriptor::to_json(result);
    CHECK(json.find("\"member\": \"value\"") != std::string::npos);
    CHECK(json.find("\"elements\": [") != std::string::npos);
    CHECK(json.find("\"delta\": true") != std::string::npos);
}

TEST_CASE("module descriptor schema IDs ignore source arena and declaration order", "[descriptor][schema]") {
    gir::Module first;
    first.path  = "checks.order";
    first.types = {
        gir::Type{.kind = hgl::ir::hir::TypeKind::Scalar, .scalar = hgl::ir::hir::ScalarType::F64},
        gir::Type{.kind = hgl::ir::hir::TypeKind::Scalar, .scalar = hgl::ir::hir::ScalarType::I64},
    };
    first.callables = {
        gir::Callable{.identity = "checks.order.zeta", .visibility = gir::CallableVisibility::Export, .result = gir::TypeId{1}},
        gir::Callable{.identity = "checks.order.alpha", .visibility = gir::CallableVisibility::Export, .result = gir::TypeId{0}},
    };

    gir::Module second;
    second.path  = first.path;
    second.types = {
        gir::Type{.kind = hgl::ir::hir::TypeKind::Scalar, .scalar = hgl::ir::hir::ScalarType::I64},
        gir::Type{.kind = hgl::ir::hir::TypeKind::Scalar, .scalar = hgl::ir::hir::ScalarType::F64},
    };
    second.callables = {
        gir::Callable{.identity = "checks.order.alpha", .visibility = gir::CallableVisibility::Export, .result = gir::TypeId{1}},
        gir::Callable{.identity = "checks.order.zeta", .visibility = gir::CallableVisibility::Export, .result = gir::TypeId{0}},
    };

    CHECK(descriptor::to_json(descriptor::describe_module(first, {})) ==
          descriptor::to_json(descriptor::describe_module(second, {})));
}
