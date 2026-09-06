#include "descriptor/module_descriptor.h"

#include <catch2/catch_test_macros.hpp>

#include <limits>
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
