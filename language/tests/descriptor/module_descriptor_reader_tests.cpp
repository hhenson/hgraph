#include "descriptor/module_descriptor_reader.h"

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <utility>

namespace descriptor = hgl::descriptor;

namespace
{
    descriptor::ModuleDescriptor minimal_descriptor() {
        descriptor::ModuleDescriptor result;
        result.module_identity   = "checks.reader";
        result.language_version  = "0.1-test";
        result.provider_identity = "checks.reader";
        return result;
    }

    descriptor::ModuleDescriptor rich_descriptor() {
        descriptor::ModuleDescriptor result = minimal_descriptor();

        result.types = {
            descriptor::TypeRecord{.category = descriptor::TypeCategory::Scalar, .scalar_name = "i64"},
            descriptor::TypeRecord{
                .category         = descriptor::TypeCategory::Symbol,
                .nominal_identity = "T",
                .binding_identity = "checks.reader.map::T",
                .arguments = {{descriptor::TypeArgumentCategory::Type, 0U}, {descriptor::TypeArgumentCategory::Constant, 0U}}},
            descriptor::TypeRecord{.category = descriptor::TypeCategory::Rolling, .children = {1U}, .size = 0U},
        };

        descriptor::ConstantExpressionRecord integer;
        integer.literal = hgl::ir::hir::Constant{std::numeric_limits<std::int64_t>::max()};
        descriptor::ConstantExpressionRecord floating;
        floating.literal = hgl::ir::hir::Constant{std::numeric_limits<double>::infinity()};
        descriptor::ConstantExpressionRecord text;
        text.literal                = hgl::ir::hir::Constant{std::string{"reader\nvalue"}};
        result.constant_expressions = {
            std::move(integer),
            std::move(floating),
            std::move(text),
            descriptor::ConstantExpressionRecord{
                .category = descriptor::ConstantExpressionCategory::Unary, .operator_spelling = "-", .lhs = 0U},
            descriptor::ConstantExpressionRecord{
                .category = descriptor::ConstantExpressionCategory::Binary, .operator_spelling = "+", .lhs = 0U, .rhs = 0U},
            descriptor::ConstantExpressionRecord{.category = descriptor::ConstantExpressionCategory::Sequence,
                                                 .elements = {{descriptor::no_schema_id, 0U}, {0U, 1U}}},
            descriptor::ConstantExpressionRecord{.category = descriptor::ConstantExpressionCategory::Index, .lhs = 5U, .rhs = 0U},
            descriptor::ConstantExpressionRecord{
                .category = descriptor::ConstantExpressionCategory::Field, .lhs = 5U, .member = "value"},
            descriptor::ConstantExpressionRecord{.category = descriptor::ConstantExpressionCategory::Tuple, .items = {0U, 1U, 2U}},
            descriptor::ConstantExpressionRecord{.category         = descriptor::ConstantExpressionCategory::Construct,
                                                 .constructed_type = 0U,
                                                 .arguments        = {{"value", 0U}},
                                                 .delta            = true},
            descriptor::ConstantExpressionRecord{.category           = descriptor::ConstantExpressionCategory::Parameter,
                                                 .parameter_identity = "checks.reader.map::max_size"},
        };

        result.constraints = {
            descriptor::ConstraintRecord{.category = descriptor::ConstraintCategory::Symbol, .identity = "T"},
            descriptor::ConstraintRecord{.category = descriptor::ConstraintCategory::Type, .type = 0U},
            descriptor::ConstraintRecord{.category = descriptor::ConstraintCategory::Value, .value = 0U},
            descriptor::ConstraintRecord{.category = descriptor::ConstraintCategory::Set, .elements = {1U}},
            descriptor::ConstraintRecord{.category = descriptor::ConstraintCategory::Call, .identity = "schema", .arguments = {0U}},
            descriptor::ConstraintRecord{.category      = descriptor::ConstraintCategory::Operator,
                                         .identity      = "hgraph.std.add",
                                         .registry_name = "add_",
                                         .result        = 0U,
                                         .arguments     = {0U}},
            descriptor::ConstraintRecord{.category          = descriptor::ConstraintCategory::Relation,
                                         .operator_spelling = "in",
                                         .relation_category = "admission",
                                         .lhs               = 0U,
                                         .rhs               = 3U},
            descriptor::ConstraintRecord{.category = descriptor::ConstraintCategory::Not, .operand = 6U},
            descriptor::ConstraintRecord{
                .category = descriptor::ConstraintCategory::Logic, .operator_spelling = "and", .lhs = 7U, .rhs = 2U},
        };

        descriptor::InterfaceDeclaration structure;
        structure.category               = descriptor::DeclarationCategory::Structure;
        structure.identity               = "checks.reader.Record";
        structure.abstract               = true;
        structure.signature.generics     = {{"T", "checks.reader.Record::T", false, descriptor::no_schema_id}};
        structure.signature.requirements = 6U;
        structure.parents                = {1U};
        structure.fields                 = {{"value", 0U, 0U, "checks.reader.Record", true}};

        descriptor::InterfaceDeclaration operation;
        operation.category               = descriptor::DeclarationCategory::Operator;
        operation.identity               = "checks.reader.map";
        operation.registry_name          = "checks.reader.map";
        operation.signature.generics     = {{"T", "checks.reader.map::T", false, descriptor::no_schema_id},
                                            {"max_size", "checks.reader.map::max_size", true, 0U}};
        operation.signature.parameters   = {{"window", "checks.reader.map::window", false, 2U, 0U}};
        operation.signature.result       = 0U;
        operation.signature.requirements = 8U;

        descriptor::InterfaceDeclaration function;
        function.category             = descriptor::DeclarationCategory::Function;
        function.identity             = "checks.reader.public_value";
        function.execution            = descriptor::ExecutionKind::Composition;
        function.signature.parameters = {{"value", "checks.reader.public_value::value", false, 0U, descriptor::no_schema_id}};
        function.signature.result     = 0U;
        result.interface              = {std::move(structure), std::move(operation), std::move(function)};

        descriptor::Implementation implementation;
        implementation.identity               = "checks.reader.map#1";
        implementation.operator_identity      = "checks.reader.map";
        implementation.operator_registry_name = "checks.reader.map";
        implementation.execution              = descriptor::ExecutionKind::RuntimeNode;
        implementation.signature.parameters   = {{"window", "checks.reader.map#1::window", false, 2U, 0U}};
        implementation.signature.result       = 0U;
        result.implementations                = {std::move(implementation)};

        result.provider_requirements     = {"hgraph.std"};
        result.build.public_headers      = {"checks_reader.h"};
        result.build.cmake_packages      = {"hgraph"};
        result.build.imported_targets    = {"hgraph::core"};
        result.build.registration_symbol = "checks::reader::register_operators";
        return result;
    }

    void replace_once(std::string &text, std::string_view from, std::string_view to) {
        const std::size_t offset = text.find(from);
        REQUIRE(offset != std::string::npos);
        text.replace(offset, from.size(), to);
    }

    void check_error(const descriptor::ReadResult &result, std::string_view path, std::string_view message) {
        REQUIRE_FALSE(result);
        REQUIRE(result.error);
        CHECK(result.error->path == path);
        CHECK(result.error->message == message);
    }
}  // namespace

TEST_CASE("module descriptor reader round-trips the complete version-one model", "[descriptor][reader]") {
    const descriptor::ModuleDescriptor expected = rich_descriptor();
    const descriptor::ReadResult       result   = descriptor::read_json(descriptor::to_json(expected));

    REQUIRE(result);
    REQUIRE(result.value);
    CHECK(*result.value == expected);
    CHECK_FALSE(result.error);
}

TEST_CASE("module descriptor reader accepts compatible unknown members", "[descriptor][reader]") {
    std::string json = descriptor::to_json(minimal_descriptor());
    replace_once(json, "  \"format\":", "  \"future_top_level\": {\"value\": true},\n  \"format\":");
    replace_once(json, "    \"identity\": \"checks.reader\",",
                 "    \"identity\": \"checks.reader\",\n    \"future_module_member\": [1, 2, 3],");

    const descriptor::ReadResult result = descriptor::read_json(json);
    REQUIRE(result);
    CHECK(result.value == minimal_descriptor());
}

TEST_CASE("module descriptor reader rejects malformed envelopes", "[descriptor][reader]") {
    SECTION("invalid JSON") {
        const descriptor::ReadResult result = descriptor::read_json("{");
        REQUIRE_FALSE(result);
        REQUIRE(result.error);
        CHECK(result.error->path == "$");
        CHECK(result.error->message.starts_with("invalid JSON:"));
    }

    SECTION("duplicate members") {
        std::string json = descriptor::to_json(minimal_descriptor());
        replace_once(json, "  \"format\": \"hgl.module\",", "  \"format\": \"hgl.module\",\n  \"format\": \"hgl.module\",");
        check_error(descriptor::read_json(json), "$.format", "duplicate member");
    }

    SECTION("duplicate members inside compatible unknown values") {
        std::string json = descriptor::to_json(minimal_descriptor());
        replace_once(json, "  \"format\":", "  \"future\": {\"nested\": {\"value\": 1, \"value\": 2}},\n  \"format\":");
        check_error(descriptor::read_json(json), "$.future.nested.value", "duplicate member");
    }

    SECTION("wrong format") {
        std::string json = descriptor::to_json(minimal_descriptor());
        replace_once(json, "\"format\": \"hgl.module\"", "\"format\": \"other.module\"");
        check_error(descriptor::read_json(json), "$.format", "expected 'hgl.module'");
    }

    SECTION("unsupported version") {
        std::string json = descriptor::to_json(minimal_descriptor());
        replace_once(json, "\"format_version\": 1", "\"format_version\": 2");
        check_error(descriptor::read_json(json), "$.format_version", "unsupported descriptor format version 2");
    }
}

TEST_CASE("module descriptor reader rejects malformed schema records", "[descriptor][reader]") {
    descriptor::ModuleDescriptor source = minimal_descriptor();
    source.types = {descriptor::TypeRecord{.category = descriptor::TypeCategory::Scalar, .scalar_name = "i64"}};

    SECTION("unknown record kind") {
        std::string json = descriptor::to_json(source);
        replace_once(json, "\"kind\": \"scalar\"", "\"kind\": \"mystery\"");
        check_error(descriptor::read_json(json), "$.schema.types[0].kind", "unknown value 'mystery'");
    }

    SECTION("record id does not match its position") {
        std::string json = descriptor::to_json(source);
        replace_once(json, "\"id\": 0", "\"id\": 1");
        check_error(descriptor::read_json(json), "$.schema.types[0].id", "record id does not match array index");
    }

    SECTION("dangling reference") {
        source.types.front().category = descriptor::TypeCategory::Tuple;
        source.types.front().scalar_name.clear();
        source.types.front().children = {4U};
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.schema.types[0].children[0]",
                    "dangling type reference 4");
    }

    SECTION("missing category payload") {
        source.types.front().scalar_name.clear();
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.schema.types[0].name",
                    "scalar type is missing its name");
    }

    SECTION("unknown scalar name") {
        source.types.front().scalar_name = "mystery";
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.schema.types[0].name",
                    "unknown scalar type 'mystery'");
    }

    SECTION("fixed-shape type has the wrong child count") {
        source.types.front().category = descriptor::TypeCategory::Map;
        source.types.front().scalar_name.clear();
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.schema.types[0].children",
                    "type requires exactly 2 children");
    }

    SECTION("leaf type has children") {
        source.types.push_back(source.types.front());
        source.types.front().children = {1U};
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.schema.types[0].children",
                    "type requires exactly 0 children");
    }
}

TEST_CASE("module descriptor reader rejects unknown constant operators", "[descriptor][reader]") {
    descriptor::ModuleDescriptor source = minimal_descriptor();
    source.constant_expressions = {
        descriptor::ConstantExpressionRecord{.literal = hgl::ir::hir::Constant{std::int64_t{1}}},
        descriptor::ConstantExpressionRecord{
            .category = descriptor::ConstantExpressionCategory::Unary, .operator_spelling = "bogus", .lhs = 0U},
    };

    check_error(descriptor::read_json(descriptor::to_json(source)), "$.schema.constant_expressions[1].operator",
                "unknown unary operator 'bogus'");

    source.constant_expressions[1] = descriptor::ConstantExpressionRecord{
        .category = descriptor::ConstantExpressionCategory::Binary,
        .operator_spelling = "bogus",
        .lhs = 0U,
        .rhs = 0U,
    };
    check_error(descriptor::read_json(descriptor::to_json(source)), "$.schema.constant_expressions[1].operator",
                "unknown binary operator 'bogus'");
}

TEST_CASE("module descriptor validation rejects incomplete semantic records", "[descriptor][reader]") {
    descriptor::ModuleDescriptor source = minimal_descriptor();
    source.constant_expressions.emplace_back();

    const std::optional<descriptor::ReadError> error = descriptor::validate(source);
    REQUIRE(error);
    CHECK(error->path == "$.schema.constant_expressions[0].literal");
    CHECK(error->message == "missing literal payload");
}
