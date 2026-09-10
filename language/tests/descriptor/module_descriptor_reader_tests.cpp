#include "descriptor/import_catalog.h"
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
            descriptor::TypeRecord{.category = descriptor::TypeCategory::Reference, .children = {0U}},
            descriptor::TypeRecord{.category = descriptor::TypeCategory::Symbol, .nominal_identity = "checks.reader.State"},
            descriptor::TypeRecord{.category = descriptor::TypeCategory::Signal},
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

        result.native_types = {
            descriptor::NativeTypeDeclaration{.category      = descriptor::NativeTypeCategory::OpaqueState,
                                              .identity      = "checks.reader.State",
                                              .cpp_type      = "checks::reader::State",
                                              .public_header = "checks_reader.h"},
        };
        descriptor::NativeDeclaration update;
        update.identity             = "checks.reader.update";
        update.cpp_symbol           = "checks::reader::update";
        update.signature.parameters = {{"state", "checks.reader.update::state", false, 4U, descriptor::no_schema_id},
                                       {"value", "checks.reader.update::value", false, 0U, descriptor::no_schema_id}};
        update.signature.result     = 0U;
        update.phases               = {descriptor::NativePhase::Evaluation};
        update.effects              = {descriptor::NativeEffect::Mutation};
        update.parameters           = {
            {"state", {descriptor::NativeOwnership::Borrowed, {}, true}},
            {"value", {descriptor::NativeOwnership::Value}},
        };
        update.thread_safety = descriptor::NativeThreadSafety::NodeLocal;

        descriptor::NativeDeclaration construct;
        construct.category         = descriptor::NativeDeclarationCategory::Constructor;
        construct.identity         = "checks.reader.make_state";
        construct.cpp_symbol       = "checks::reader::make_state";
        construct.signature.result = 4U;
        construct.phases           = {descriptor::NativePhase::Start};
        construct.effects          = {descriptor::NativeEffect::Allocation};
        construct.result.ownership = descriptor::NativeOwnership::Owned;
        construct.exception_policy = descriptor::NativeExceptionPolicy::Translated;
        result.native_declarations = {std::move(update), std::move(construct)};

        result.provider_requirements     = {"hgraph.std"};
        result.build.public_headers      = {"checks_reader.h"};
        result.build.cmake_packages      = {"hgraph"};
        result.build.imported_targets    = {"hgraph::core"};
        result.build.runtime_images      = {"libchecks_reader.so"};
        result.build.registration_symbol = "checks::reader::register_operators";
        result.build.lifecycle           = {1U, "hgl_query_native_module_v1"};
        descriptor::seal(result);
        return result;
    }

    descriptor::ModuleDescriptor scalar_native_descriptor() {
        descriptor::ModuleDescriptor result = minimal_descriptor();
        result.types                        = {
            descriptor::TypeRecord{.category = descriptor::TypeCategory::Scalar, .scalar_name = "f64"},
            descriptor::TypeRecord{.category = descriptor::TypeCategory::Scalar, .scalar_name = "i64"},
        };
        descriptor::NativeDeclaration blend;
        blend.identity                = "checks.reader::blend";
        blend.cpp_symbol              = "checks::reader::blend";
        blend.signature.parameters    = {{"value", "checks.reader::blend::value", false, 0U, descriptor::no_schema_id},
                                         {"window", "checks.reader::blend::window", true, 1U, descriptor::no_schema_id}};
        blend.signature.result        = 0U;
        blend.phases                  = {descriptor::NativePhase::Evaluation};
        blend.parameters              = {{"value", {}}, {"window", {}}};
        result.native_declarations    = {std::move(blend)};
        result.build.public_headers   = {"checks/reader.h"};
        result.build.cmake_packages   = {"checks"};
        result.build.imported_targets = {"checks::reader"};
        result.build.runtime_images   = {"libchecks_reader.so"};
        descriptor::seal(result);
        return result;
    }

    descriptor::ModuleDescriptor collection_native_descriptor() {
        descriptor::ModuleDescriptor result = minimal_descriptor();
        result.types                        = {
            descriptor::TypeRecord{.category = descriptor::TypeCategory::Scalar, .scalar_name = "i64"},
            descriptor::TypeRecord{
                .category = descriptor::TypeCategory::Symbol, .nominal_identity = "T", .binding_identity = "checks.reader::len::T"},
            descriptor::TypeRecord{.category = descriptor::TypeCategory::List, .children = {1U}, .size = 0U},
            descriptor::TypeRecord{.category = descriptor::TypeCategory::Set, .children = {1U}},
        };
        result.constant_expressions = {descriptor::ConstantExpressionRecord{
            .category           = descriptor::ConstantExpressionCategory::Parameter,
            .parameter_identity = "checks.reader::len::N",
        }};

        descriptor::NativeDeclaration list_len;
        list_len.identity             = "checks.reader::len";
        list_len.cpp_symbol           = "checks::reader::len";
        list_len.signature.generics   = {{"T", "checks.reader::len::T", false, descriptor::no_schema_id},
                                         {"N", "checks.reader::len::N", true, 0U}};
        list_len.signature.parameters = {{"value", "checks.reader::len::value", false, 2U, descriptor::no_schema_id}};
        list_len.signature.result     = 0U;
        list_len.phases               = {descriptor::NativePhase::Evaluation};
        list_len.parameters           = {{"value", {}, descriptor::NativeParameterAccess::InputView}};

        descriptor::NativeDeclaration set_len = list_len;
        set_len.signature.generics.resize(1U);
        set_len.signature.parameters.front().type = 3U;
        result.native_declarations                = {std::move(list_len), std::move(set_len)};
        descriptor::seal(result);
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

TEST_CASE("validated native scalar functions form a deterministic import catalog", "[descriptor][catalog]") {
    hgl::semantics::ModuleCatalog catalog;
    const auto                    error = descriptor::add_to_catalog(scalar_native_descriptor(), catalog);
    REQUIRE_FALSE(error);

    const hgl::semantics::ImportedFunction *function = catalog.find_function("checks.reader", "blend");
    REQUIRE(function != nullptr);
    CHECK(function->identity == "checks.reader::blend");
    CHECK(function->cpp_symbol == "checks::reader::blend");
    REQUIRE(function->parameters.size() == 2U);
    CHECK(function->parameters[0].type == hgl::semantics::ImportedScalarType::F64);
    CHECK(function->parameters[1].is_const);
    CHECK(function->result == hgl::semantics::ImportedScalarType::F64);
    CHECK(function->phases == std::vector{hgl::semantics::NativeCallPhase::Evaluation});
    CHECK(function->public_headers == std::vector<std::string>{"checks/reader.h"});
    CHECK(function->cmake_packages == std::vector<std::string>{"checks"});
    CHECK(function->imported_targets == std::vector<std::string>{"checks::reader"});
    CHECK(function->runtime_images == std::vector<std::string>{"libchecks_reader.so"});
}

TEST_CASE("validated native collection views form one overload family", "[descriptor][catalog]") {
    hgl::semantics::ModuleCatalog catalog;
    REQUIRE_FALSE(descriptor::add_to_catalog(collection_native_descriptor(), catalog));

    const std::span<const hgl::semantics::ImportedFunction> functions = catalog.find_functions("checks.reader", "len");
    REQUIRE(functions.size() == 2U);
    CHECK(functions[0].identity == "checks.reader::len");
    CHECK(functions[1].identity == "checks.reader::len");
    CHECK(functions[0].candidate_identity != functions[1].candidate_identity);
    CHECK(functions[0].parameters.front().access == hgl::semantics::NativeParameterAccess::InputView);
    CHECK(functions[1].parameters.front().access == hgl::semantics::NativeParameterAccess::InputView);
}

TEST_CASE("validated native signal views enter the import catalog", "[descriptor][catalog][signal]") {
    descriptor::ModuleDescriptor source = scalar_native_descriptor();
    source.types.push_back(descriptor::TypeRecord{.category = descriptor::TypeCategory::Signal});
    source.native_declarations.front().signature.parameters.front().type = 2U;
    source.native_declarations.front().parameters.front().access         = descriptor::NativeParameterAccess::InputView;
    source.descriptor_fingerprint.clear();
    descriptor::seal(source);

    hgl::semantics::ModuleCatalog catalog;
    REQUIRE_FALSE(descriptor::add_to_catalog(source, catalog));
    const hgl::semantics::ImportedFunction *function = catalog.find_function("checks.reader", "blend");
    REQUIRE(function != nullptr);
    REQUIRE(function->parameters.size() == 2U);
    CHECK(function->parameters.front().type.kind == hgl::semantics::ImportedTypeKind::Signal);
    CHECK(function->parameters.front().access == hgl::semantics::NativeParameterAccess::InputView);
    CHECK(function->support_error.empty());
}

TEST_CASE("catalog import rejects native identities outside their module namespace", "[descriptor][catalog]") {
    descriptor::ModuleDescriptor source         = scalar_native_descriptor();
    source.native_declarations.front().identity = "checks.reader.blend";
    source.descriptor_fingerprint.clear();
    descriptor::seal(source);

    hgl::semantics::ModuleCatalog catalog;
    const auto                    error = descriptor::add_to_catalog(source, catalog);
    REQUIRE(error);
    CHECK(error->path == "$.native.declarations[0].identity");
    CHECK(error->message == "native function identity must be 'checks.reader::<name>'");
    CHECK(catalog.modules().empty());
}

TEST_CASE("catalog preserves unsupported native declarations for precise import diagnostics", "[descriptor][catalog]") {
    SECTION("effects") {
        descriptor::ModuleDescriptor source        = scalar_native_descriptor();
        source.native_declarations.front().effects = {descriptor::NativeEffect::Allocation};
        source.descriptor_fingerprint.clear();
        descriptor::seal(source);

        hgl::semantics::ModuleCatalog catalog;
        REQUIRE_FALSE(descriptor::add_to_catalog(source, catalog));
        const hgl::semantics::ImportedFunction *function = catalog.find_function("checks.reader", "blend");
        REQUIRE(function != nullptr);
        CHECK(function->support_error == "native value calls with declared effects are not supported yet");
    }

    SECTION("phase") {
        descriptor::ModuleDescriptor source       = scalar_native_descriptor();
        source.native_declarations.front().phases = {descriptor::NativePhase::Wiring};
        source.descriptor_fingerprint.clear();
        descriptor::seal(source);

        hgl::semantics::ModuleCatalog catalog;
        REQUIRE_FALSE(descriptor::add_to_catalog(source, catalog));
        const hgl::semantics::ImportedFunction *function = catalog.find_function("checks.reader", "blend");
        REQUIRE(function != nullptr);
        CHECK(function->support_error == "native value calls currently require the evaluation phase only");
    }
}

TEST_CASE("module descriptor reader accepts compatible unknown members", "[descriptor][reader]") {
    const descriptor::ModuleDescriptor expected = rich_descriptor();
    std::string                        json     = descriptor::to_json(expected);
    replace_once(json, "  \"format\":", "  \"future_top_level\": {\"value\": true},\n  \"format\":");
    replace_once(json, "    \"identity\": \"checks.reader\",",
                 "    \"identity\": \"checks.reader\",\n    \"future_module_member\": [1, 2, 3],");

    const descriptor::ReadResult result = descriptor::read_json(json);
    REQUIRE(result);
    CHECK(result.value == expected);
}

TEST_CASE("module descriptor reader rejects a stale canonical fingerprint", "[descriptor][reader][fingerprint]") {
    descriptor::ModuleDescriptor source = rich_descriptor();
    std::string                  json   = descriptor::to_json(source);
    replace_once(json, "\"identity\": \"checks.reader\"", "\"identity\": \"checks.changed\"");

    check_error(descriptor::read_json(json), "$.module.descriptor_fingerprint",
                "descriptor fingerprint does not match canonical contents");
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
        replace_once(json, "\"format_version\": 2", "\"format_version\": 3");
        check_error(descriptor::read_json(json), "$.format_version", "unsupported descriptor format version 3");
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
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.schema.types[0].name", "unknown scalar type 'mystery'");
    }

    SECTION("fixed-shape type has the wrong child count") {
        source.types.front().category = descriptor::TypeCategory::Map;
        source.types.front().scalar_name.clear();
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.schema.types[0].children",
                    "type requires exactly 2 children");
    }

    SECTION("reference type has the wrong child count") {
        source.types.front().category = descriptor::TypeCategory::Reference;
        source.types.front().scalar_name.clear();
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.schema.types[0].children",
                    "type requires exactly 1 child");
    }

    SECTION("leaf type has children") {
        source.types.push_back(source.types.front());
        source.types.front().children = {1U};
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.schema.types[0].children",
                    "type requires exactly 0 children");
    }

    SECTION("signal cannot be nested") {
        source.types.push_back(descriptor::TypeRecord{.category = descriptor::TypeCategory::Signal});
        source.types.front().category = descriptor::TypeCategory::List;
        source.types.front().scalar_name.clear();
        source.types.front().children = {1U};
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.schema.types[0].children[0]",
                    "'signal' is only valid as a complete non-const parameter type");
    }
}

TEST_CASE("module descriptors restrict signal to non-const inputs", "[descriptor][reader][signal]") {
    descriptor::ModuleDescriptor source = minimal_descriptor();
    source.types                        = {
        descriptor::TypeRecord{.category = descriptor::TypeCategory::Scalar, .scalar_name = "i64"},
        descriptor::TypeRecord{.category = descriptor::TypeCategory::Signal},
    };

    descriptor::InterfaceDeclaration observe;
    observe.identity             = "checks.reader.observe";
    observe.execution            = descriptor::ExecutionKind::RuntimeNode;
    observe.signature.parameters = {{"pulse", "checks.reader.observe::pulse", false, 1U, descriptor::no_schema_id}};
    observe.signature.result     = 0U;
    source.interface             = {observe};

    SECTION("a non-const signal parameter is valid") { REQUIRE(descriptor::read_json(descriptor::to_json(source))); }

    SECTION("signal cannot be const") {
        source.interface.front().signature.parameters.front().is_const = true;
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.interface[0].signature.parameters[0].type",
                    "'signal' is only valid as a complete non-const parameter type");
    }

    SECTION("signal cannot have a default") {
        source.constant_expressions.push_back(descriptor::ConstantExpressionRecord{.literal = hgl::ir::hir::Constant{true}});
        source.interface.front().signature.parameters.front().default_value = 0U;
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.interface[0].signature.parameters[0].default",
                    "a 'signal' input cannot have a default value");
    }

    SECTION("signal cannot be a result") {
        source.interface.front().signature.result = 1U;
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.interface[0].signature.result",
                    "'signal' is only valid as a complete non-const parameter type");
    }

    SECTION("signal cannot be a struct field") {
        source.interface.front().category  = descriptor::DeclarationCategory::Structure;
        source.interface.front().signature = {};
        source.interface.front().fields    = {{"pulse", 1U, descriptor::no_schema_id, "checks.reader.observe", false}};
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.interface[0].fields[0].type",
                    "'signal' is only valid as a complete non-const parameter type");
    }
}

TEST_CASE("module descriptor reader rejects unknown constant operators", "[descriptor][reader]") {
    descriptor::ModuleDescriptor source = minimal_descriptor();
    source.constant_expressions         = {
        descriptor::ConstantExpressionRecord{.literal = hgl::ir::hir::Constant{std::int64_t{1}}},
        descriptor::ConstantExpressionRecord{
            .category = descriptor::ConstantExpressionCategory::Unary, .operator_spelling = "bogus", .lhs = 0U},
    };

    check_error(descriptor::read_json(descriptor::to_json(source)), "$.schema.constant_expressions[1].operator",
                "unknown unary operator 'bogus'");

    source.constant_expressions[1] = descriptor::ConstantExpressionRecord{
        .category          = descriptor::ConstantExpressionCategory::Binary,
        .operator_spelling = "bogus",
        .lhs               = 0U,
        .rhs               = 0U,
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

TEST_CASE("native descriptor validation enforces the initial safety envelope", "[descriptor][reader][native]") {
    SECTION("native overloads require distinct signatures") {
        descriptor::ModuleDescriptor source = scalar_native_descriptor();
        source.native_declarations.push_back(source.native_declarations.front());
        source.descriptor_fingerprint.clear();
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.native.declarations[1].identity",
                    "duplicate native overload for declaration identity 'checks.reader::blend'");
    }

    SECTION("native overload signatures compare schema structure and alpha-equivalent generics") {
        descriptor::ModuleDescriptor source = collection_native_descriptor();
        const descriptor::SchemaId   i64{static_cast<descriptor::SchemaId>(source.types.size())};
        source.types.push_back({.category = descriptor::TypeCategory::Scalar, .scalar_name = "i64"});
        const descriptor::SchemaId element{static_cast<descriptor::SchemaId>(source.types.size())};
        source.types.push_back({.category         = descriptor::TypeCategory::Symbol,
                                .nominal_identity = "Element",
                                .binding_identity = "checks.reader::len#duplicate::Element"});
        const descriptor::SchemaId extent{static_cast<descriptor::SchemaId>(source.constant_expressions.size())};
        source.constant_expressions.push_back({.category           = descriptor::ConstantExpressionCategory::Parameter,
                                               .parameter_identity = "checks.reader::len#duplicate::Extent"});
        const descriptor::SchemaId list{static_cast<descriptor::SchemaId>(source.types.size())};
        source.types.push_back({.category = descriptor::TypeCategory::List, .children = {element}, .size = extent});

        descriptor::NativeDeclaration duplicate = source.native_declarations.front();
        duplicate.cpp_symbol                    = "checks::reader::duplicate_len";
        duplicate.signature.generics = {{"Element", "checks.reader::len#duplicate::Element", false, descriptor::no_schema_id},
                                        {"Extent", "checks.reader::len#duplicate::Extent", true, i64}};
        duplicate.signature.parameters.front().binding_identity = "checks.reader::len#duplicate::value";
        duplicate.signature.parameters.front().type             = list;
        source.native_declarations.push_back(std::move(duplicate));
        source.descriptor_fingerprint.clear();

        check_error(descriptor::read_json(descriptor::to_json(source)), "$.native.declarations[2].identity",
                    "duplicate native overload for declaration identity 'checks.reader::len'");
    }

    SECTION("collection parameters require explicit input-view access") {
        descriptor::ModuleDescriptor source                          = collection_native_descriptor();
        source.native_declarations.front().parameters.front().access = descriptor::NativeParameterAccess::Value;
        source.descriptor_fingerprint.clear();
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.native.declarations[0].signature.parameters[0].type",
                    "a native collection parameter requires input-view access");
    }

    SECTION("input-view access requires a collection or signal pattern") {
        descriptor::ModuleDescriptor source                          = scalar_native_descriptor();
        source.native_declarations.front().parameters.front().access = descriptor::NativeParameterAccess::InputView;
        source.descriptor_fingerprint.clear();
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.native.declarations[0].signature.parameters[0].type",
                    "input-view access requires a collection or signal parameter");
    }

    SECTION("signal parameters require explicit input-view access") {
        descriptor::ModuleDescriptor source = scalar_native_descriptor();
        source.types.push_back(descriptor::TypeRecord{.category = descriptor::TypeCategory::Signal});
        source.native_declarations.front().signature.parameters.front().type = 2U;
        source.descriptor_fingerprint.clear();
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.native.declarations[0].signature.parameters[0].type",
                    "a native signal parameter requires input-view access");
    }

    SECTION("signal input-view parameters are valid") {
        descriptor::ModuleDescriptor source = scalar_native_descriptor();
        source.types.push_back(descriptor::TypeRecord{.category = descriptor::TypeCategory::Signal});
        source.native_declarations.front().signature.parameters.front().type = 2U;
        source.native_declarations.front().parameters.front().access         = descriptor::NativeParameterAccess::InputView;
        source.descriptor_fingerprint.clear();
        REQUIRE(descriptor::read_json(descriptor::to_json(source)));
    }

    SECTION("native types name a nominal descriptor type") {
        descriptor::ModuleDescriptor source  = rich_descriptor();
        source.native_types.front().identity = "checks.reader.Missing";
        source.descriptor_fingerprint.clear();
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.native.types[0].identity",
                    "native type does not name a nominal descriptor type");
    }

    SECTION("evaluation functions are noexcept") {
        descriptor::ModuleDescriptor source                 = rich_descriptor();
        source.native_declarations.front().exception_policy = descriptor::NativeExceptionPolicy::Translated;
        source.descriptor_fingerprint.clear();
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.native.declarations[0].exception",
                    "evaluation native functions must be noexcept");
    }

    SECTION("native symbols are exact qualified identifiers") {
        descriptor::ModuleDescriptor source           = rich_descriptor();
        source.native_declarations.front().cpp_symbol = "checks::reader::update(); injected";
        source.descriptor_fingerprint.clear();
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.native.declarations[0].cpp_symbol",
                    "C++ symbol must be one exact qualified identifier");

        source.native_declarations.front().cpp_symbol = "::checks::reader::update";
        CHECK(descriptor::validate(source) == std::nullopt);
    }

    SECTION("mutation identifies one borrowed argument") {
        descriptor::ModuleDescriptor source                                       = rich_descriptor();
        source.native_declarations.front().parameters.front().value.mutable_value = false;
        source.descriptor_fingerprint.clear();
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.native.declarations[0].effects",
                    "mutation requires exactly one explicitly mutable borrowed argument");
    }

    SECTION("borrowed results name their lifetime source") {
        descriptor::ModuleDescriptor source = rich_descriptor();
        auto                        &result = source.native_declarations.front().result;
        result.ownership                    = descriptor::NativeOwnership::Borrowed;
        source.descriptor_fingerprint.clear();
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.native.declarations[0].result.dependent_on",
                    "a borrowed native result must name its lifetime parameter");
    }

    SECTION("borrowed results depend on borrowed parameters") {
        descriptor::ModuleDescriptor source = rich_descriptor();
        auto                        &result = source.native_declarations.front().result;
        result.ownership                    = descriptor::NativeOwnership::Borrowed;
        result.dependent_on                 = "value";
        source.descriptor_fingerprint.clear();
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.native.declarations[0].result.dependent_on",
                    "a dependent lifetime must name a borrowed parameter");
    }

    SECTION("mutable parameters require mutation metadata") {
        descriptor::ModuleDescriptor source = rich_descriptor();
        auto                        &update = source.native_declarations.front();
        update.effects.clear();
        update.parameters[1].value = {descriptor::NativeOwnership::Borrowed, {}, true};
        source.descriptor_fingerprint.clear();
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.native.declarations[0].effects",
                    "mutation requires exactly one explicitly mutable borrowed argument");
    }

    SECTION("native collection patterns reject undeclared generics") {
        descriptor::ModuleDescriptor source                             = rich_descriptor();
        source.native_declarations.front().signature.parameters[1].type = 2U;
        source.descriptor_fingerprint.clear();
        check_error(descriptor::read_json(descriptor::to_json(source)),
                    "$.native.declarations[0].signature.parameters[1].type.children[0]",
                    "native signature type is outside the scalar, declared-native, and collection-view envelope");
    }

    SECTION("native signatures reject undeclared nominal types") {
        descriptor::ModuleDescriptor source = rich_descriptor();
        source.native_types.clear();
        source.descriptor_fingerprint.clear();
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.native.declarations[0].signature.parameters[0].type",
                    "native signature type is outside the scalar, declared-native, and collection-view envelope");
    }

    SECTION("constructors declare their owned result type") {
        descriptor::ModuleDescriptor source                = rich_descriptor();
        source.native_declarations.back().signature.result = descriptor::no_schema_id;
        source.descriptor_fingerprint.clear();
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.native.declarations[1].signature.result",
                    "a native constructor must declare its result type");
    }

    SECTION("lifecycle ABI and query symbol agree") {
        descriptor::ModuleDescriptor source = rich_descriptor();
        source.build.lifecycle.query_symbol = "wrong_query";
        source.descriptor_fingerprint.clear();
        check_error(descriptor::read_json(descriptor::to_json(source)), "$.build.lifecycle.query_symbol",
                    "query symbol does not match native module ABI version 1");
    }
}
