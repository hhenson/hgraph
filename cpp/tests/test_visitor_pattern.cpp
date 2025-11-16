/**
 * Unit tests for the Time Series Visitor Pattern implementation
 *
 * Tests both CRTP and Acyclic visitor patterns across all time series types.
 */

#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_template_test_macros.hpp>
#include <hgraph/types/time_series_visitor.h>
#include <hgraph/types/ts.h>
#include <hgraph/types/tsb.h>
#include <hgraph/types/tsl.h>
#include <hgraph/types/tsd.h>
#include <hgraph/types/tss.h>
#include <hgraph/types/tsw.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/ts_signal.h>
#include <vector>
#include <string>
#include <sstream>

using namespace hgraph;

// ============================================================================
// CRTP Visitor Tests
// ============================================================================

/**
 * Test CRTP visitor that collects type names
 */
struct TypeCollectorVisitor : TimeSeriesOutputVisitorCRTP<TypeCollectorVisitor> {
    std::vector<std::string> type_names;

    // TS types
    template<typename T>
    void visit(TimeSeriesValueOutput<T>& output) {
        type_names.push_back("TS<" + std::string(typeid(T).name()) + ">");
    }

    // TSB types
    void visit(TimeSeriesBundleOutput& output) {
        type_names.push_back("TSB");
    }

    // TSL types
    void visit(TimeSeriesListOutput& output) {
        type_names.push_back("TSL");
    }

    // TSD types
    template<typename K>
    void visit(TimeSeriesDictOutput_T<K>& output) {
        type_names.push_back("TSD<" + std::string(typeid(K).name()) + ">");
    }

    // TSS types
    template<typename T>
    void visit(TimeSeriesSetOutput_T<T>& output) {
        type_names.push_back("TSS<" + std::string(typeid(T).name()) + ">");
    }

    // TSW types
    template<typename T>
    void visit(TimeSeriesFixedWindowOutput<T>& output) {
        type_names.push_back("TSW_Fixed<" + std::string(typeid(T).name()) + ">");
    }

    template<typename T>
    void visit(TimeSeriesTimeWindowOutput<T>& output) {
        type_names.push_back("TSW_Time<" + std::string(typeid(T).name()) + ">");
    }

    // REF types
    void visit(TimeSeriesReferenceOutput& output) {
        type_names.push_back("REF");
    }
};

/**
 * Test CRTP visitor for Input types
 */
struct InputTypeCollectorVisitor : TimeSeriesInputVisitorCRTP<InputTypeCollectorVisitor> {
    std::vector<std::string> type_names;

    // TS types
    template<typename T>
    void visit(TimeSeriesValueInput<T>& input) {
        type_names.push_back("TS_Input<" + std::string(typeid(T).name()) + ">");
    }

    // TSB types
    void visit(TimeSeriesBundleInput& input) {
        type_names.push_back("TSB_Input");
    }

    // TSL types
    void visit(TimeSeriesListInput& input) {
        type_names.push_back("TSL_Input");
    }

    // TSD types
    template<typename K>
    void visit(TimeSeriesDictInput_T<K>& input) {
        type_names.push_back("TSD_Input<" + std::string(typeid(K).name()) + ">");
    }

    // TSS types
    template<typename T>
    void visit(TimeSeriesSetInput_T<T>& input) {
        type_names.push_back("TSS_Input<" + std::string(typeid(T).name()) + ">");
    }

    // TSW types
    template<typename T>
    void visit(TimeSeriesWindowInput<T>& input) {
        type_names.push_back("TSW_Input<" + std::string(typeid(T).name()) + ">");
    }

    // REF types
    void visit(TimeSeriesReferenceInput& input) {
        type_names.push_back("REF_Input");
    }

    // Signal types
    void visit(TimeSeriesSignalInput& input) {
        type_names.push_back("SIGNAL_Input");
    }
};

/**
 * Test CRTP visitor that counts visited nodes
 */
struct CountingVisitor : TimeSeriesOutputVisitorCRTP<CountingVisitor> {
    int count = 0;

    template<typename T>
    void visit(T& output) {
        count++;
    }
};

TEST_CASE("CRTP Visitor - Basic TS Output", "[visitor][crtp][ts]") {
    auto ts_output = TimeSeriesValueOutput<int>();
    TypeCollectorVisitor visitor;

    ts_output.accept(visitor);

    REQUIRE(visitor.type_names.size() == 1);
    REQUIRE(visitor.type_names[0].find("TS") != std::string::npos);
}

TEST_CASE("CRTP Visitor - Basic TS Input", "[visitor][crtp][ts]") {
    auto ts_input = TimeSeriesValueInput<double>();
    InputTypeCollectorVisitor visitor;

    ts_input.accept(visitor);

    REQUIRE(visitor.type_names.size() == 1);
    REQUIRE(visitor.type_names[0].find("TS_Input") != std::string::npos);
}

TEST_CASE("CRTP Visitor - TSB Output", "[visitor][crtp][tsb]") {
    auto tsb_output = TimeSeriesBundleOutput();
    TypeCollectorVisitor visitor;

    tsb_output.accept(visitor);

    REQUIRE(visitor.type_names.size() == 1);
    REQUIRE(visitor.type_names[0] == "TSB");
}

TEST_CASE("CRTP Visitor - TSL Output", "[visitor][crtp][tsl]") {
    auto tsl_output = TimeSeriesListOutput();
    TypeCollectorVisitor visitor;

    tsl_output.accept(visitor);

    REQUIRE(visitor.type_names.size() == 1);
    REQUIRE(visitor.type_names[0] == "TSL");
}

TEST_CASE("CRTP Visitor - TSD Output", "[visitor][crtp][tsd]") {
    auto tsd_output = TimeSeriesDictOutput_T<int>();
    TypeCollectorVisitor visitor;

    tsd_output.accept(visitor);

    REQUIRE(visitor.type_names.size() == 1);
    REQUIRE(visitor.type_names[0].find("TSD") != std::string::npos);
}

TEST_CASE("CRTP Visitor - TSS Output", "[visitor][crtp][tss]") {
    auto tss_output = TimeSeriesSetOutput_T<int>();
    TypeCollectorVisitor visitor;

    tss_output.accept(visitor);

    REQUIRE(visitor.type_names.size() == 1);
    REQUIRE(visitor.type_names[0].find("TSS") != std::string::npos);
}

TEST_CASE("CRTP Visitor - TSW Fixed Window Output", "[visitor][crtp][tsw]") {
    auto tsw_output = TimeSeriesFixedWindowOutput<int>();
    TypeCollectorVisitor visitor;

    tsw_output.accept(visitor);

    REQUIRE(visitor.type_names.size() == 1);
    REQUIRE(visitor.type_names[0].find("TSW_Fixed") != std::string::npos);
}

TEST_CASE("CRTP Visitor - TSW Time Window Output", "[visitor][crtp][tsw]") {
    auto tsw_output = TimeSeriesTimeWindowOutput<double>();
    TypeCollectorVisitor visitor;

    tsw_output.accept(visitor);

    REQUIRE(visitor.type_names.size() == 1);
    REQUIRE(visitor.type_names[0].find("TSW_Time") != std::string::npos);
}

TEST_CASE("CRTP Visitor - REF Output", "[visitor][crtp][ref]") {
    auto ref_output = TimeSeriesReferenceOutput();
    TypeCollectorVisitor visitor;

    ref_output.accept(visitor);

    REQUIRE(visitor.type_names.size() == 1);
    REQUIRE(visitor.type_names[0] == "REF");
}

TEST_CASE("CRTP Visitor - Signal Input", "[visitor][crtp][signal]") {
    auto signal_input = TimeSeriesSignalInput();
    InputTypeCollectorVisitor visitor;

    signal_input.accept(visitor);

    REQUIRE(visitor.type_names.size() == 1);
    REQUIRE(visitor.type_names[0] == "SIGNAL_Input");
}

TEST_CASE("CRTP Visitor - Counting multiple types", "[visitor][crtp][multiple]") {
    CountingVisitor visitor;

    auto ts_int = TimeSeriesValueOutput<int>();
    auto ts_double = TimeSeriesValueOutput<double>();
    auto tsb = TimeSeriesBundleOutput();

    ts_int.accept(visitor);
    ts_double.accept(visitor);
    tsb.accept(visitor);

    REQUIRE(visitor.count == 3);
}

// ============================================================================
// Acyclic Visitor Tests
// ============================================================================

/**
 * Test acyclic visitor for specific types
 */
struct IntegerTSVisitor : TimeSeriesVisitor,
                          TimeSeriesOutputVisitor<TimeSeriesValueOutput<int>>,
                          TimeSeriesOutputVisitor<TimeSeriesValueOutput<long>> {
    std::vector<std::string> visited;

    void visit(TimeSeriesValueOutput<int>& output) override {
        visited.push_back("int");
    }

    void visit(TimeSeriesValueOutput<long>& output) override {
        visited.push_back("long");
    }
};

/**
 * Test acyclic visitor for bundle types
 */
struct BundleVisitor : TimeSeriesVisitor,
                       TimeSeriesOutputVisitor<TimeSeriesBundleOutput> {
    bool visited = false;

    void visit(TimeSeriesBundleOutput& output) override {
        visited = true;
    }
};

/**
 * Test acyclic visitor for collection types
 */
struct CollectionVisitor : TimeSeriesVisitor,
                           TimeSeriesOutputVisitor<TimeSeriesListOutput>,
                           TimeSeriesOutputVisitor<TimeSeriesDictOutput_T<int>>,
                           TimeSeriesOutputVisitor<TimeSeriesSetOutput_T<int>> {
    std::vector<std::string> visited;

    void visit(TimeSeriesListOutput& output) override {
        visited.push_back("list");
    }

    void visit(TimeSeriesDictOutput_T<int>& output) override {
        visited.push_back("dict");
    }

    void visit(TimeSeriesSetOutput_T<int>& output) override {
        visited.push_back("set");
    }
};

TEST_CASE("Acyclic Visitor - Specific type int", "[visitor][acyclic][ts]") {
    auto ts_int = TimeSeriesValueOutput<int>();
    IntegerTSVisitor visitor;

    ts_int.accept(visitor);

    REQUIRE(visitor.visited.size() == 1);
    REQUIRE(visitor.visited[0] == "int");
}

TEST_CASE("Acyclic Visitor - Specific type long", "[visitor][acyclic][ts]") {
    auto ts_long = TimeSeriesValueOutput<long>();
    IntegerTSVisitor visitor;

    ts_long.accept(visitor);

    REQUIRE(visitor.visited.size() == 1);
    REQUIRE(visitor.visited[0] == "long");
}

TEST_CASE("Acyclic Visitor - Unsupported type ignored", "[visitor][acyclic][ts]") {
    auto ts_double = TimeSeriesValueOutput<double>();
    IntegerTSVisitor visitor;

    // Should not throw, just silently ignore
    REQUIRE_NOTHROW(ts_double.accept(visitor));
    REQUIRE(visitor.visited.empty());
}

TEST_CASE("Acyclic Visitor - Bundle type", "[visitor][acyclic][tsb]") {
    auto tsb = TimeSeriesBundleOutput();
    BundleVisitor visitor;

    tsb.accept(visitor);

    REQUIRE(visitor.visited);
}

TEST_CASE("Acyclic Visitor - Collection types", "[visitor][acyclic][collections]") {
    CollectionVisitor visitor;

    auto tsl = TimeSeriesListOutput();
    auto tsd = TimeSeriesDictOutput_T<int>();
    auto tss = TimeSeriesSetOutput_T<int>();

    tsl.accept(visitor);
    tsd.accept(visitor);
    tss.accept(visitor);

    REQUIRE(visitor.visited.size() == 3);
    REQUIRE(visitor.visited[0] == "list");
    REQUIRE(visitor.visited[1] == "dict");
    REQUIRE(visitor.visited[2] == "set");
}

// ============================================================================
// Const Visitor Tests
// ============================================================================

/**
 * Const CRTP visitor for read-only operations
 */
struct ConstTypeCollector : ConstTimeSeriesOutputVisitorCRTP<ConstTypeCollector> {
    std::vector<std::string> type_names;

    template<typename T>
    void visit(const TimeSeriesValueOutput<T>& output) {
        type_names.push_back("const_TS");
    }

    void visit(const TimeSeriesBundleOutput& output) {
        type_names.push_back("const_TSB");
    }
};

/**
 * Const acyclic visitor
 */
struct ConstIntVisitor : TimeSeriesVisitor,
                         ConstTimeSeriesOutputVisitor<TimeSeriesValueOutput<int>> {
    bool visited = false;

    void visit(const TimeSeriesValueOutput<int>& output) override {
        visited = true;
    }
};

TEST_CASE("Const CRTP Visitor - TS", "[visitor][crtp][const]") {
    const auto ts_int = TimeSeriesValueOutput<int>();
    ConstTypeCollector visitor;

    ts_int.accept(visitor);

    REQUIRE(visitor.type_names.size() == 1);
    REQUIRE(visitor.type_names[0] == "const_TS");
}

TEST_CASE("Const CRTP Visitor - TSB", "[visitor][crtp][const]") {
    const auto tsb = TimeSeriesBundleOutput();
    ConstTypeCollector visitor;

    tsb.accept(visitor);

    REQUIRE(visitor.type_names.size() == 1);
    REQUIRE(visitor.type_names[0] == "const_TSB");
}

TEST_CASE("Const Acyclic Visitor - TS", "[visitor][acyclic][const]") {
    const auto ts_int = TimeSeriesValueOutput<int>();
    ConstIntVisitor visitor;

    ts_int.accept(visitor);

    REQUIRE(visitor.visited);
}

// ============================================================================
// Input Type Visitor Tests
// ============================================================================

struct InputCountingVisitor : TimeSeriesVisitor,
                              TimeSeriesInputVisitor<TimeSeriesValueInput<int>>,
                              TimeSeriesInputVisitor<TimeSeriesBundleInput>,
                              TimeSeriesInputVisitor<TimeSeriesSignalInput> {
    int count = 0;

    void visit(TimeSeriesValueInput<int>& input) override { count++; }
    void visit(TimeSeriesBundleInput& input) override { count++; }
    void visit(TimeSeriesSignalInput& input) override { count++; }
};

TEST_CASE("Acyclic Visitor - Input types", "[visitor][acyclic][input]") {
    InputCountingVisitor visitor;

    auto ts_input = TimeSeriesValueInput<int>();
    auto tsb_input = TimeSeriesBundleInput();
    auto signal_input = TimeSeriesSignalInput();

    ts_input.accept(visitor);
    tsb_input.accept(visitor);
    signal_input.accept(visitor);

    REQUIRE(visitor.count == 3);
}

// ============================================================================
// Polymorphic Visitor Tests (via base class pointers)
// ============================================================================

TEST_CASE("Polymorphic CRTP Visitor via TimeSeriesOutput*", "[visitor][crtp][polymorphic]") {
    TypeCollectorVisitor visitor;

    TimeSeriesOutput* ts_base = new TimeSeriesValueOutput<int>();
    TimeSeriesOutput* tsb_base = new TimeSeriesBundleOutput();

    ts_base->accept(visitor);
    tsb_base->accept(visitor);

    REQUIRE(visitor.type_names.size() == 2);

    delete ts_base;
    delete tsb_base;
}

TEST_CASE("Polymorphic Acyclic Visitor via TimeSeriesOutput*", "[visitor][acyclic][polymorphic]") {
    IntegerTSVisitor visitor;

    TimeSeriesOutput* ts_int = new TimeSeriesValueOutput<int>();
    TimeSeriesOutput* ts_double = new TimeSeriesValueOutput<double>();

    ts_int->accept(visitor);
    ts_double->accept(visitor);  // Should be ignored

    REQUIRE(visitor.visited.size() == 1);
    REQUIRE(visitor.visited[0] == "int");

    delete ts_int;
    delete ts_double;
}

// ============================================================================
// Mixed Pattern Tests
// ============================================================================

/**
 * Visitor that uses both CRTP and Acyclic patterns
 */
struct MixedPatternVisitor : TimeSeriesOutputVisitorCRTP<MixedPatternVisitor>,
                             TimeSeriesVisitor,
                             TimeSeriesOutputVisitor<TimeSeriesBundleOutput> {
    std::vector<std::string> operations;

    // CRTP path - for generic handling
    template<typename T>
    void visit(T& output) {
        operations.push_back("crtp_generic");
    }

    // Acyclic path - for specific handling
    void visit(TimeSeriesBundleOutput& output) override {
        operations.push_back("acyclic_specific");
    }
};

TEST_CASE("Mixed Pattern - CRTP generic", "[visitor][mixed]") {
    MixedPatternVisitor visitor;
    auto ts_int = TimeSeriesValueOutput<int>();

    // Should use CRTP path
    ts_int.accept(visitor);

    REQUIRE(visitor.operations.size() == 1);
    REQUIRE(visitor.operations[0] == "crtp_generic");
}

TEST_CASE("Mixed Pattern - Acyclic specific", "[visitor][mixed]") {
    MixedPatternVisitor visitor;
    auto tsb = TimeSeriesBundleOutput();

    // Acyclic visitor needs explicit cast
    TimeSeriesVisitor& acyclic_visitor = visitor;
    tsb.accept(acyclic_visitor);

    REQUIRE(visitor.operations.size() == 1);
    REQUIRE(visitor.operations[0] == "acyclic_specific");
}

// ============================================================================
// Edge Cases and Error Handling
// ============================================================================

/**
 * Empty visitor that doesn't implement any visit methods
 */
struct EmptyVisitor : TimeSeriesOutputVisitorCRTP<EmptyVisitor> {
    // No visit methods implemented - should still compile
};

/**
 * Selective visitor that only handles some types
 */
struct SelectiveAcyclicVisitor : TimeSeriesVisitor,
                                 TimeSeriesOutputVisitor<TimeSeriesBundleOutput> {
    bool visited = false;

    void visit(TimeSeriesBundleOutput& output) override {
        visited = true;
    }
};

TEST_CASE("Edge Case - Visitor with no implementations", "[visitor][edge]") {
    EmptyVisitor visitor;
    auto ts_int = TimeSeriesValueOutput<int>();

    // Should compile but do nothing
    REQUIRE_NOTHROW(ts_int.accept(visitor));
}

TEST_CASE("Edge Case - Selective visitor ignores unsupported types", "[visitor][edge]") {
    SelectiveAcyclicVisitor visitor;

    auto ts_int = TimeSeriesValueOutput<int>();
    auto tsb = TimeSeriesBundleOutput();

    ts_int.accept(visitor);  // Ignored
    REQUIRE_FALSE(visitor.visited);

    tsb.accept(visitor);     // Handled
    REQUIRE(visitor.visited);
}

// ============================================================================
// Template Instantiation Tests
// ============================================================================

TEST_CASE("Template Types - Multiple TSD instantiations", "[visitor][templates][tsd]") {
    TypeCollectorVisitor visitor;

    auto tsd_int = TimeSeriesDictOutput_T<int>();
    auto tsd_bool = TimeSeriesDictOutput_T<bool>();
    auto tsd_double = TimeSeriesDictOutput_T<double>();

    tsd_int.accept(visitor);
    tsd_bool.accept(visitor);
    tsd_double.accept(visitor);

    REQUIRE(visitor.type_names.size() == 3);
    // All should be TSD types
    for (const auto& name : visitor.type_names) {
        REQUIRE(name.find("TSD") != std::string::npos);
    }
}

TEST_CASE("Template Types - Multiple TSS instantiations", "[visitor][templates][tss]") {
    TypeCollectorVisitor visitor;

    auto tss_int = TimeSeriesSetOutput_T<int>();
    auto tss_long = TimeSeriesSetOutput_T<long>();

    tss_int.accept(visitor);
    tss_long.accept(visitor);

    REQUIRE(visitor.type_names.size() == 2);
    for (const auto& name : visitor.type_names) {
        REQUIRE(name.find("TSS") != std::string::npos);
    }
}

TEST_CASE("Template Types - Multiple TSW instantiations", "[visitor][templates][tsw]") {
    TypeCollectorVisitor visitor;

    auto tsw_fixed_int = TimeSeriesFixedWindowOutput<int>();
    auto tsw_time_double = TimeSeriesTimeWindowOutput<double>();

    tsw_fixed_int.accept(visitor);
    tsw_time_double.accept(visitor);

    REQUIRE(visitor.type_names.size() == 2);
    REQUIRE(visitor.type_names[0].find("TSW_Fixed") != std::string::npos);
    REQUIRE(visitor.type_names[1].find("TSW_Time") != std::string::npos);
}
