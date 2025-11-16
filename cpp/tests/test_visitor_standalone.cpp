/**
 * Standalone Unit tests for the Time Series Visitor Pattern
 *
 * This test file uses minimal mock types to test the visitor pattern infrastructure
 * without requiring the full hgraph runtime dependencies (nanobind, Python, etc.)
 */

#include <catch2/catch_test_macros.hpp>
#include <vector>
#include <string>
#include <typeinfo>
#include <type_traits>

// ============================================================================
// Mock Visitor Infrastructure (mirroring time_series_visitor.h)
// ============================================================================

// CRTP Base for Output Visitors
template<typename Derived>
struct TimeSeriesOutputVisitorCRTP {
    template<typename TS>
    decltype(auto) operator()(TS& ts) {
        return static_cast<Derived*>(this)->visit(ts);
    }
};

// CRTP Base for Const Output Visitors
template<typename Derived>
struct ConstTimeSeriesOutputVisitorCRTP {
    template<typename TS>
    decltype(auto) operator()(const TS& ts) const {
        return static_cast<const Derived*>(this)->visit(ts);
    }
};

// Acyclic Visitor Base
struct TimeSeriesVisitor {
    virtual ~TimeSeriesVisitor() = default;
};

// Typed Acyclic Visitor Interface
template<typename T>
struct TimeSeriesOutputVisitor {
    virtual void visit(T& output) = 0;
    virtual ~TimeSeriesOutputVisitor() = default;
};

// Const variant
template<typename T>
struct ConstTimeSeriesOutputVisitor {
    virtual void visit(const T& output) = 0;
    virtual ~ConstTimeSeriesOutputVisitor() = default;
};

// ============================================================================
// Mock Time Series Types
// ============================================================================

// Base Output interface with visitable support
struct MockTimeSeriesOutput {
    virtual ~MockTimeSeriesOutput() = default;

    // Acyclic visitor support (runtime dispatch)
    virtual void accept(TimeSeriesVisitor& visitor) = 0;
    virtual void accept(TimeSeriesVisitor& visitor) const = 0;
};

// Mock TS (scalar value time series)
template<typename T>
struct MockTS : MockTimeSeriesOutput {
    T value{};

    // Acyclic visitor support
    void accept(TimeSeriesVisitor& visitor) override {
        if (auto* typed_visitor = dynamic_cast<TimeSeriesOutputVisitor<MockTS<T>>*>(&visitor)) {
            typed_visitor->visit(*this);
        }
    }

    void accept(TimeSeriesVisitor& visitor) const override {
        if (auto* typed_visitor = dynamic_cast<ConstTimeSeriesOutputVisitor<MockTS<T>>*>(&visitor)) {
            typed_visitor->visit(*this);
        }
    }

    // CRTP visitor support
    template<typename Visitor>
        requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)
    decltype(auto) accept(Visitor& visitor) {
        return visitor(*this);
    }

    template<typename Visitor>
        requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)
    decltype(auto) accept(Visitor& visitor) const {
        return visitor(*this);
    }
};

// Mock TSB (bundle time series)
struct MockTSB : MockTimeSeriesOutput {
    // Acyclic visitor support
    void accept(TimeSeriesVisitor& visitor) override {
        if (auto* typed_visitor = dynamic_cast<TimeSeriesOutputVisitor<MockTSB>*>(&visitor)) {
            typed_visitor->visit(*this);
        }
    }

    void accept(TimeSeriesVisitor& visitor) const override {
        if (auto* typed_visitor = dynamic_cast<ConstTimeSeriesOutputVisitor<MockTSB>*>(&visitor)) {
            typed_visitor->visit(*this);
        }
    }

    // CRTP visitor support
    template<typename Visitor>
        requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)
    decltype(auto) accept(Visitor& visitor) {
        return visitor(*this);
    }

    template<typename Visitor>
        requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)
    decltype(auto) accept(Visitor& visitor) const {
        return visitor(*this);
    }
};

// Mock TSL (list time series)
struct MockTSL : MockTimeSeriesOutput {
    // Acyclic visitor support
    void accept(TimeSeriesVisitor& visitor) override {
        if (auto* typed_visitor = dynamic_cast<TimeSeriesOutputVisitor<MockTSL>*>(&visitor)) {
            typed_visitor->visit(*this);
        }
    }

    void accept(TimeSeriesVisitor& visitor) const override {
        if (auto* typed_visitor = dynamic_cast<ConstTimeSeriesOutputVisitor<MockTSL>*>(&visitor)) {
            typed_visitor->visit(*this);
        }
    }

    // CRTP visitor support
    template<typename Visitor>
        requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)
    decltype(auto) accept(Visitor& visitor) {
        return visitor(*this);
    }

    template<typename Visitor>
        requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)
    decltype(auto) accept(Visitor& visitor) const {
        return visitor(*this);
    }
};

// Mock TSD (dict time series) - template
template<typename K>
struct MockTSD : MockTimeSeriesOutput {
    // Acyclic visitor support
    void accept(TimeSeriesVisitor& visitor) override {
        if (auto* typed_visitor = dynamic_cast<TimeSeriesOutputVisitor<MockTSD<K>>*>(&visitor)) {
            typed_visitor->visit(*this);
        }
    }

    void accept(TimeSeriesVisitor& visitor) const override {
        if (auto* typed_visitor = dynamic_cast<ConstTimeSeriesOutputVisitor<MockTSD<K>>*>(&visitor)) {
            typed_visitor->visit(*this);
        }
    }

    // CRTP visitor support
    template<typename Visitor>
        requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)
    decltype(auto) accept(Visitor& visitor) {
        return visitor(*this);
    }

    template<typename Visitor>
        requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)
    decltype(auto) accept(Visitor& visitor) const {
        return visitor(*this);
    }
};

// Mock TSS (set time series) - template
template<typename T>
struct MockTSS : MockTimeSeriesOutput {
    // Acyclic visitor support
    void accept(TimeSeriesVisitor& visitor) override {
        if (auto* typed_visitor = dynamic_cast<TimeSeriesOutputVisitor<MockTSS<T>>*>(&visitor)) {
            typed_visitor->visit(*this);
        }
    }

    void accept(TimeSeriesVisitor& visitor) const override {
        if (auto* typed_visitor = dynamic_cast<ConstTimeSeriesOutputVisitor<MockTSS<T>>*>(&visitor)) {
            typed_visitor->visit(*this);
        }
    }

    // CRTP visitor support
    template<typename Visitor>
        requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)
    decltype(auto) accept(Visitor& visitor) {
        return visitor(*this);
    }

    template<typename Visitor>
        requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)
    decltype(auto) accept(Visitor& visitor) const {
        return visitor(*this);
    }
};

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
    void visit(MockTS<T>& output) {
        type_names.push_back("TS<" + std::string(typeid(T).name()) + ">");
    }

    // TSB type
    void visit(MockTSB& output) {
        type_names.push_back("TSB");
    }

    // TSL type
    void visit(MockTSL& output) {
        type_names.push_back("TSL");
    }

    // TSD types
    template<typename K>
    void visit(MockTSD<K>& output) {
        type_names.push_back("TSD<" + std::string(typeid(K).name()) + ">");
    }

    // TSS types
    template<typename T>
    void visit(MockTSS<T>& output) {
        type_names.push_back("TSS<" + std::string(typeid(T).name()) + ">");
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

TEST_CASE("CRTP Visitor - Basic TS int", "[visitor][crtp][ts]") {
    auto ts_int = MockTS<int>();
    TypeCollectorVisitor visitor;

    ts_int.accept(visitor);

    REQUIRE(visitor.type_names.size() == 1);
    REQUIRE(visitor.type_names[0].find("TS") != std::string::npos);
}

TEST_CASE("CRTP Visitor - Basic TS double", "[visitor][crtp][ts]") {
    auto ts_double = MockTS<double>();
    TypeCollectorVisitor visitor;

    ts_double.accept(visitor);

    REQUIRE(visitor.type_names.size() == 1);
    REQUIRE(visitor.type_names[0].find("TS") != std::string::npos);
}

TEST_CASE("CRTP Visitor - TSB", "[visitor][crtp][tsb]") {
    auto tsb = MockTSB();
    TypeCollectorVisitor visitor;

    tsb.accept(visitor);

    REQUIRE(visitor.type_names.size() == 1);
    REQUIRE(visitor.type_names[0] == "TSB");
}

TEST_CASE("CRTP Visitor - TSL", "[visitor][crtp][tsl]") {
    auto tsl = MockTSL();
    TypeCollectorVisitor visitor;

    tsl.accept(visitor);

    REQUIRE(visitor.type_names.size() == 1);
    REQUIRE(visitor.type_names[0] == "TSL");
}

TEST_CASE("CRTP Visitor - TSD template", "[visitor][crtp][tsd]") {
    auto tsd_int = MockTSD<int>();
    auto tsd_bool = MockTSD<bool>();
    TypeCollectorVisitor visitor;

    tsd_int.accept(visitor);
    tsd_bool.accept(visitor);

    REQUIRE(visitor.type_names.size() == 2);
    REQUIRE(visitor.type_names[0].find("TSD") != std::string::npos);
    REQUIRE(visitor.type_names[1].find("TSD") != std::string::npos);
}

TEST_CASE("CRTP Visitor - TSS template", "[visitor][crtp][tss]") {
    auto tss_int = MockTSS<int>();
    auto tss_long = MockTSS<long>();
    TypeCollectorVisitor visitor;

    tss_int.accept(visitor);
    tss_long.accept(visitor);

    REQUIRE(visitor.type_names.size() == 2);
    REQUIRE(visitor.type_names[0].find("TSS") != std::string::npos);
    REQUIRE(visitor.type_names[1].find("TSS") != std::string::npos);
}

TEST_CASE("CRTP Visitor - Counting multiple types", "[visitor][crtp][counting]") {
    CountingVisitor visitor;

    auto ts_int = MockTS<int>();
    auto ts_double = MockTS<double>();
    auto tsb = MockTSB();
    auto tsl = MockTSL();

    ts_int.accept(visitor);
    ts_double.accept(visitor);
    tsb.accept(visitor);
    tsl.accept(visitor);

    REQUIRE(visitor.count == 4);
}

// ============================================================================
// Acyclic Visitor Tests
// ============================================================================

/**
 * Test acyclic visitor for specific types
 */
struct IntegerTSVisitor : TimeSeriesVisitor,
                          TimeSeriesOutputVisitor<MockTS<int>>,
                          TimeSeriesOutputVisitor<MockTS<long>> {
    std::vector<std::string> visited;

    void visit(MockTS<int>& output) override {
        visited.push_back("int");
    }

    void visit(MockTS<long>& output) override {
        visited.push_back("long");
    }
};

/**
 * Test acyclic visitor for bundle type
 */
struct BundleVisitor : TimeSeriesVisitor,
                       TimeSeriesOutputVisitor<MockTSB> {
    bool visited = false;

    void visit(MockTSB& output) override {
        visited = true;
    }
};

/**
 * Test acyclic visitor for collection types
 */
struct CollectionVisitor : TimeSeriesVisitor,
                           TimeSeriesOutputVisitor<MockTSL>,
                           TimeSeriesOutputVisitor<MockTSD<int>>,
                           TimeSeriesOutputVisitor<MockTSS<int>> {
    std::vector<std::string> visited;

    void visit(MockTSL& output) override {
        visited.push_back("list");
    }

    void visit(MockTSD<int>& output) override {
        visited.push_back("dict");
    }

    void visit(MockTSS<int>& output) override {
        visited.push_back("set");
    }
};

TEST_CASE("Acyclic Visitor - Specific type int", "[visitor][acyclic][ts]") {
    auto ts_int = MockTS<int>();
    IntegerTSVisitor visitor;

    ts_int.accept(visitor);

    REQUIRE(visitor.visited.size() == 1);
    REQUIRE(visitor.visited[0] == "int");
}

TEST_CASE("Acyclic Visitor - Specific type long", "[visitor][acyclic][ts]") {
    auto ts_long = MockTS<long>();
    IntegerTSVisitor visitor;

    ts_long.accept(visitor);

    REQUIRE(visitor.visited.size() == 1);
    REQUIRE(visitor.visited[0] == "long");
}

TEST_CASE("Acyclic Visitor - Unsupported type ignored", "[visitor][acyclic][ts]") {
    auto ts_double = MockTS<double>();
    IntegerTSVisitor visitor;

    // Should not throw, just silently ignore
    REQUIRE_NOTHROW(ts_double.accept(visitor));
    REQUIRE(visitor.visited.empty());
}

TEST_CASE("Acyclic Visitor - Bundle type", "[visitor][acyclic][tsb]") {
    auto tsb = MockTSB();
    BundleVisitor visitor;

    tsb.accept(visitor);

    REQUIRE(visitor.visited);
}

TEST_CASE("Acyclic Visitor - Collection types", "[visitor][acyclic][collections]") {
    CollectionVisitor visitor;

    auto tsl = MockTSL();
    auto tsd = MockTSD<int>();
    auto tss = MockTSS<int>();

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
    mutable std::vector<std::string> type_names;

    template<typename T>
    void visit(const MockTS<T>& output) const {
        type_names.push_back("const_TS");
    }

    void visit(const MockTSB& output) const {
        type_names.push_back("const_TSB");
    }
};

/**
 * Const acyclic visitor
 */
struct ConstIntVisitor : TimeSeriesVisitor,
                         ConstTimeSeriesOutputVisitor<MockTS<int>> {
    bool visited = false;

    void visit(const MockTS<int>& output) override {
        visited = true;
    }
};

TEST_CASE("Const CRTP Visitor - TS", "[visitor][crtp][const]") {
    const auto ts_int = MockTS<int>();
    ConstTypeCollector visitor;

    ts_int.accept(visitor);

    REQUIRE(visitor.type_names.size() == 1);
    REQUIRE(visitor.type_names[0] == "const_TS");
}

TEST_CASE("Const CRTP Visitor - TSB", "[visitor][crtp][const]") {
    const auto tsb = MockTSB();
    ConstTypeCollector visitor;

    tsb.accept(visitor);

    REQUIRE(visitor.type_names.size() == 1);
    REQUIRE(visitor.type_names[0] == "const_TSB");
}

TEST_CASE("Const Acyclic Visitor - TS", "[visitor][acyclic][const]") {
    const auto ts_int = MockTS<int>();
    ConstIntVisitor visitor;

    ts_int.accept(visitor);

    REQUIRE(visitor.visited);
}

// ============================================================================
// Polymorphic Visitor Tests (via base class pointers)
// ============================================================================

TEST_CASE("Polymorphic CRTP Visitor via concrete types", "[visitor][crtp][polymorphic]") {
    // Note: CRTP visitors work through static dispatch, so we use concrete types
    TypeCollectorVisitor visitor;

    MockTS<int> ts_int;
    MockTSB tsb;

    ts_int.accept(visitor);
    tsb.accept(visitor);

    REQUIRE(visitor.type_names.size() == 2);
}

TEST_CASE("Polymorphic Acyclic Visitor via base pointer", "[visitor][acyclic][polymorphic]") {
    IntegerTSVisitor visitor;

    MockTimeSeriesOutput* ts_int = new MockTS<int>();
    MockTimeSeriesOutput* ts_double = new MockTS<double>();

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
 * Pure CRTP visitor for generic handling
 */
struct PureCRTPVisitor : TimeSeriesOutputVisitorCRTP<PureCRTPVisitor> {
    std::vector<std::string> operations;

    template<typename T>
    void visit(T& output) {
        operations.push_back("crtp_generic");
    }
};

/**
 * Visitor that demonstrates switching between patterns
 * Note: A single visitor can't use both CRTP and Acyclic simultaneously
 * due to the requires clause. Instead, use separate visitors for each pattern.
 */
TEST_CASE("Mixed Pattern - CRTP for generic operations", "[visitor][mixed]") {
    PureCRTPVisitor visitor;
    auto ts_int = MockTS<int>();
    auto tsb = MockTSB();

    // CRTP path handles all types generically
    ts_int.accept(visitor);
    tsb.accept(visitor);

    REQUIRE(visitor.operations.size() == 2);
    REQUIRE(visitor.operations[0] == "crtp_generic");
    REQUIRE(visitor.operations[1] == "crtp_generic");
}

TEST_CASE("Mixed Pattern - Acyclic for specific types", "[visitor][mixed]") {
    BundleVisitor visitor;  // Acyclic visitor
    auto tsb = MockTSB();

    // Acyclic visitor handles specific types
    tsb.accept(visitor);

    REQUIRE(visitor.visited);
}

// ============================================================================
// Edge Cases and Error Handling
// ============================================================================

/**
 * Selective visitor that only handles some types
 */
struct SelectiveAcyclicVisitor : TimeSeriesVisitor,
                                 TimeSeriesOutputVisitor<MockTSB> {
    bool visited = false;

    void visit(MockTSB& output) override {
        visited = true;
    }
};

TEST_CASE("Edge Case - Selective visitor ignores unsupported types", "[visitor][edge]") {
    SelectiveAcyclicVisitor visitor;

    auto ts_int = MockTS<int>();
    auto tsb = MockTSB();

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

    auto tsd_int = MockTSD<int>();
    auto tsd_bool = MockTSD<bool>();
    auto tsd_double = MockTSD<double>();

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

    auto tss_int = MockTSS<int>();
    auto tss_long = MockTSS<long>();

    tss_int.accept(visitor);
    tss_long.accept(visitor);

    REQUIRE(visitor.type_names.size() == 2);
    for (const auto& name : visitor.type_names) {
        REQUIRE(name.find("TSS") != std::string::npos);
    }
}

// ============================================================================
// Dispatch Mechanism Tests
// ============================================================================

TEST_CASE("Dispatch - CRTP selected over Acyclic when no constraint", "[visitor][dispatch]") {
    // This test verifies that the requires clause correctly distinguishes
    // between CRTP and Acyclic visitors

    TypeCollectorVisitor crtp_visitor;
    auto ts = MockTS<int>();

    // CRTP path should be taken (constraint excludes TimeSeriesVisitor base)
    ts.accept(crtp_visitor);

    REQUIRE(crtp_visitor.type_names.size() == 1);
}

TEST_CASE("Dispatch - Acyclic selected when visitor derives from TimeSeriesVisitor", "[visitor][dispatch]") {
    IntegerTSVisitor acyclic_visitor;
    auto ts = MockTS<int>();

    // Acyclic path should be taken
    ts.accept(acyclic_visitor);

    REQUIRE(acyclic_visitor.visited.size() == 1);
    REQUIRE(acyclic_visitor.visited[0] == "int");
}
