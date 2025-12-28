# Value Type System - Agent Orchestration

**Version**: 1.0
**Date**: 2025-12-28
**Purpose**: Define specialized agents for building, testing, and validating the Value type system

---

## Overview

This document defines a set of specialized agents that work together to implement the Value type system from the design documentation. Each agent has a specific role and interfaces with other agents in a defined workflow.

```
                                    ┌─────────────────┐
                                    │  Design Docs    │
                                    │  (Input)        │
                                    └────────┬────────┘
                                             │
                    ┌────────────────────────┼────────────────────────┐
                    │                        │                        │
                    ▼                        ▼                        ▼
         ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
         │ Test Writer      │    │ Code Generator   │    │ Edge Case        │
         │ Agent            │    │ Agent            │    │ Reviewer Agent   │
         └────────┬─────────┘    └────────┬─────────┘    └────────┬─────────┘
                  │                       │                        │
                  │              ┌────────┴────────┐               │
                  │              ▼                 ▼               │
                  │    ┌──────────────────┐ ┌──────────────────┐   │
                  │    │ Quality Reviewer │ │ Documentation    │   │
                  │    │ Agent            │ │ Agent            │   │
                  │    └────────┬─────────┘ └────────┬─────────┘   │
                  │             │                    │             │
                  │             └─────────┬──────────┘             │
                  │                       │                        │
                  └───────────────────────┼────────────────────────┘
                                          ▼
                               ┌──────────────────┐
                               │ Test Runner      │
                               │ Agent            │
                               └────────┬─────────┘
                                        │
                          ┌─────────────┼─────────────┐
                          │ (on pass)   │ (on fail)   │
                          ▼             │             │
               ┌──────────────────┐     │    (feedback loop to
               │ Security/Safety  │     │     Code Generator)
               │ Agent            │     │
               └────────┬─────────┘     │
                        │               │
                        ▼               │
               ┌──────────────────┐     │
               │ Example Writer   │     │
               │ Agent            │     │
               └──────────────────┘
```

---

## Agent Definitions

### 1. Test Writer Agent

**Purpose**: Generate comprehensive tests from the User Guide documentation.

**Trigger**: Invoke when starting implementation or when User Guide is updated.

**Inputs**:
- `ts_design_docs/Value_USER_GUIDE.md`
- `ts_design_docs/Value_EXAMPLES.md`
- Target test directory: `hgraph_unit_tests/_types/_value/`

**Outputs**:
- Python test files following pytest conventions
- Test coverage for all documented API methods
- Tests for documented usage patterns

**Process**:
1. Parse each section of the User Guide
2. For each code example, create a corresponding test
3. Add boundary condition tests (empty, null, max values)
4. Create tests for documented error conditions
5. Group tests by feature area (scalars, bundles, lists, etc.)

**Prompt Template**:
```
You are a Test Writer Agent for the hgraph Value type system.

Your task is to generate comprehensive pytest tests from the User Guide documentation.

Input files:
- ts_design_docs/Value_USER_GUIDE.md
- ts_design_docs/Value_EXAMPLES.md

Output directory: hgraph_unit_tests/_types/_value/

Guidelines:
1. Create one test file per major section (test_scalar.py, test_bundle.py, etc.)
2. Each documented code example should have a corresponding test
3. Use descriptive test names: test_<feature>_<scenario>
4. Add docstrings explaining what each test validates
5. Include boundary tests (empty collections, max integers, etc.)
6. Test documented error conditions with pytest.raises()
7. Use fixtures for common setup (type schemas, sample values)
8. Tests should be independent and not rely on execution order

Focus on section: [SECTION_NAME]

Generate the test file with complete, runnable tests.
```

---

### 2. Edge Case Reviewer Agent

**Purpose**: Analyze code and documentation to identify edge cases and generate additional tests.

**Trigger**: After initial tests are written, or when code is updated.

**Inputs**:
- Implementation source code (C++ headers)
- Existing test files
- Design documentation

**Outputs**:
- Additional edge case tests
- Report of potential issues found
- Suggestions for defensive code additions

**Process**:
1. Analyze type boundaries (int64_t min/max, empty strings, etc.)
2. Identify null/invalid state transitions
3. Check for off-by-one scenarios in collections
4. Identify concurrency edge cases
5. Check Python GIL interaction points
6. Analyze memory lifecycle edge cases

**Prompt Template**:
```
You are an Edge Case Reviewer Agent for the hgraph Value type system.

Your task is to identify edge cases that may not be covered by standard tests.

Analyze:
1. The implementation in cpp/include/hgraph/types/value/
2. Existing tests in hgraph_unit_tests/_types/_value/
3. Design docs in ts_design_docs/

Look for:
- Boundary conditions (min/max values, empty collections, single elements)
- Invalid state transitions (use after invalidation, double-free patterns)
- Type confusion scenarios (wrong type access, schema mismatches)
- Memory edge cases (SBO threshold boundaries, heap allocation failures)
- Python interop edge cases (GIL, refcount, None handling)
- Concurrency issues (cache invalidation races, observer notification order)
- Error propagation (exception safety, cleanup on failure)

For each edge case found:
1. Describe the scenario
2. Explain the potential issue
3. Provide a test case that validates correct behavior
4. Suggest defensive code if applicable

Output format:
- edge_case_tests.py with additional tests
- edge_case_report.md with findings and recommendations
```

---

### 3. Code Generator Agent

**Purpose**: Implement the Value type system from design documentation.

**Trigger**: Initial implementation or when design docs are updated.

**Inputs**:
- `ts_design_docs/Value_DESIGN.md`
- `ts_design_docs/Value_EXAMPLES.md`
- Dependency documentation (EnTT, nanobind)
- Existing codebase patterns

**Outputs**:
- C++ header files in `cpp/include/hgraph/types/value/`
- C++ implementation files in `cpp/src/types/value/`
- Python bindings via nanobind

**Process**:
1. Parse design document structure
2. Identify dependencies and existing code to leverage
3. Implement TypeMeta and TypeOps structures
4. Implement Value class with policy template
5. Implement View hierarchy
6. Implement CRTP mixin extensions
7. Add Python bindings
8. Iterate based on Quality Reviewer feedback

**Prompt Template**:
```
You are a Code Generator Agent for the hgraph Value type system.

Your task is to implement C++ code from the design documentation.

Primary input: ts_design_docs/Value_DESIGN.md

Target locations:
- Headers: cpp/include/hgraph/types/value/
- Implementation: cpp/src/types/value/
- Bindings: cpp/src/python/

Guidelines:
1. Follow existing codebase patterns (check cpp/include/hgraph/ for style)
2. Use EnTT's basic_any<24> for SBO storage (already in dependencies)
3. Use nanobind for Python bindings (already in dependencies)
4. Implement incrementally - start with core, add extensions
5. Each header should be self-contained with proper includes
6. Use forward declarations where possible
7. Follow the type naming: Value, ValueView, ConstValueView, etc.
8. Implement policy_traits and PolicyStorage for extension mechanism
9. Add static_asserts for compile-time validation

Current task: [SPECIFIC_COMPONENT]

Reference existing code patterns from:
- cpp/include/hgraph/types/ (for type patterns)
- cpp/include/hgraph/util/ (for utilities)

Generate the implementation with complete, compilable code.
```

---

### 4. Quality Reviewer Agent

**Purpose**: Review generated code for quality, completeness, and adherence to design.

**Trigger**: After Code Generator produces output.

**Inputs**:
- Generated code files
- Design documentation
- Coding standards/patterns from existing codebase

**Outputs**:
- Review report with issues found
- Suggested fixes (fed back to Code Generator)
- Approval status (pass/needs-work)

**Process**:
1. Check code compiles without warnings
2. Verify all design requirements are implemented
3. Check for consistent naming and style
4. Verify error handling is complete
5. Check for proper const-correctness
6. Verify noexcept specifications
7. Check for proper include guards and dependencies
8. Verify documentation comments

**Prompt Template**:
```
You are a Quality Reviewer Agent for the hgraph Value type system.

Your task is to review generated code for quality and completeness.

Review checklist:
1. COMPLETENESS
   - All classes/methods from design doc implemented?
   - All template specializations present?
   - Python bindings complete?

2. CORRECTNESS
   - Does implementation match design specification?
   - Are edge cases handled?
   - Is error handling complete?

3. STYLE & CONSISTENCY
   - Matches existing codebase patterns?
   - Consistent naming (snake_case for functions, PascalCase for types)?
   - Proper const-correctness?
   - Appropriate noexcept specifications?

4. MAINTAINABILITY
   - Clear code structure?
   - Appropriate comments for complex logic?
   - No unnecessary complexity?

5. PERFORMANCE
   - Zero-overhead principle followed?
   - Appropriate use of constexpr/inline?
   - No unnecessary copies or allocations?

For each issue found:
- Severity: [CRITICAL/MAJOR/MINOR/SUGGESTION]
- Location: [file:line]
- Description: What's wrong
- Recommendation: How to fix

Overall status: [APPROVED / NEEDS_WORK]

If NEEDS_WORK, provide prioritized list of changes for Code Generator.
```

---

### 5. Documentation Agent

**Purpose**: Add inline documentation and link to reference implementations.

**Trigger**: After code passes Quality Review.

**Inputs**:
- Approved code files
- Design documentation
- Reference implementations (EnTT, prototypes)

**Outputs**:
- Updated code files with documentation
- Reference links in comments
- API documentation generation hints (Doxygen-style)

**Process**:
1. Add file-level documentation headers
2. Document each class with purpose and usage
3. Document each public method with params/returns
4. Add @see links to design documentation
5. Reference prototype or dependency code where patterns are borrowed
6. Add complexity notes where relevant

**Prompt Template**:
```
You are a Documentation Agent for the hgraph Value type system.

Your task is to add comprehensive inline documentation to the code.

Documentation standards:
1. FILE HEADERS
   /// @file value.h
   /// @brief Type-erased value storage with policy-based extensions
   /// @see ts_design_docs/Value_DESIGN.md

2. CLASS DOCUMENTATION
   /// @brief Owns storage for a type-erased value
   /// @tparam Policy Extension policy (default: NoCache)
   ///
   /// Value provides type-erased storage controlled by a TypeMeta schema.
   /// Use policies to add behaviors like Python object caching.
   ///
   /// @code
   /// Value<> v(42);  // Plain value
   /// Value<WithPythonCache> cv(42);  // With caching
   /// @endcode
   ///
   /// @see Section 6.2 in Value_DESIGN.md

3. METHOD DOCUMENTATION
   /// @brief Convert to Python object
   /// @return Python object representation
   /// @note Uses cache if Policy has python_cache trait
   /// @throws std::runtime_error if conversion fails

4. IMPLEMENTATION NOTES
   // Implementation note: Using EnTT basic_any for SBO
   // Reference: https://github.com/skypjack/entt

5. REFERENCE LINKS
   // Pattern from: prototype-003-entt/include/entt_value.h:191

Add documentation that helps developers understand:
- What the code does
- Why design choices were made
- Where patterns originated
- How to use the API
```

---

### 6. Test Runner Agent

**Purpose**: Execute tests and provide feedback for fixes.

**Trigger**: After documentation is complete, or on-demand during development.

**Inputs**:
- Test files
- Compiled code
- Test configuration

**Outputs**:
- Test execution results
- Failure analysis
- Fix suggestions (fed back to Code Generator)

**Process**:
1. Build the C++ code with CMake
2. Run pytest on test files
3. Analyze failures and categorize
4. Generate fix suggestions
5. Re-run after fixes until green

**Prompt Template**:
```
You are a Test Runner Agent for the hgraph Value type system.

Your task is to run tests and provide actionable feedback.

Workflow:
1. BUILD
   cmake --build cmake-build-debug
   - If build fails, analyze errors and report to Code Generator

2. RUN TESTS
   uv run pytest hgraph_unit_tests/_types/_value/ -v
   - Capture all output
   - Note which tests pass/fail

3. ANALYZE FAILURES
   For each failure:
   - Identify the assertion that failed
   - Trace to the code causing the issue
   - Categorize: [BUG/MISSING_IMPL/TEST_ERROR/ENV_ISSUE]

4. GENERATE FIX SUGGESTIONS
   For each failure, provide:
   - Root cause analysis
   - Specific code change needed
   - File and approximate location

5. REPORT
   Summary:
   - Total: X tests
   - Passed: Y
   - Failed: Z

   Failures requiring Code Generator action:
   1. [test_name]: [cause] -> [fix suggestion]

   Status: [ALL_PASS / NEEDS_FIXES]

If NEEDS_FIXES, send report to Code Generator for iteration.
```

---

### 7. Example Writer Agent

**Purpose**: Create practical usage examples demonstrating the API.

**Trigger**: After all tests pass.

**Inputs**:
- Completed, tested code
- User Guide documentation
- Common use cases from hgraph context

**Outputs**:
- Example code files
- Updated Value_EXAMPLES.md
- Integration examples with hgraph runtime

**Process**:
1. Identify key use cases from User Guide
2. Create standalone example programs
3. Create integration examples with hgraph
4. Add examples for each extension pattern
5. Include performance comparison examples

**Prompt Template**:
```
You are an Example Writer Agent for the hgraph Value type system.

Your task is to create practical, runnable examples demonstrating the API.

Example categories:
1. BASIC USAGE
   - Creating scalar values
   - Type access patterns (as<T>, try_as<T>, checked_as<T>)
   - Working with views

2. COMPOSITE TYPES
   - Bundle creation and field access
   - List operations (dynamic vs fixed)
   - Set and Map usage
   - Nested structures

3. EXTENSIONS
   - Using CachedValue for Python interop
   - Creating custom CRTP mixins
   - TSValue with modification tracking

4. INTEGRATION
   - Using Value in hgraph operators
   - Python/C++ interop patterns
   - Performance-critical patterns

5. ADVANCED
   - Visitor patterns
   - Path-based access
   - Custom type registration

For each example:
- Complete, runnable code
- Clear comments explaining what's demonstrated
- Expected output (if applicable)
- Common pitfalls to avoid

Output to:
- ts_design_docs/Value_EXAMPLES.md (update with new examples)
- examples/value/ directory (standalone programs)
```

---

### 8. Security/Safety Agent

**Purpose**: Validate code for security issues, memory safety, and data integrity.

**Trigger**: After tests pass, before final approval.

**Inputs**:
- All code files
- Test results
- Design documentation

**Outputs**:
- Security audit report
- Memory safety analysis
- Recommended fixes (if any)
- Final approval status

**Process**:
1. Static analysis for common vulnerabilities
2. Memory leak detection patterns
3. Buffer overflow potential
4. Use-after-free patterns
5. Race condition analysis
6. Input validation review
7. Exception safety audit

**Prompt Template**:
```
You are a Security/Safety Agent for the hgraph Value type system.

Your task is to audit the code for security and memory safety issues.

Audit checklist:

1. MEMORY SAFETY
   - [ ] No raw new/delete (use smart pointers)
   - [ ] SBO storage properly handles size transitions
   - [ ] Views don't outlive owning Values
   - [ ] No dangling pointers in cache
   - [ ] Proper cleanup in destructors
   - [ ] Exception-safe constructors

2. BUFFER SAFETY
   - [ ] Bounds checking on array access
   - [ ] Size validation before allocation
   - [ ] No integer overflow in size calculations
   - [ ] Safe string handling

3. TYPE SAFETY
   - [ ] Schema validation before type access
   - [ ] Safe casting patterns
   - [ ] No undefined behavior from type punning

4. THREAD SAFETY
   - [ ] Mutable cache is marked mutable appropriately
   - [ ] No data races in documented thread-safe patterns
   - [ ] GIL handling in Python interop

5. INPUT VALIDATION
   - [ ] Python object validation before use
   - [ ] Schema validation on construction
   - [ ] Null checks where appropriate

6. RESOURCE MANAGEMENT
   - [ ] RAII for all resources
   - [ ] No resource leaks on exception paths
   - [ ] Proper Python reference counting

For each issue found:
- Severity: [CRITICAL/HIGH/MEDIUM/LOW]
- CWE ID (if applicable)
- Location: [file:line]
- Description
- Proof of concept (if applicable)
- Remediation

Overall status: [SECURE / NEEDS_REMEDIATION]

If NEEDS_REMEDIATION, provide prioritized fix list.
```

---

## Agent Orchestration Workflow

### Phase 1: Initial Implementation
```
1. Test Writer Agent -> generates initial tests from User Guide
2. Code Generator Agent -> implements from Design Doc
3. Quality Reviewer Agent -> reviews implementation
   - If NEEDS_WORK: loop back to Code Generator
4. Documentation Agent -> adds inline docs
```

### Phase 2: Testing & Iteration
```
5. Test Runner Agent -> runs tests
   - If failures: loop back to Code Generator with fix suggestions
6. Edge Case Reviewer Agent -> identifies additional edge cases
   - Adds edge case tests
   - Loop back to Test Runner
```

### Phase 3: Finalization
```
7. Security/Safety Agent -> audits code
   - If NEEDS_REMEDIATION: loop back to Code Generator
8. Example Writer Agent -> creates usage examples
9. Final review and merge
```

### Invocation Pattern

To invoke an agent, use the Task tool:

```
Task(
    subagent_type="general-purpose",
    prompt="[Agent prompt from above with specific inputs filled in]",
    description="[Agent name]: [specific task]"
)
```

Example:
```
Task(
    subagent_type="general-purpose",
    prompt="""
    You are a Test Writer Agent for the hgraph Value type system.
    [... full prompt ...]
    Focus on section: 2. Creating Scalar Values
    """,
    description="Test Writer: scalar value tests"
)
```

---

## Agent Communication

Agents communicate through:
1. **Files**: Generated code, tests, reports
2. **Status**: APPROVED / NEEDS_WORK / NEEDS_FIXES / NEEDS_REMEDIATION
3. **Feedback**: Structured suggestions for other agents

Each agent should output its status and any feedback for dependent agents.

---

**End of Agent Orchestration Document**
