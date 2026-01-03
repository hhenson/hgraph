# Feature Design Workflow

**Purpose:** Standard workflow for researching, designing, and documenting new features before implementation.

---

## Overview

This workflow ensures new features are well-researched and properly designed before any code is written. The output is documentation sufficient to implement without ambiguity.

```
┌─────────────────┐
│ 1. Concept      │  Capture key concepts and requirements
│    Capture      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 2. Codebase     │  Research existing behavior and patterns
│    Research     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 3. External     │  Research best practices and libraries
│    Research     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 4. Synthesis    │  Match codebase with best practices
│                 │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 5. Document     │  Create design doc and user guide
│                 │
└────────┬────────┘
         │
         ▼
┌─────────────────┐     ┌─────────────────┐
│ 6. Review       │────▶│ 7. Iterate      │──┐
│    & Present    │     │                 │  │
└─────────────────┘     └─────────────────┘  │
         ▲                                    │
         └────────────────────────────────────┘
         │
         ▼
┌─────────────────┐
│ 8. Finalize     │  Ready for implementation
│                 │
└─────────────────┘
```

---

## Phase 1: Concept Capture

### Objective
Clearly define what the feature is and what problem it solves.

### Deliverables
Document the following:

1. **Feature Name** - Clear, descriptive name
2. **Problem Statement** - What problem does this solve?
3. **Key Concepts** - Core ideas and terminology
4. **Requirements** - What must the feature do?
5. **Constraints** - What limitations exist?
6. **Success Criteria** - How do we know it's done?

### Template

```markdown
## Feature: [Name]

### Problem Statement
[What problem are we solving? Why is this needed?]

### Key Concepts
- **Concept A**: [Definition]
- **Concept B**: [Definition]

### Requirements
1. Must [requirement]
2. Must [requirement]
3. Should [nice-to-have]

### Constraints
- [Constraint from existing architecture]
- [Performance constraint]
- [Compatibility constraint]

### Success Criteria
- [ ] [Measurable outcome]
- [ ] [Measurable outcome]
```

---

## Phase 2: Codebase Research

### Objective
Understand how the existing codebase handles related functionality.

### Activities

1. **Find Related Code**
   - Search for similar patterns or concepts
   - Identify code that the new feature will interact with
   - Note existing abstractions that could be reused

2. **Understand Current Behavior**
   - Trace through relevant Python implementation
   - Identify the authoritative behavior (Python is reference)
   - Document any quirks or edge cases

3. **Map Dependencies**
   - What existing components will this feature use?
   - What existing components might need modification?
   - What tests cover related functionality?

### Search Strategies

```bash
# Find related types/classes
grep -r "class.*TypeName" hgraph/

# Find related functions
grep -r "def function_name" hgraph/

# Find usages
grep -r "TypeName" hgraph/ hgraph_unit_tests/

# Find tests
ls hgraph_unit_tests/**/*test*.py
```

### Deliverables

```markdown
## Codebase Research

### Related Code
| Location | Description | Relevance |
|----------|-------------|-----------|
| `path/to/file.py` | [What it does] | [How it relates] |

### Current Behavior
[Description of how related features currently work]

### Integration Points
- [Component that will be affected]
- [Component that will be used]

### Existing Tests
- `test_file.py::test_name` - [What it tests]
```

---

## Phase 3: External Research

### Objective
Identify best practices, patterns, and existing solutions from the broader ecosystem.

### Activities

1. **Pattern Research**
   - How do other projects solve this problem?
   - What design patterns are commonly used?
   - What are the trade-offs of different approaches?

2. **Library Research**
   - Are there existing libraries that do this?
   - What can we learn from their API design?
   - What implementation techniques do they use?

3. **Documentation Research**
   - Academic papers or technical articles
   - Official documentation for related technologies
   - Community discussions and best practices

### Research Sources

- GitHub repositories with similar functionality
- Stack Overflow discussions
- Technical blogs and articles
- Library documentation (Boost, Abseil, etc.)
- C++ Core Guidelines
- Python documentation (for behavioral reference)

### Deliverables

```markdown
## External Research

### Patterns Found
| Pattern | Description | Pros | Cons |
|---------|-------------|------|------|
| [Name] | [Description] | [Pros] | [Cons] |

### Libraries Reviewed
| Library | Relevant Feature | Notes |
|---------|------------------|-------|
| [Name] | [Feature] | [What we can learn] |

### Key Insights
- [Insight from research]
- [Best practice discovered]

### References
- [URL or citation]
```

---

## Phase 4: Synthesis

### Objective
Combine codebase knowledge and external research to find the optimal approach.

### Activities

1. **Compare Options**
   - List all viable approaches
   - Evaluate each against requirements
   - Consider fit with existing codebase

2. **Identify Trade-offs**
   - Performance vs. simplicity
   - Flexibility vs. safety
   - Consistency vs. optimization

3. **Make Decisions**
   - Choose primary approach
   - Document rationale
   - Note alternatives considered

### Deliverables

```markdown
## Synthesis

### Options Considered
| Option | Description | Fit with Codebase | Trade-offs |
|--------|-------------|-------------------|------------|
| A | [Description] | [Good/Moderate/Poor] | [Trade-offs] |
| B | [Description] | [Good/Moderate/Poor] | [Trade-offs] |

### Recommended Approach
[Description of chosen approach]

### Rationale
- [Why this approach is best]
- [How it fits with existing code]
- [What trade-offs we're accepting]

### Alternatives Rejected
- **Option B**: Rejected because [reason]
```

---

## Phase 5: Documentation

### Objective
Create comprehensive documentation for the feature.

### Deliverables

#### Design Document
Location: `docs/design/[feature_name]_design.md`

```markdown
# [Feature Name] Design Document

## Overview
[Brief description of the feature]

## Problem Statement
[From Phase 1]

## Design

### Architecture
[How the feature fits into the system]

### Components
[Key classes, functions, data structures]

### Interfaces
[Public APIs and contracts]

### Data Flow
[How data moves through the system]

## Implementation Notes

### C++ Considerations
[C++ specific details]

### Python Binding
[How it will be exposed to Python]

### Error Handling
[How errors are handled]

## Testing Strategy
[How the feature will be tested]

## Migration/Compatibility
[Any migration needs or compatibility concerns]
```

#### User Guide
Location: `docs/guides/[feature_name]_guide.md`

```markdown
# [Feature Name] User Guide

## Introduction
[What this feature does and when to use it]

## Quick Start
[Minimal example to get started]

## Usage

### Basic Usage
[Common use cases with examples]

### Advanced Usage
[Complex scenarios]

## API Reference
[Key functions and classes]

## Examples
[Complete working examples]

## Troubleshooting
[Common issues and solutions]
```

---

## Phase 6: Review & Present

### Objective
Get feedback on the design before implementation.

### Activities

1. **Prepare Summary**
   - Highlight key decisions
   - Present trade-offs clearly
   - Identify open questions

2. **Present to User**
   - Walk through the design
   - Explain rationale for decisions
   - Solicit feedback

3. **Capture Feedback**
   - Note concerns and suggestions
   - Identify areas needing more research
   - Track decisions made

### Review Checklist

- [ ] Does the design meet all requirements?
- [ ] Is it consistent with existing codebase patterns?
- [ ] Are trade-offs clearly documented?
- [ ] Is the testing strategy adequate?
- [ ] Are there any open questions?

---

## Phase 7: Iterate

### Objective
Refine the design based on feedback.

### Activities

1. **Capture Changes**
   - Document all feedback received
   - Track what's changing and why

2. **Update Research** (if needed)
   - Additional codebase research
   - Additional external research

3. **Revise Design**
   - Update design document
   - Update user guide

4. **Re-present**
   - Show changes made
   - Confirm alignment

### Iteration Log Template

```markdown
## Iteration [N] - [Date]

### Feedback Received
- [Feedback item]

### Changes Made
- [Change made in response]

### Additional Research
- [If any research was needed]

### Open Items
- [Remaining questions]
```

---

## Phase 8: Finalize

### Objective
Lock down the design for implementation.

### Final Checklist

- [ ] All requirements addressed
- [ ] No open questions
- [ ] Design document complete
- [ ] User guide complete
- [ ] Implementation approach clear
- [ ] Testing strategy defined
- [ ] User approval obtained

### Output

The following documents should be complete:
1. `docs/design/[feature_name]_design.md` - Technical design
2. `docs/guides/[feature_name]_guide.md` - User documentation
3. Any supporting research notes

---

## Quick Reference

| Phase | Key Question | Output |
|-------|--------------|--------|
| 1. Concept | What are we building? | Requirements doc |
| 2. Codebase | How does existing code work? | Research notes |
| 3. External | What's best practice? | Research notes |
| 4. Synthesis | What's our approach? | Decision document |
| 5. Document | How does it work? | Design + Guide |
| 6. Review | Does user approve? | Feedback |
| 7. Iterate | What needs to change? | Updated docs |
| 8. Finalize | Are we ready? | Final docs |
