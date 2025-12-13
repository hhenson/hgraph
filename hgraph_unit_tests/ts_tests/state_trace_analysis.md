# Time-Series State Models

Auto-generated from test execution traces.

Total transitions analyzed: 9873

## Overview

| Type | Properties | Methods | Transitions |
|------|------------|---------|-------------|
| REF | 1 | 2 | 1 |
| SIGNAL | 7 | 5 | 7 |
| TS | 10 | 10 | 15 |
| TSB | 9 | 8 | 9 |
| TSD | 10 | 9 | 11 |
| TSL | 9 | 9 | 14 |
| TSS | 10 | 9 | 13 |
| TSW | 10 | 9 | 9 |

## REF State Model

### Properties

- `active`

### State Diagram

```mermaid
stateDiagram-v2
    direction LR

    %% active states
    "active_False" --> "active_True": make_active
```

### Transition Table

| From State | To State | Trigger | Tests |
|------------|----------|---------|-------|
| active=False | active=True | make_active | 1 tests |

## SIGNAL State Model

### Properties

- `active`
- `all_valid`
- `bound`
- `has_peer`
- `modified`
- `sample_time`
- `valid`

### State Diagram

```mermaid
stateDiagram-v2
    direction LR

    %% active states
    "active_False" --> "active_True": make_active

    %% bound states
    "bound_False" --> "bound_True": bind_output
    "bound_True" --> "bound_False": un_bind_output

    %% has_peer states
    "has_peer_False" --> "has_peer_True": bind_output
    "has_peer_True" --> "has_peer_False": un_bind_output

    %% modified states
    "modified_False" --> "modified_None": bind_output

    %% valid states
    "valid_True" --> "valid_False": un_bind_output
```

### Transition Table

| From State | To State | Trigger | Tests |
|------------|----------|---------|-------|
| active=False | active=True | make_active | 9 tests |
| bound=False | bound=True | bind_output | 9 tests |
| bound=True | bound=False | un_bind_output | 8 tests |
| has_peer=False | has_peer=True | bind_output | 9 tests |
| has_peer=True | has_peer=False | un_bind_output | 8 tests |
| modified=False | modified=None | bind_output | 9 tests |
| valid=True | valid=False | un_bind_output | 9 tests |

## TS State Model

### Properties

- `active`
- `all_valid`
- `bound`
- `has_peer`
- `last_modified_time`
- `modified`
- `sample_time`
- `subscriber_count`
- `valid`
- `value`

### State Diagram

```mermaid
stateDiagram-v2
    direction LR

    %% active states
    "active_False" --> "active_True": make_active
    "active_True" --> "active_False": make_passive

    %% bound states
    "bound_False" --> "bound_True": bind_output
    "bound_True" --> "bound_False": un_bind_output

    %% has_peer states
    "has_peer_False" --> "has_peer_True": bind_output
    "has_peer_True" --> "has_peer_False": un_bind_output

    %% modified states
    "modified_False" --> "modified_None": bind_output
    "modified_False" --> "modified_True": bind_output
    "modified_False" --> "modified_True": mark_modified
    "modified_False" --> "modified_True": value.setter
    "modified_None" --> "modified_False": un_bind_output

    %% valid states
    "valid_False" --> "valid_True": bind_output
    "valid_False" --> "valid_True": mark_modified
    "valid_False" --> "valid_True": value.setter
    "valid_True" --> "valid_False": un_bind_output
```

### Transition Table

| From State | To State | Trigger | Tests |
|------------|----------|---------|-------|
| active=False | active=True | make_active | 201 tests |
| active=True | active=False | make_passive | 31 tests |
| bound=False | bound=True | bind_output | 203 tests |
| bound=True | bound=False | un_bind_output | 203 tests |
| has_peer=False | has_peer=True | bind_output | 203 tests |
| has_peer=True | has_peer=False | un_bind_output | 203 tests |
| modified=False | modified=None | bind_output | 187 tests |
| modified=False | modified=True | bind_output | 34 tests |
| modified=False | modified=True | mark_modified | 205 tests |
| modified=False | modified=True | value.setter | 205 tests |
| modified=None | modified=False | un_bind_output | 5 tests |
| valid=False | valid=True | bind_output | 34 tests |
| valid=False | valid=True | mark_modified | 205 tests |
| valid=False | valid=True | value.setter | 205 tests |
| valid=True | valid=False | un_bind_output | 203 tests |

## TSB State Model

### Properties

- `active`
- `all_valid`
- `bound`
- `has_peer`
- `last_modified_time`
- `modified`
- `sample_time`
- `subscriber_count`
- `valid`

### State Diagram

```mermaid
stateDiagram-v2
    direction LR

    %% active states
    "active_False" --> "active_True": make_active

    %% bound states
    "bound_False" --> "bound_True": bind_output
    "bound_True" --> "bound_False": un_bind_output

    %% has_peer states
    "has_peer_False" --> "has_peer_True": bind_output
    "has_peer_True" --> "has_peer_False": un_bind_output

    %% modified states
    "modified_False" --> "modified_None": bind_output
    "modified_False" --> "modified_True": mark_modified

    %% valid states
    "valid_False" --> "valid_True": mark_modified
    "valid_True" --> "valid_False": un_bind_output
```

### Transition Table

| From State | To State | Trigger | Tests |
|------------|----------|---------|-------|
| active=False | active=True | make_active | 10 tests |
| bound=False | bound=True | bind_output | 11 tests |
| bound=True | bound=False | un_bind_output | 11 tests |
| has_peer=False | has_peer=True | bind_output | 11 tests |
| has_peer=True | has_peer=False | un_bind_output | 11 tests |
| modified=False | modified=None | bind_output | 10 tests |
| modified=False | modified=True | mark_modified | 20 tests |
| valid=False | valid=True | mark_modified | 20 tests |
| valid=True | valid=False | un_bind_output | 17 tests |

## TSD State Model

### Properties

- `active`
- `all_valid`
- `bound`
- `has_peer`
- `last_modified_time`
- `length`
- `modified`
- `sample_time`
- `subscriber_count`
- `valid`

### State Diagram

```mermaid
stateDiagram-v2
    direction LR

    %% active states
    "active_False" --> "active_True": make_active
    "active_True" --> "active_False": make_passive

    %% bound states
    "bound_False" --> "bound_True": bind_output
    "bound_True" --> "bound_False": un_bind_output

    %% has_peer states
    "has_peer_False" --> "has_peer_True": bind_output

    %% modified states
    "modified_False" --> "modified_None": bind_output
    "modified_False" --> "modified_True": bind_output
    "modified_False" --> "modified_True": mark_modified

    %% valid states
    "valid_False" --> "valid_True": bind_output
    "valid_False" --> "valid_True": mark_modified
    "valid_True" --> "valid_False": un_bind_output
```

### Transition Table

| From State | To State | Trigger | Tests |
|------------|----------|---------|-------|
| active=False | active=True | make_active | 26 tests |
| active=True | active=False | make_passive | 1 tests |
| bound=False | bound=True | bind_output | 26 tests |
| bound=True | bound=False | un_bind_output | 26 tests |
| has_peer=False | has_peer=True | bind_output | 26 tests |
| modified=False | modified=None | bind_output | 24 tests |
| modified=False | modified=True | bind_output | 2 tests |
| modified=False | modified=True | mark_modified | 34 tests |
| valid=False | valid=True | bind_output | 2 tests |
| valid=False | valid=True | mark_modified | 34 tests |
| valid=True | valid=False | un_bind_output | 26 tests |

## TSL State Model

### Properties

- `active`
- `all_valid`
- `bound`
- `has_peer`
- `last_modified_time`
- `modified`
- `sample_time`
- `subscriber_count`
- `valid`

### State Diagram

```mermaid
stateDiagram-v2
    direction LR

    %% active states
    "active_False" --> "active_True": bind_output
    "active_False" --> "active_True": make_active
    "active_True" --> "active_False": make_passive

    %% bound states
    "bound_False" --> "bound_True": bind_output
    "bound_True" --> "bound_False": un_bind_output

    %% has_peer states
    "has_peer_False" --> "has_peer_True": bind_output
    "has_peer_True" --> "has_peer_False": un_bind_output

    %% modified states
    "modified_False" --> "modified_None": bind_output
    "modified_False" --> "modified_True": bind_output
    "modified_False" --> "modified_True": mark_modified
    "modified_False" --> "modified_True": un_bind_output

    %% valid states
    "valid_False" --> "valid_True": bind_output
    "valid_False" --> "valid_True": mark_modified
    "valid_True" --> "valid_False": un_bind_output
```

### Transition Table

| From State | To State | Trigger | Tests |
|------------|----------|---------|-------|
| active=False | active=True | bind_output | 3 tests |
| active=False | active=True | make_active | 20 tests |
| active=True | active=False | make_passive | 1 tests |
| bound=False | bound=True | bind_output | 20 tests |
| bound=True | bound=False | un_bind_output | 20 tests |
| has_peer=False | has_peer=True | bind_output | 20 tests |
| has_peer=True | has_peer=False | un_bind_output | 20 tests |
| modified=False | modified=None | bind_output | 17 tests |
| modified=False | modified=True | bind_output | 3 tests |
| modified=False | modified=True | mark_modified | 27 tests |
| modified=False | modified=True | un_bind_output | 1 tests |
| valid=False | valid=True | bind_output | 4 tests |
| valid=False | valid=True | mark_modified | 27 tests |
| valid=True | valid=False | un_bind_output | 28 tests |

## TSS State Model

### Properties

- `active`
- `all_valid`
- `bound`
- `has_peer`
- `last_modified_time`
- `length`
- `modified`
- `sample_time`
- `subscriber_count`
- `valid`

### State Diagram

```mermaid
stateDiagram-v2
    direction LR

    %% active states
    "active_False" --> "active_True": make_active
    "active_True" --> "active_False": make_passive

    %% bound states
    "bound_False" --> "bound_True": bind_output
    "bound_True" --> "bound_False": un_bind_output

    %% has_peer states
    "has_peer_False" --> "has_peer_True": bind_output
    "has_peer_True" --> "has_peer_False": un_bind_output

    %% modified states
    "modified_False" --> "modified_None": bind_output
    "modified_False" --> "modified_True": bind_output
    "modified_False" --> "modified_True": mark_modified
    "modified_False" --> "modified_True": un_bind_output

    %% valid states
    "valid_False" --> "valid_True": bind_output
    "valid_False" --> "valid_True": mark_modified
    "valid_True" --> "valid_False": un_bind_output
```

### Transition Table

| From State | To State | Trigger | Tests |
|------------|----------|---------|-------|
| active=False | active=True | make_active | 33 tests |
| active=True | active=False | make_passive | 5 tests |
| bound=False | bound=True | bind_output | 57 tests |
| bound=True | bound=False | un_bind_output | 57 tests |
| has_peer=False | has_peer=True | bind_output | 57 tests |
| has_peer=True | has_peer=False | un_bind_output | 57 tests |
| modified=False | modified=None | bind_output | 51 tests |
| modified=False | modified=True | bind_output | 6 tests |
| modified=False | modified=True | mark_modified | 68 tests |
| modified=False | modified=True | un_bind_output | 57 tests |
| valid=False | valid=True | bind_output | 6 tests |
| valid=False | valid=True | mark_modified | 68 tests |
| valid=True | valid=False | un_bind_output | 57 tests |

## TSW State Model

### Properties

- `active`
- `all_valid`
- `bound`
- `has_peer`
- `last_modified_time`
- `length`
- `modified`
- `sample_time`
- `subscriber_count`
- `valid`

### State Diagram

```mermaid
stateDiagram-v2
    direction LR

    %% active states
    "active_False" --> "active_True": make_active

    %% bound states
    "bound_False" --> "bound_True": bind_output
    "bound_True" --> "bound_False": un_bind_output

    %% has_peer states
    "has_peer_False" --> "has_peer_True": bind_output
    "has_peer_True" --> "has_peer_False": un_bind_output

    %% modified states
    "modified_False" --> "modified_None": bind_output
    "modified_False" --> "modified_True": mark_modified

    %% valid states
    "valid_False" --> "valid_True": mark_modified
    "valid_True" --> "valid_False": un_bind_output
```

### Transition Table

| From State | To State | Trigger | Tests |
|------------|----------|---------|-------|
| active=False | active=True | make_active | 18 tests |
| bound=False | bound=True | bind_output | 18 tests |
| bound=True | bound=False | un_bind_output | 18 tests |
| has_peer=False | has_peer=True | bind_output | 18 tests |
| has_peer=True | has_peer=False | un_bind_output | 18 tests |
| modified=False | modified=None | bind_output | 18 tests |
| modified=False | modified=True | mark_modified | 18 tests |
| valid=False | valid=True | mark_modified | 18 tests |
| valid=True | valid=False | un_bind_output | 18 tests |

# Test Coverage Report

## Summary by Type

### REF
- Properties tracked: active
- Methods instrumented: make_active, notify
- Unique transitions: 1
- Transitions with test coverage: 1

### SIGNAL
- Properties tracked: active, all_valid, bound, has_peer, modified, sample_time, valid
- Methods instrumented: bind_output, make_active, make_passive, notify, un_bind_output
- Unique transitions: 7
- Transitions with test coverage: 7

### TS
- Properties tracked: active, all_valid, bound, has_peer, last_modified_time, modified, sample_time, subscriber_count, valid, value
- Methods instrumented: _notify, bind_output, make_active, make_passive, mark_modified, notify, subscribe, un_bind_output, unsubscribe, value.setter
- Unique transitions: 15
- Transitions with test coverage: 15

### TSB
- Properties tracked: active, all_valid, bound, has_peer, last_modified_time, modified, sample_time, subscriber_count, valid
- Methods instrumented: _notify, bind_output, make_active, mark_modified, notify, subscribe, un_bind_output, unsubscribe
- Unique transitions: 9
- Transitions with test coverage: 9

### TSD
- Properties tracked: active, all_valid, bound, has_peer, last_modified_time, length, modified, sample_time, subscriber_count, valid
- Methods instrumented: _notify, bind_output, make_active, make_passive, mark_modified, notify, subscribe, un_bind_output, unsubscribe
- Unique transitions: 11
- Transitions with test coverage: 11

### TSL
- Properties tracked: active, all_valid, bound, has_peer, last_modified_time, modified, sample_time, subscriber_count, valid
- Methods instrumented: _notify, bind_output, make_active, make_passive, mark_modified, notify, subscribe, un_bind_output, unsubscribe
- Unique transitions: 14
- Transitions with test coverage: 14

### TSS
- Properties tracked: active, all_valid, bound, has_peer, last_modified_time, length, modified, sample_time, subscriber_count, valid
- Methods instrumented: _notify, bind_output, make_active, make_passive, mark_modified, notify, subscribe, un_bind_output, unsubscribe
- Unique transitions: 13
- Transitions with test coverage: 13

### TSW
- Properties tracked: active, all_valid, bound, has_peer, last_modified_time, length, modified, sample_time, subscriber_count, valid
- Methods instrumented: _notify, bind_output, make_active, make_passive, mark_modified, notify, subscribe, un_bind_output, unsubscribe
- Unique transitions: 9
- Transitions with test coverage: 9
