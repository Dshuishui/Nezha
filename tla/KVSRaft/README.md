# KVS-Raft TLA+ Verification Guide

## File Description

1. **KVSRaft.tla** - TLA+ specification for the KVS-Raft algorithm
2. **KVSRaft.cfg** - Configuration file for the TLC model checker

## Running Verification

### 1. Using TLA+ Toolbox
1. Open TLA+ Toolbox
2. Create a new specification: File -> Open Spec -> Add New Spec
3. Select the `KVSRaft.tla` file
4. Create a new model: TLC Model Checker -> New Model
5. In the model configuration:
   - Ensure Behavior Spec is set to `Spec`
   - Add `Safety` to Invariants
   - Add constraints from the config file to State Constraint
6. Run model checking

### 2. Using Command Line TLC
```bash
tlc KVSRaft.tla -config KVSRaft.cfg
```

## Verification Focus

### Safety Properties

1. **ElectionSafety**
   - Verifies at most one Leader per term
   - Core safety guarantee of the Raft algorithm

2. **LogMatching** 
   - Verifies identical log entries at same index and term across different servers
   - Ensures log consistency

3. **OffsetCorrectness**
   - KVS-Raft specific property
   - Verifies all offsets point to valid ValueLog positions
   - Ensures offset mappings in state machine are valid

4. **ValueConsistency**
   - Verifies committed log entries are consistent across all nodes
   - Basic guarantee of distributed consistency

## Model Parameters

Current configuration uses a small model to ensure verification completes:

- **Server count**: 3 servers (s1, s2, s3)
- **Value set**: 2 values (v1, v2)
- **Log length limit**: Maximum 4 entries
- **Term limit**: Maximum 4 terms
- **Message count limit**: Maximum 10 messages