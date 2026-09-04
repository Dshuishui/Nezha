# TLA+ Formal Verification

This folder contains TLA+ formal specifications and verification code for the KVS-Raft algorithm used in the Nezha system. It proves the safety and correctness of the algorithm through mathematical modeling.

## Directory Structure

### miniNezhaRaft/
Minimal KVS-Raft specification designed for quick verification of core safety properties. Configured with a small state space (2 server nodes, 1 value) that can complete model checking within seconds. Primarily verifies election safety and offset correctness, the two key properties.

### NezhaRaft/
Standard version of the KVS-Raft specification with more complete algorithm logic. Uses a configuration of 3 server nodes and 2 values, resulting in a relatively large state space and significantly longer verification times. This version is closer to actual system behavior.

### miniKVSRaft/
Simplified version of KVS-Raft that adds more message passing mechanisms on top of miniNezhaRaft, while still maintaining a small state space to ensure verification can complete.

### KVSRaft/
Most complete KVS-Raft specification, including detailed message passing, network partition handling, and various edge cases. Due to the enormous state space, it's prone to state space explosion problems, leading to extremely long verification times or inability to complete verification.

## About State Space Explosion

TLA+ model checkers need to traverse all possible system states to verify safety properties. When system parameters increase (such as number of servers, message types, log length, etc.), the number of states grows exponentially. For example:
- Increasing servers from 2 to 3 grows possible interaction combinations from a few to dozens
- Adding message types and network failure simulation further inflates the state space
- Relaxing log length constraints means every possible log state needs to be checked

This is why verification of complete versions becomes extremely time-consuming, while simplified versions ensure verification feasibility by limiting parameter ranges.

## Core Verification Focus

All specifications focus on verifying KVS-Raft's key innovation over traditional Raft: the key-value separation storage mechanism. Traditional Raft stores complete key-value pairs in the log, while KVS-Raft stores values in a separate valueLog with the log only recording offsets. This design requires special verification of offset reference correctness to ensure no dangling pointers or out-of-bounds access.

## Standard Reference

### raft.tla
This is the official standard Raft algorithm TLA+ specification, serving as a reference baseline. Our KVS-Raft specifications extend and modify this foundation, adding logic related to key-value separation.

## Running Verification

For mini versions:
```bash
tlc NezhaRaft.cfg NezhaRaft.tla
```

Complete versions are recommended to run on high-performance machines with appropriate state constraints to control verification scope.