--------------------------------- MODULE NezhaRaft ---------------------------------
\* 最简单的KVS-Raft TLA+规范，先确保能运行

EXTENDS Naturals, FiniteSets, Sequences

CONSTANTS Server, Value, Follower, Candidate, Leader, Nil

VARIABLES currentTerm, state, log, valueLog

----

Init ==
    /\ currentTerm = [i \in Server |-> 0]
    /\ state = [i \in Server |-> Follower]
    /\ log = [i \in Server |-> << >>]
    /\ valueLog = [i \in Server |-> << >>]

\* 最简单的超时动作，让某个节点变成候选者
Timeout(i) ==
    /\ state[i] = Follower
    /\ state' = [state EXCEPT ![i] = Candidate]
    /\ currentTerm' = [currentTerm EXCEPT ![i] = @ + 1]
    /\ UNCHANGED <<log, valueLog>>

\* 简单的客户端请求，直接写入日志（KVS-Raft核心：分离存储）
ClientRequest(i, v) ==
    /\ state[i] = Leader
    /\ LET offset == Len(valueLog[i]) + 1
           entry == [term |-> currentTerm[i], offset |-> offset]
       IN /\ valueLog' = [valueLog EXCEPT ![i] = Append(@, v)]
          /\ log' = [log EXCEPT ![i] = Append(@, entry)]
    /\ UNCHANGED <<currentTerm, state>>

\* 候选者成为领导者（确保选举安全性）
BecomeLeader(i) ==
    /\ state[i] = Candidate
    \* 确保在同一term中没有其他Leader
    /\ \A j \in Server : 
        j = i \/ state[j] # Leader \/ currentTerm[j] # currentTerm[i]
    /\ state' = [state EXCEPT ![i] = Leader]
    /\ UNCHANGED <<currentTerm, log, valueLog>>

Next ==
    \/ \E i \in Server : Timeout(i)
    \/ \E i \in Server : BecomeLeader(i)
    \/ \E i \in Server, v \in Value : ClientRequest(i, v)

Spec == Init /\ [][Next]_<<currentTerm, state, log, valueLog>>

----
\* 最基本的安全属性

ElectionSafety ==
    \A i,j \in Server :
        /\ state[i] = Leader
        /\ state[j] = Leader
        => i = j

\* KVS-Raft核心属性：offset正确性
OffsetCorrectness ==
    \A i \in Server :
    \A index \in DOMAIN log[i] :
        /\ log[i][index].offset > 0
        /\ log[i][index].offset <= Len(valueLog[i])

Safety == ElectionSafety /\ OffsetCorrectness

\* 状态约束，限制验证范围
StateConstraint ==
    /\ \A i \in Server : currentTerm[i] <= 2
    /\ \A i \in Server : Len(log[i]) <= 1
    /\ \A i \in Server : Len(valueLog[i]) <= 1

====