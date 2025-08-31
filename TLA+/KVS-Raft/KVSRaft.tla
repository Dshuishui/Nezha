----
\* 对称性定义
ServerSymmetry == Permutations(Server)

--------------------------------- MODULE KVSRaft ---------------------------------
\* KVS-Raft: 键值分离的Raft共识算法TLA+规范
\* 基于标准Raft算法，集成键值分离存储机制

EXTENDS Naturals, FiniteSets, Sequences, TLC

\* 服务器节点集合
CONSTANTS Server

\* 请求值集合
CONSTANTS Value

\* 服务器状态常量
CONSTANTS Follower, Candidate, Leader

\* 特殊值
CONSTANTS Nil

\* 消息类型
CONSTANTS RequestVoteRequest, RequestVoteResponse,
          AppendEntriesRequest, AppendEntriesResponse

----
\* 全局变量

\* 消息包：表示节点间发送的请求和响应
VARIABLE messages

\* 历史变量：记录成功的选举信息（用于证明）
VARIABLE elections

\* 历史变量：记录系统中所有曾经存在的日志（用于证明）
VARIABLE allLogs

----
\* 每个服务器的状态变量

\* 服务器任期号
VARIABLE currentTerm
\* 服务器状态（Follower、Candidate或Leader）
VARIABLE state
\* 当前任期投票给的候选者
VARIABLE votedFor
serverVars == <<currentTerm, state, votedFor>>

\* Raft日志序列（标准Raft日志，包含完整的key-value信息）
VARIABLE log
\* 已提交的日志索引
VARIABLE commitIndex
logVars == <<log, commitIndex>>

----
\* KVS-Raft特有的存储层变量

\* ValueLog：每个节点本地存储实际的键值对数据
\* valueLog[i] 表示节点i的ValueLog，是一个序列，每个元素包含key和value
VARIABLE valueLog

\* OffsetMap：每个节点的状态机，存储key到本地ValueLog中offset的映射
\* offsetMap[i] 表示节点i的状态机，是一个函数：Key -> Offset
VARIABLE offsetMap

kvsVars == <<valueLog, offsetMap>>

----
\* 候选者状态变量
VARIABLE votesResponded
VARIABLE votesGranted
VARIABLE voterLog
candidateVars == <<votesResponded, votesGranted, voterLog>>

\* Leader状态变量
VARIABLE nextIndex
VARIABLE matchIndex
leaderVars == <<nextIndex, matchIndex, elections>>

----
\* 所有变量
vars == <<messages, allLogs, serverVars, candidateVars, leaderVars, logVars, kvsVars>>

----
\* 辅助函数

\* 多数派集合
Quorum == {i \in SUBSET(Server) : Cardinality(i) * 2 > Cardinality(Server)}

\* 获取日志的最后一个任期
LastTerm(xlog) == IF Len(xlog) = 0 THEN 0 ELSE xlog[Len(xlog)].term

\* 获取日志的最后一个索引
LastIndex(xlog) == Len(xlog)

\* 消息处理辅助函数
WithMessage(m, msgs) ==
    IF m \in DOMAIN msgs THEN
        [msgs EXCEPT ![m] = msgs[m] + 1]
    ELSE
        msgs @@ (m :> 1)

WithoutMessage(m, msgs) ==
    IF m \in DOMAIN msgs THEN
        IF msgs[m] <= 1 THEN [i \in DOMAIN msgs \ {m} |-> msgs[i]]
        ELSE [msgs EXCEPT ![m] = msgs[m] - 1]
    ELSE
        msgs

Send(m) == messages' = WithMessage(m, messages)

Discard(m) == messages' = WithoutMessage(m, messages)

Reply(response, request) ==
    messages' = WithoutMessage(request, WithMessage(response, messages))

Min(s) == CHOOSE x \in s : \A y \in s : x <= y
Max(s) == CHOOSE x \in s : \A y \in s : x >= y

----
\* 初始状态

Init == /\ currentTerm = [i \in Server |-> 1]
        /\ state       = [i \in Server |-> Follower]
        /\ votedFor    = [i \in Server |-> Nil]
        /\ log         = [i \in Server |-> << >>]
        /\ commitIndex = [i \in Server |-> 0]
        \* KVS-Raft特有的初始化
        /\ valueLog    = [i \in Server |-> << >>]
        /\ offsetMap   = [i \in Server |-> [k \in {} |-> 0]]
        \* 其他变量初始化
        /\ messages = [m \in {} |-> 0]
        /\ elections = {}
        /\ allLogs = {}
        /\ votesResponded = [i \in Server |-> {}]
        /\ votesGranted   = [i \in Server |-> {}]
        /\ voterLog       = [i \in Server |-> [j \in {} |-> <<>>]]
        /\ nextIndex      = [i \in Server |-> [j \in Server |-> 1]]
        /\ matchIndex     = [i \in Server |-> [j \in Server |-> 0]]

----
\* 状态转换

\* 节点重启
Restart(i) ==
    /\ state' = [state EXCEPT ![i] = Follower]
    /\ votedFor' = [votedFor EXCEPT ![i] = Nil]
    /\ votesResponded' = [votesResponded EXCEPT ![i] = {}]
    /\ votesGranted' = [votesGranted EXCEPT ![i] = {}]
    /\ voterLog' = [voterLog EXCEPT ![i] = [j \in {} |-> <<>>]]
    /\ UNCHANGED <<messages, currentTerm, log, commitIndex, valueLog, offsetMap, 
                  nextIndex, matchIndex, elections, allLogs>>

\* 选举超时，节点变为候选者
Timeout(i) == 
    /\ state[i] \in {Follower, Candidate}
    /\ state' = [state EXCEPT ![i] = Candidate]
    /\ currentTerm' = [currentTerm EXCEPT ![i] = currentTerm[i] + 1]
    /\ votedFor' = [votedFor EXCEPT ![i] = Nil]
    /\ votesResponded' = [votesResponded EXCEPT ![i] = {}]
    /\ votesGranted' = [votesGranted EXCEPT ![i] = {}]
    /\ voterLog' = [voterLog EXCEPT ![i] = [j \in {} |-> <<>>]]
    /\ UNCHANGED <<messages, leaderVars, logVars, kvsVars, allLogs>>

\* 候选者请求投票
RequestVote(i, j) ==
    /\ state[i] = Candidate
    /\ j \notin votesResponded[i]
    /\ Send([mtype         |-> RequestVoteRequest,
             mterm         |-> currentTerm[i],
             mlastLogTerm  |-> LastTerm(log[i]),
             mlastLogIndex |-> LastIndex(log[i]),
             msource       |-> i,
             mdest         |-> j])
    /\ UNCHANGED <<serverVars, candidateVars, leaderVars, logVars, kvsVars, allLogs>>

\* KVS-Raft客户端请求处理
ClientRequest(i, k, v) ==
    /\ state[i] = Leader
    /\ LET \* 创建ValueLog条目
           valueEntry == [key |-> k, value |-> v]
           \* 写入本地ValueLog
           newValueLog == Append(valueLog[i], valueEntry)
           \* 创建Raft日志条目（包含完整的key-value信息）
           logEntry == [term  |-> currentTerm[i],
                       key   |-> k,
                       value |-> v]
       IN  /\ valueLog' = [valueLog EXCEPT ![i] = newValueLog]
           /\ log' = [log EXCEPT ![i] = Append(log[i], logEntry)]
           /\ UNCHANGED <<messages, serverVars, candidateVars, leaderVars, 
                         commitIndex, offsetMap, elections, allLogs>>

\* Leader发送AppendEntries
AppendEntries(i, j) ==
    /\ i /= j
    /\ state[i] = Leader
    /\ LET prevLogIndex == nextIndex[i][j] - 1
           prevLogTerm == IF prevLogIndex > 0 THEN
                              log[i][prevLogIndex].term
                          ELSE
                              0
           lastEntry == Min({Len(log[i]), nextIndex[i][j]})
           entries == SubSeq(log[i], nextIndex[i][j], lastEntry)
       IN Send([mtype          |-> AppendEntriesRequest,
                mterm          |-> currentTerm[i],
                mprevLogIndex  |-> prevLogIndex,
                mprevLogTerm   |-> prevLogTerm,
                mentries       |-> entries,
                mlog           |-> log[i],
                mcommitIndex   |-> Min({commitIndex[i], lastEntry}),
                msource        |-> i,
                mdest          |-> j])
    /\ UNCHANGED <<serverVars, candidateVars, leaderVars, logVars, kvsVars, allLogs>>

\* 候选者成为Leader
BecomeLeader(i) ==
    /\ state[i] = Candidate
    /\ votesGranted[i] \in Quorum
    /\ state'      = [state EXCEPT ![i] = Leader]
    /\ nextIndex'  = [nextIndex EXCEPT ![i] =
                         [j \in Server |-> Len(log[i]) + 1]]
    /\ matchIndex' = [matchIndex EXCEPT ![i] =
                         [j \in Server |-> 0]]
    /\ elections'  = elections \cup
                         {[eterm     |-> currentTerm[i],
                           eleader   |-> i,
                           elog      |-> log[i],
                           evotes    |-> votesGranted[i],
                           evoterLog |-> voterLog[i]]}
    /\ UNCHANGED <<messages, serverVars, candidateVars, logVars, kvsVars, allLogs>>

\* 更新任期（收到更大任期时）
UpdateTerm(i, j, m) ==
    /\ m.mterm > currentTerm[i]
    /\ currentTerm'    = [currentTerm EXCEPT ![i] = m.mterm]
    /\ state'          = [state EXCEPT ![i] = Follower]
    /\ votedFor'       = [votedFor EXCEPT ![i] = Nil]
    /\ UNCHANGED <<messages, candidateVars, leaderVars, logVars, kvsVars, elections, allLogs>>

\* 处理RequestVote请求
HandleRequestVoteRequest(i, j, m) ==
    LET logOk == \/ m.mlastLogTerm > LastTerm(log[i])
                 \/ /\ m.mlastLogTerm = LastTerm(log[i])
                    /\ m.mlastLogIndex >= LastIndex(log[i])
        grant == /\ m.mterm = currentTerm[i]
                 /\ logOk
                 /\ votedFor[i] \in {Nil, j}
    IN /\ m.mterm <= currentTerm[i]
       /\ IF grant THEN votedFor' = [votedFor EXCEPT ![i] = j]
                   ELSE UNCHANGED votedFor
       /\ Reply([mtype        |-> RequestVoteResponse,
                 mterm        |-> currentTerm[i],
                 mvoteGranted |-> grant,
                 msource      |-> i,
                 mdest        |-> j],
                m)
       /\ UNCHANGED <<currentTerm, state, candidateVars, leaderVars, logVars, kvsVars, elections, allLogs>>

\* 处理RequestVote响应
HandleRequestVoteResponse(i, j, m) ==
    /\ m.mterm = currentTerm[i]
    /\ votesResponded' = [votesResponded EXCEPT ![i] =
                              votesResponded[i] \cup {j}]
    /\ \/ /\ m.mvoteGranted
          /\ votesGranted' = [votesGranted EXCEPT ![i] =
                                  votesGranted[i] \cup {j}]
          /\ voterLog' = [voterLog EXCEPT ![i] =
                              voterLog[i] @@ (j :> log[j])]
       \/ /\ ~m.mvoteGranted
          /\ UNCHANGED <<votesGranted, voterLog>>
    /\ Discard(m)
    /\ UNCHANGED <<serverVars, leaderVars, logVars, kvsVars, elections, allLogs>>

\* 处理AppendEntries请求
HandleAppendEntriesRequest(i, j, m) ==
    LET logOk == \/ m.mprevLogIndex = 0
                 \/ /\ m.mprevLogIndex > 0
                    /\ m.mprevLogIndex <= Len(log[i])
                    /\ log[i][m.mprevLogIndex].term = m.mprevLogTerm
    IN /\ m.mterm = currentTerm[i]
       /\ IF logOk /\ m.mentries /= << >>
          THEN 
            \* 接受日志条目并写入本地ValueLog
            LET entry == m.mentries[1]  \* 简化：一次处理一个条目
                index == m.mprevLogIndex + 1
                \* 更新Raft日志
                newLog == IF index <= Len(log[i])
                         THEN [log[i] EXCEPT ![index] = entry]
                         ELSE Append(log[i], entry)
                \* 将条目写入本地ValueLog
                valueEntry == [key |-> entry.key, value |-> entry.value]
                newValueLog == Append(valueLog[i], valueEntry)
            IN /\ log' = [log EXCEPT ![i] = newLog]
               /\ valueLog' = [valueLog EXCEPT ![i] = newValueLog]
               /\ Reply([mtype           |-> AppendEntriesResponse,
                        mterm           |-> currentTerm[i],
                        msuccess        |-> TRUE,
                        mmatchIndex     |-> index,
                        msource         |-> i,
                        mdest           |-> j],
                       m)
          ELSE
            /\ Reply([mtype           |-> AppendEntriesResponse,
                     mterm           |-> currentTerm[i],
                     msuccess        |-> logOk,
                     mmatchIndex     |-> IF logOk THEN m.mprevLogIndex ELSE 0,
                     msource         |-> i,
                     mdest           |-> j],
                    m)
            /\ UNCHANGED <<log, valueLog>>
       /\ UNCHANGED <<serverVars, candidateVars, leaderVars, commitIndex, offsetMap, elections, allLogs>>

\* 应用已提交的日志到状态机（KVS-Raft关键部分）
ApplyToStateMachine(i) ==
    /\ commitIndex[i] < Len(log[i])
    /\ LET index == commitIndex[i] + 1
           entry == log[i][index]
           \* 计算该entry在本地ValueLog中的offset
           \* 简化假设：offset就是ValueLog中对应位置的索引
           offset == index  \* 简化处理
       IN /\ commitIndex' = [commitIndex EXCEPT ![i] = index]
          \* 更新状态机：存储key到offset的映射而不是key到value的映射
          /\ offsetMap' = [offsetMap EXCEPT ![i] = 
                              offsetMap[i] @@ (entry.key :> offset)]
          /\ UNCHANGED <<messages, serverVars, candidateVars, leaderVars, log, valueLog, elections, allLogs>>

\* 处理AppendEntries响应
HandleAppendEntriesResponse(i, j, m) ==
    /\ m.mterm = currentTerm[i]
    /\ \/ /\ m.msuccess
          /\ nextIndex'  = [nextIndex  EXCEPT ![i][j] = m.mmatchIndex + 1]
          /\ matchIndex' = [matchIndex EXCEPT ![i][j] = m.mmatchIndex]
       \/ /\ ~m.msuccess
          /\ nextIndex' = [nextIndex EXCEPT ![i][j] =
                               Max({nextIndex[i][j] - 1, 1})]
          /\ UNCHANGED matchIndex
    /\ Discard(m)
    /\ UNCHANGED <<serverVars, candidateVars, logVars, kvsVars, elections, allLogs>>

\* 丢弃过时响应
DropStaleResponse(i, j, m) ==
    /\ m.mterm < currentTerm[i]
    /\ Discard(m)
    /\ UNCHANGED <<serverVars, candidateVars, leaderVars, logVars, kvsVars, elections, allLogs>>

\* 接收消息
Receive(m) ==
    LET i == m.mdest
        j == m.msource
    IN \/ UpdateTerm(i, j, m)
       \/ /\ m.mtype = RequestVoteRequest
          /\ HandleRequestVoteRequest(i, j, m)
       \/ /\ m.mtype = RequestVoteResponse
          /\ \/ DropStaleResponse(i, j, m)
             \/ HandleRequestVoteResponse(i, j, m)
       \/ /\ m.mtype = AppendEntriesRequest
          /\ HandleAppendEntriesRequest(i, j, m)
       \/ /\ m.mtype = AppendEntriesResponse
          /\ \/ DropStaleResponse(i, j, m)
             \/ HandleAppendEntriesResponse(i, j, m)

\* 网络消息重复
DuplicateMessage(m) ==
    /\ Send(m)
    /\ UNCHANGED <<serverVars, candidateVars, leaderVars, logVars, kvsVars, elections, allLogs>>

\* 网络消息丢失
DropMessage(m) ==
    /\ Discard(m)
    /\ UNCHANGED <<serverVars, candidateVars, leaderVars, logVars, kvsVars, elections, allLogs>>

----
\* 状态转换定义
Next == /\ \/ \E i \in Server : Restart(i)
           \/ \E i \in Server : Timeout(i)
           \/ \E i,j \in Server : RequestVote(i, j)
           \/ \E i \in Server : BecomeLeader(i)
           \/ \E i \in Server, k \in Value, v \in Value : ClientRequest(i, k, v)
           \/ \E i \in Server : ApplyToStateMachine(i)
           \/ \E i,j \in Server : AppendEntries(i, j)
           \/ \E m \in DOMAIN messages : Receive(m)
           \/ \E m \in DOMAIN messages : DuplicateMessage(m)
           \/ \E m \in DOMAIN messages : DropMessage(m)
        /\ allLogs' = allLogs \cup {log[i] : i \in Server}

\* 系统规范
Spec == Init /\ [][Next]_vars

----
\* 不变式

\* 选举安全性：每个任期最多一个Leader
ElectionSafety == 
    \A i,j \in Server : 
        /\ state[i] = Leader 
        /\ state[j] = Leader 
        /\ currentTerm[i] = currentTerm[j] 
        => i = j

\* 日志匹配性：不同服务器的日志在相同索引处具有相同任期时，条目相同
LogMatching == 
    \A i,j \in Server : \A k \in 1..Min({Len(log[i]), Len(log[j])}) :
        log[i][k].term = log[j][k].term => log[i][k] = log[j][k]

\* KVS-Raft特有不变式：Offset正确性
OffsetCorrectness ==
    \A i \in Server : \A k \in DOMAIN offsetMap[i] :
        /\ offsetMap[i][k] > 0
        /\ offsetMap[i][k] <= Len(valueLog[i])

\* KVS-Raft特有不变式：值一致性
\* 通过commitIndex确保的一致性：已提交的条目在所有节点上一致
ValueConsistency ==
    \A i,j \in Server : 
        \A idx \in 1..Min({commitIndex[i], commitIndex[j]}) :
            /\ idx <= Len(log[i]) 
            /\ idx <= Len(log[j])
            => log[i][idx] = log[j][idx]

\* 综合安全属性
Safety == ElectionSafety /\ LogMatching /\ OffsetCorrectness /\ ValueConsistency

================================================================================