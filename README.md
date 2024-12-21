# raft-parent
For learning raft

### 3.4.1 需要持久化的状态数据
1. currentTerm 当前纪元
2. votedFor 投票过给谁
3. log[] 日志条目

### 3.4.2 服务器可变状态数据
每台服务器在运行中需要记录的数据（leader服务器）
1. commitIndex 已经提交的最高的日志索引，初始为0
2. lastApplied 已应用的最高的日志索引，初始为0
一开始都是0，需要注意lastApplied<=commitIndex,算法中的commitIndex推进时候，lastApplied会一同增加

### 3.4.3 服务器可变状态数据
非leader服务器有以下可变状态数据
1. Follower角色服务器的leaderId，当前leader服务器的成员id
2. Candidate角色服务器的votesCount，即作为候选人收到的票数
3. 不管是什么角色，服务器都需要知道一个状态数据：当前服务器的角色role

### 3.4.4 Leader服务器可变状态数据
Leader服务器需要记录的数据如下
1. nextIndex[]:Follower服务器的下一个要复制的日志索引，推断类型为整数数组，刚开始时每个元素（nextIndex）为本地日志副本最后的日志索引+1
2. matchIndex[]:Follower服务器和Leader服务器相匹配的日志索引，推断类型为整数数组，刚开始时每个元素matchIndex为0













