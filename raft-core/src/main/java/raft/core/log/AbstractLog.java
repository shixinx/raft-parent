package raft.core.log;



import com.google.common.eventbus.EventBus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.core.log.entry.*;
import raft.core.log.event.GroupConfigEntryBatchRemovedEvent;
import raft.core.log.event.GroupConfigEntryCommittedEvent;
import raft.core.log.event.GroupConfigEntryFromLeaderAppendEvent;
import raft.core.log.event.SnapshotGenerateEvent;
import raft.core.log.sequence.EntrySequence;
import raft.core.log.sequence.GroupConfigEntryList;
import raft.core.log.sequence.LogException;
import raft.core.log.snapshot.*;
import raft.core.log.statemachine.EmptyStateMachine;
import raft.core.log.statemachine.StateMachine;
import raft.core.log.statemachine.StateMachineContext;
import raft.core.node.NodeEndpoint;
import raft.core.node.NodeId;
import raft.core.rpc.AppendEntriesRpc;
import raft.core.rpc.message.InstallSnapshotRpc;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.*;

abstract class AbstractLog implements Log {

    private static final Logger logger = LoggerFactory.getLogger(AbstractLog.class);

    protected final EventBus eventBus;
    protected Snapshot snapshot;
    protected EntrySequence entrySequence;

    protected SnapshotBuilder snapshotBuilder = new NullSnapshotBuilder();
    protected GroupConfigEntryList groupConfigEntryList = new GroupConfigEntryList();
    private final StateMachineContext stateMachineContext = new StateMachineContextImpl();
    protected StateMachine stateMachine = new EmptyStateMachine();
    protected int commitIndex = 0;

    AbstractLog(EventBus eventBus) {
        this.eventBus = eventBus;
    }

    //获取最后一条数据的元信息
    @Override
    @Nonnull
    public EntryMeta getLastEntryMeta() {
        if (entrySequence.isEmpty()) {
            return new EntryMeta(Entry.KIND_NO_OP, snapshot.getLastIncludedIndex(), snapshot.getLastIncludedTerm());
        }
        return entrySequence.getLastEntry().getMeta();
    }

    //创建appendEntries消息
    @Override
    public AppendEntriesRpc createAppendEntriesRpc(int term, NodeId selfId, int nextIndex, int maxEntries) {
        int nextLogIndex = entrySequence.getNextLogIndex();
        if (nextIndex > nextLogIndex) {
            throw new IllegalArgumentException("illegal next index " + nextIndex);
        }
        if (nextIndex <= snapshot.getLastIncludedIndex()) {
            throw new EntryInSnapshotException(nextIndex);
        }
        AppendEntriesRpc rpc = new AppendEntriesRpc();
        rpc.setMessageId(UUID.randomUUID().toString());
        rpc.setTerm(term);
        rpc.setLeaderId(selfId);
        rpc.setLeaderCommit(commitIndex);
        if (nextIndex == snapshot.getLastIncludedIndex() + 1) {
            rpc.setPrevLogIndex(snapshot.getLastIncludedIndex());
            rpc.setPrevLogTerm(snapshot.getLastIncludedTerm());
        } else {
            // if entry sequence is empty,
            // snapshot.lastIncludedIndex + 1 == nextLogIndex,
            // so it has been rejected at the first line.
            //
            // if entry sequence is not empty,
            // snapshot.lastIncludedIndex + 1 < nextIndex <= nextLogIndex
            // and snapshot.lastIncludedIndex + 1 = firstLogIndex
            //     nextLogIndex = lastLogIndex + 1
            // then firstLogIndex < nextIndex <= lastLogIndex + 1
            //      firstLogIndex + 1 <= nextIndex <= lastLogIndex + 1
            //      firstLogIndex <= nextIndex - 1 <= lastLogIndex
            // it is ok to get entry without null check
            // 设置前一条日志的元信息，有可能不存在
            Entry entry = entrySequence.getEntry(nextIndex - 1);
            assert entry != null;
            rpc.setPrevLogIndex(entry.getIndex());
            rpc.setPrevLogTerm(entry.getTerm());
        }
        if (!entrySequence.isEmpty()) {
            int maxIndex = (maxEntries == ALL_ENTRIES ? nextLogIndex : Math.min(nextLogIndex, nextIndex + maxEntries));
            rpc.setEntries(entrySequence.subList(nextIndex, maxIndex));
        }
        return rpc;
    }

    @Override
    public InstallSnapshotRpc createInstallSnapshotRpc(int term, NodeId selfId, int offset, int length) {
        InstallSnapshotRpc rpc = new InstallSnapshotRpc();
        rpc.setTerm(term);
        rpc.setLeaderId(selfId);
        rpc.setLastIndex(snapshot.getLastIncludedIndex());
        rpc.setLastTerm(snapshot.getLastIncludedTerm());
        if (offset == 0) {
            rpc.setLastConfig(snapshot.getLastConfig());
        }
        rpc.setOffset(offset);

        SnapshotChunk chunk = snapshot.readData(offset, length);
        rpc.setData(chunk.toByteArray());
        rpc.setDone(chunk.isLastChunk());
        return rpc;
    }

    @Override
    public GroupConfigEntry getLastUncommittedGroupConfigEntry() {
        GroupConfigEntry lastEntry = groupConfigEntryList.getLast();
        return (lastEntry != null && lastEntry.getIndex() > commitIndex) ? lastEntry : null;
    }

    @Override
    public int getNextIndex() {
        return entrySequence.getNextLogIndex();
    }

    @Override
    public int getCommitIndex() {
        return commitIndex;
    }

    // 取出最后一条日志的元信息，先判断term再判断索引
    @Override
    public boolean isNewerThan(int lastLogIndex, int lastLogTerm) {
        EntryMeta lastEntryMeta = getLastEntryMeta();
        logger.debug("last entry ({}, {}), candidate ({}, {})", lastEntryMeta.getIndex(), lastEntryMeta.getTerm(), lastLogIndex, lastLogTerm);
        return lastEntryMeta.getTerm() > lastLogTerm || lastEntryMeta.getIndex() > lastLogIndex;
    }

    // 追加noop日志
    @Override
    public NoOpEntry appendEntry(int term) {
        NoOpEntry entry = new NoOpEntry(entrySequence.getNextLogIndex(), term);
        entrySequence.append(entry);
        return entry;
    }

    //追加一般日志
    @Override
    public GeneralEntry appendEntry(int term, byte[] command) {
        GeneralEntry entry = new GeneralEntry(entrySequence.getNextLogIndex(), term, command);
        entrySequence.append(entry);
        return entry;
    }

    @Override
    public AddNodeEntry appendEntryForAddNode(int term, Set<NodeEndpoint> nodeEndpoints, NodeEndpoint newNodeEndpoint) {
        AddNodeEntry entry = new AddNodeEntry(entrySequence.getNextLogIndex(), term, nodeEndpoints, newNodeEndpoint);
        entrySequence.append(entry);
        groupConfigEntryList.add(entry);
        return entry;
    }

    @Override
    public RemoveNodeEntry appendEntryForRemoveNode(int term, Set<NodeEndpoint> nodeEndpoints, NodeId nodeToRemove) {
        RemoveNodeEntry entry = new RemoveNodeEntry(entrySequence.getNextLogIndex(), term, nodeEndpoints, nodeToRemove);
        entrySequence.append(entry);
        groupConfigEntryList.add(entry);
        return entry;
    }
    // 在追加之前还需要移除不一致的日志条目。移除时从最后一条匹配的日志条目开始，之后所有冲突的日志条目都会被移除
    @Override
    public boolean appendEntriesFromLeader(int prevLogIndex, int prevLogTerm, List<Entry> leaderEntries) {
        // check previous log 检查前一条日志是否匹配
        if (!checkIfPreviousLogMatches(prevLogIndex, prevLogTerm)) {
            return false;
        }
        // heartbeat 从leader节点传递过来的日志条目为空
        if (leaderEntries.isEmpty()) {
            return true;
        }
        assert prevLogIndex + 1 == leaderEntries.get(0).getIndex();
        // 移除冲突的日志条目并且返回接下来要追加的日志条目
        EntrySequenceView newEntries = removeUnmatchedLog(new EntrySequenceView(leaderEntries));
        // 仅仅追加日志
        appendEntriesFromLeader(newEntries);
        return true;
    }

    private void appendEntriesFromLeader(EntrySequenceView leaderEntries) {
        if (leaderEntries.isEmpty()) {
            return;
        }
        logger.debug("append entries from leader from {} to {}", leaderEntries.getFirstLogIndex(), leaderEntries.getLastLogIndex());
        for (Entry leaderEntry : leaderEntries) {
            appendEntryFromLeader(leaderEntry);
        }
    }

    private void appendEntryFromLeader(Entry leaderEntry) {
        entrySequence.append(leaderEntry);
        if (leaderEntry instanceof GroupConfigEntry) {
            eventBus.post(new GroupConfigEntryFromLeaderAppendEvent(
                    (GroupConfigEntry) leaderEntry)
            );
        }
    }

    private EntrySequenceView removeUnmatchedLog(EntrySequenceView leaderEntries) {
        //从leader节点过来的entries不能为空
        assert !leaderEntries.isEmpty();
        //找到第一个不匹配的日志索引
        int firstUnmatched = findFirstUnmatchedLog(leaderEntries);
        //移除不匹配的日志索引开始的所有日志
        removeEntriesAfter(firstUnmatched - 1);
        //返回之后追加的日志条目
        return leaderEntries.subView(firstUnmatched);
    }

    private int findFirstUnmatchedLog(EntrySequenceView leaderEntries) {
        assert !leaderEntries.isEmpty();
        int logIndex;
        EntryMeta followerEntryMeta;
        // 从前往后遍历leaderEntries
        for (Entry leaderEntry : leaderEntries) {
            logIndex = leaderEntry.getIndex();
            //按照索引查找日志条目元信息
            followerEntryMeta = entrySequence.getEntryMeta(logIndex);
            if (followerEntryMeta == null || followerEntryMeta.getTerm() != leaderEntry.getTerm()) {
                return logIndex;
            }
        }
        // 否则没有不一致条目
        return leaderEntries.getLastLogIndex() + 1;
    }

    private boolean checkIfPreviousLogMatches(int prevLogIndex, int prevLogTerm) {
        int lastIncludedIndex = snapshot.getLastIncludedIndex();
        if (prevLogIndex < lastIncludedIndex) {
            logger.debug("previous log index {} < snapshot's last included index {}", prevLogIndex, lastIncludedIndex);
            return false;
        }
        if (prevLogIndex == lastIncludedIndex) {
            int lastIncludedTerm = snapshot.getLastIncludedTerm();
            if (prevLogTerm != lastIncludedTerm) {
                logger.debug("previous log index matches snapshot's last included index, " +
                        "but term not (expected {}, actual {})", lastIncludedTerm, prevLogTerm);
                return false;
            }
            return true;
        }
        //检查指定索引的日志条目
        Entry entry = entrySequence.getEntry(prevLogIndex);
        //日志不存在
        if (entry == null) {
            logger.debug("previous log {} not found", prevLogIndex);
            return false;
        }
        int term = entry.getTerm();
        if (term != prevLogTerm) {
            logger.debug("different term of previous log, local {}, remote {}", term, prevLogTerm);
            return false;
        }
        return true;
    }

    private void removeEntriesAfter(int index) {
        if (entrySequence.isEmpty() || index >= entrySequence.getLastLogIndex()) {
            return;
        }
        int lastApplied = stateMachine.getLastApplied();
        if (index < lastApplied && entrySequence.subList(index + 1, lastApplied + 1).stream().anyMatch(this::isApplicable)) {
            logger.warn("applied log removed, reapply from start");
            applySnapshot(snapshot);
            logger.debug("apply log from {} to {}", entrySequence.getFirstLogIndex(), index);
            entrySequence.subList(entrySequence.getFirstLogIndex(), index + 1).forEach(this::applyEntry);
        }
        //如果移除了已经应用的日志，需要从头开始重新构建状态机
        logger.debug("remove entries after {}", index);
        entrySequence.removeAfter(index);
        if (index < commitIndex) {
            commitIndex = index;
        }
        GroupConfigEntry firstRemovedEntry = groupConfigEntryList.removeAfter(index);
        if (firstRemovedEntry != null) {
            logger.info("group config removed");
            eventBus.post(new GroupConfigEntryBatchRemovedEvent(firstRemovedEntry));
        }
    }

    @Override
    public void advanceCommitIndex(int newCommitIndex, int currentTerm) {
        if (!validateNewCommitIndex(newCommitIndex, currentTerm)) {
            return;
        }
        logger.debug("advance commit index from {} to {}", commitIndex, newCommitIndex);
        entrySequence.commit(newCommitIndex);
        groupConfigsCommitted(newCommitIndex);
        commitIndex = newCommitIndex;

        advanceApplyIndex();
    }

    @Override
    public void generateSnapshot(int lastIncludedIndex, Set<NodeEndpoint> groupConfig) {
        logger.info("generate snapshot, last included index {}", lastIncludedIndex);
        EntryMeta lastAppliedEntryMeta = entrySequence.getEntryMeta(lastIncludedIndex);
        replaceSnapshot(generateSnapshot(lastAppliedEntryMeta, groupConfig));
    }

    private void advanceApplyIndex() {
        // start up and snapshot exists
        int lastApplied = stateMachine.getLastApplied();
        int lastIncludedIndex = snapshot.getLastIncludedIndex();
        if (lastApplied == 0 && lastIncludedIndex > 0) {
            assert commitIndex >= lastIncludedIndex;
            applySnapshot(snapshot);
            lastApplied = lastIncludedIndex;
        }
        for (Entry entry : entrySequence.subList(lastApplied + 1, commitIndex + 1)) {
            applyEntry(entry);
        }
    }

    private void applySnapshot(Snapshot snapshot) {
        logger.debug("apply snapshot, last included index {}", snapshot.getLastIncludedIndex());
        try {
            stateMachine.applySnapshot(snapshot);
        } catch (IOException e) {
            throw new LogException("failed to apply snapshot", e);
        }
    }

    private void applyEntry(Entry entry) {
        // skip no-op entry and membership-change entry
        if (isApplicable(entry)) {
            stateMachine.applyLog(stateMachineContext, entry.getIndex(), entry.getCommandBytes(), entrySequence.getFirstLogIndex());
        }
    }

    private boolean isApplicable(Entry entry) {
        return entry.getKind() == Entry.KIND_GENERAL;
    }

    private void groupConfigsCommitted(int newCommitIndex) {
        for (GroupConfigEntry groupConfigEntry : groupConfigEntryList.subList(commitIndex + 1, newCommitIndex + 1)) {
            eventBus.post(new GroupConfigEntryCommittedEvent(groupConfigEntry));
        }
    }

    private boolean validateNewCommitIndex(int newCommitIndex, int currentTerm) {
        //小于当前的commitIndex
        if (newCommitIndex <= commitIndex) {
            return false;
        }
        Entry entry = entrySequence.getEntry(newCommitIndex);
        if (entry == null) {
            logger.debug("log of new commit index {} not found", newCommitIndex);
            return false;
        }
        // 必须是当前term，才能推进commitIndex
        if (entry.getTerm() != currentTerm) {
            logger.debug("log term of new commit index != current term ({} != {})", entry.getTerm(), currentTerm);
            return false;
        }
        return true;
    }

    protected abstract Snapshot generateSnapshot(EntryMeta lastAppliedEntryMeta, Set<NodeEndpoint> groupConfig);

    @Override
    public InstallSnapshotState installSnapshot(InstallSnapshotRpc rpc) {
        if (rpc.getLastIndex() <= snapshot.getLastIncludedIndex()) {
            logger.debug("snapshot's last included index from rpc <= current one ({} <= {}), ignore",
                    rpc.getLastIndex(), snapshot.getLastIncludedIndex());
            return new InstallSnapshotState(InstallSnapshotState.StateName.ILLEGAL_INSTALL_SNAPSHOT_RPC);
        }
        if (rpc.getOffset() == 0) {
            assert rpc.getLastConfig() != null;
            snapshotBuilder.close();
            snapshotBuilder = newSnapshotBuilder(rpc);
        } else {
            snapshotBuilder.append(rpc);
        }
        if (!rpc.isDone()) {
            return new InstallSnapshotState(InstallSnapshotState.StateName.INSTALLING);
        }
        Snapshot newSnapshot = snapshotBuilder.build();
        applySnapshot(newSnapshot);
        replaceSnapshot(newSnapshot);
        int lastIncludedIndex = snapshot.getLastIncludedIndex();
        if (commitIndex < lastIncludedIndex) {
            commitIndex = lastIncludedIndex;
        }
        return new InstallSnapshotState(InstallSnapshotState.StateName.INSTALLED, newSnapshot.getLastConfig());
    }

    protected abstract SnapshotBuilder newSnapshotBuilder(InstallSnapshotRpc firstRpc);

    protected abstract void replaceSnapshot(Snapshot newSnapshot);

    @Override
    public void setStateMachine(StateMachine stateMachine) {
        this.stateMachine = stateMachine;
    }

    @Override
    public void close() {
        snapshot.close();
        entrySequence.close();
        snapshotBuilder.close();
        stateMachine.shutdown();
    }

    private class StateMachineContextImpl implements StateMachineContext {

        @Override
        public void generateSnapshot(int lastIncludedIndex) {
            eventBus.post(new SnapshotGenerateEvent(lastIncludedIndex));
        }

    }

    /**
     * 方便操作的类，封装日志列表，提供日志索引查询
     */
    private static class EntrySequenceView implements Iterable<Entry> {

        private final List<Entry> entries;
        private int firstLogIndex;
        private int lastLogIndex;

        EntrySequenceView(List<Entry> entries) {
            this.entries = entries;
            if (!entries.isEmpty()) {
                firstLogIndex = entries.get(0).getIndex();
                lastLogIndex = entries.get(entries.size() - 1).getIndex();
            }
        }

        //获取指定位置的日志条目
        Entry get(int index) {
            if (entries.isEmpty() || index < firstLogIndex || index > lastLogIndex) {
                return null;
            }
            return entries.get(index - firstLogIndex);
        }

        boolean isEmpty() {
            return entries.isEmpty();
        }

        int getFirstLogIndex() {
            return firstLogIndex;
        }

        int getLastLogIndex() {
            return lastLogIndex;
        }

        EntrySequenceView subView(int fromIndex) {
            if (entries.isEmpty() || fromIndex > lastLogIndex) {
                return new EntrySequenceView(Collections.emptyList());
            }
            return new EntrySequenceView(
                    entries.subList(fromIndex - firstLogIndex, entries.size())
            );
        }

        @Override
        @Nonnull
        public Iterator<Entry> iterator() {
            return entries.iterator();
        }

    }

}