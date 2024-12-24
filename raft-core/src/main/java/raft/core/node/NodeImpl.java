package raft.core.node;

import com.google.common.base.Preconditions;
import com.google.common.eventbus.DeadEvent;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.core.log.InstallSnapshotState;
import raft.core.log.entry.Entry;
import raft.core.log.entry.EntryMeta;
import raft.core.log.entry.GroupConfigEntry;
import raft.core.log.entry.RemoveNodeEntry;
import raft.core.log.event.GroupConfigEntryBatchRemovedEvent;
import raft.core.log.event.GroupConfigEntryCommittedEvent;
import raft.core.log.event.GroupConfigEntryFromLeaderAppendEvent;
import raft.core.log.event.SnapshotGenerateEvent;
import raft.core.log.snapshot.EntryInSnapshotException;
import raft.core.log.statemachine.StateMachine;
import raft.core.node.role.*;
import raft.core.node.store.NodeStore;
import raft.core.node.task.*;
import raft.core.rpc.AppendEntriesResult;
import raft.core.rpc.AppendEntriesRpc;
import raft.core.rpc.RequestVoteResult;
import raft.core.rpc.RequestVoteRpc;
import raft.core.rpc.message.*;
import raft.core.schedule.ElectionTimeout;
import raft.core.schedule.LogReplicationTask;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

/**
 * Node implementation.
 *
 * @see NodeContext
 */
@ThreadSafe
public class NodeImpl implements Node {

    private static final Logger logger = LoggerFactory.getLogger(NodeImpl.class);

    // callback for async tasks.
    private static final FutureCallback<Object> LOGGING_FUTURE_CALLBACK = new FutureCallback<Object>() {
        @Override
        public void onSuccess(@Nullable Object result) {
        }

        @Override
        public void onFailure(@Nonnull Throwable t) {
            logger.warn("failure", t);
        }
    };

    //核心上下文组件
    private final NodeContext context;
    //是否已经启动
    @GuardedBy("this")
    private boolean started;
    //当前角色信息
    private volatile AbstractNodeRole role;
    private final List<NodeRoleListener> roleListeners = new CopyOnWriteArrayList<>();

    // NewNodeCatchUpTask and GroupConfigChangeTask related
    private final NewNodeCatchUpTaskContext newNodeCatchUpTaskContext = new NewNodeCatchUpTaskContextImpl();
    private final NewNodeCatchUpTaskGroup newNodeCatchUpTaskGroup = new NewNodeCatchUpTaskGroup();
    private final GroupConfigChangeTaskContext groupConfigChangeTaskContext = new GroupConfigChangeTaskContextImpl();
    private volatile GroupConfigChangeTaskHolder groupConfigChangeTaskHolder = new GroupConfigChangeTaskHolder();

    /**
     * Create with context.
     *
     * @param context context
     */
    NodeImpl(NodeContext context) {
        this.context = context;
    }

    /**
     * Get context.
     *
     * @return context
     */
    NodeContext getContext() {
        return context;
    }

    @Override
    public synchronized void registerStateMachine(@Nonnull StateMachine stateMachine) {
        Preconditions.checkNotNull(stateMachine);
        context.log().setStateMachine(stateMachine);
    }

    @Override
    @Nonnull
    public RoleNameAndLeaderId getRoleNameAndLeaderId() {
        return role.getNameAndLeaderId(context.selfId());
    }

    /**
     * Get role state.
     *
     * @return role state
     */
    @Nonnull
    RoleState getRoleState() {
        return role.getState();
    }

    @Override
    public void addNodeRoleListener(@Nonnull NodeRoleListener listener) {
        Preconditions.checkNotNull(listener);
        roleListeners.add(listener);
    }

    @Override
    public synchronized void start() {
        if (started) {
            return;
        }
        //注册到自己的eventBus
        context.eventBus().register(this);
        context.connector().initialize();

        // load term, votedFor from store and become follower
        NodeStore store = context.store();
        changeToRole(new FollowerNodeRole(store.getTerm(), store.getVotedFor(), null, scheduleElectionTimeout()));
        started = true;
    }

    @Override
    public void appendLog(@Nonnull byte[] commandBytes) {
        Preconditions.checkNotNull(commandBytes);
        ensureLeader();
        context.taskExecutor().submit(() -> {
            context.log().appendEntry(role.getTerm(), commandBytes);
            doReplicateLog();
        }, LOGGING_FUTURE_CALLBACK);
    }

    @Override
    @Nonnull
    public GroupConfigChangeTaskReference addNode(@Nonnull NodeEndpoint endpoint) {
        Preconditions.checkNotNull(endpoint);
        ensureLeader();

        // self cannot be added
        if (context.selfId().equals(endpoint.getId())) {
            throw new IllegalArgumentException("new node cannot be self");
        }

        NewNodeCatchUpTask newNodeCatchUpTask = new NewNodeCatchUpTask(newNodeCatchUpTaskContext, endpoint, context.config());

        // task for node exists
        if (!newNodeCatchUpTaskGroup.add(newNodeCatchUpTask)) {
            throw new IllegalArgumentException("node " + endpoint.getId() + " is adding");
        }

        // catch up new server
        // this will be run in caller thread
        NewNodeCatchUpTaskResult newNodeCatchUpTaskResult;
        try {
            newNodeCatchUpTaskResult = newNodeCatchUpTask.call();
            switch (newNodeCatchUpTaskResult.getState()) {
                case REPLICATION_FAILED:
                    return new FixedResultGroupConfigTaskReference(GroupConfigChangeTaskResult.REPLICATION_FAILED);
                case TIMEOUT:
                    return new FixedResultGroupConfigTaskReference(GroupConfigChangeTaskResult.TIMEOUT);
            }
        } catch (Exception e) {
            if (!(e instanceof InterruptedException)) {
                logger.warn("failed to catch up new node " + endpoint.getId(), e);
            }
            return new FixedResultGroupConfigTaskReference(GroupConfigChangeTaskResult.ERROR);
        }

        // new server caught up
        // wait for previous group config change
        // it will wait forever by default, but you can change to fixed timeout by setting in NodeConfig
        GroupConfigChangeTaskResult result = awaitPreviousGroupConfigChangeTask();
        if (result != null) {
            return new FixedResultGroupConfigTaskReference(result);
        }

        // submit group config change task
        synchronized (this) {

            // it will happen when try to add two or more nodes at the same time
            if (!groupConfigChangeTaskHolder.isEmpty()) {
                throw new IllegalStateException("group config change concurrently");
            }

            AddNodeTask addNodeTask = new AddNodeTask(groupConfigChangeTaskContext, endpoint, newNodeCatchUpTaskResult);
            Future<GroupConfigChangeTaskResult> future = context.groupConfigChangeTaskExecutor().submit(addNodeTask);
            GroupConfigChangeTaskReference reference = new FutureGroupConfigChangeTaskReference(future);
            groupConfigChangeTaskHolder = new GroupConfigChangeTaskHolder(addNodeTask, reference);
            return reference;
        }
    }

    /**
     * Await previous group config change task.
     *
     * @return {@code null} if previous task done, otherwise error or timeout
     * @see GroupConfigChangeTaskResult#ERROR
     * @see GroupConfigChangeTaskResult#TIMEOUT
     */
    @Nullable
    private GroupConfigChangeTaskResult awaitPreviousGroupConfigChangeTask() {
        try {
            groupConfigChangeTaskHolder.awaitDone(context.config().getPreviousGroupConfigChangeTimeout());
            return null;
        } catch (InterruptedException ignored) {
            return GroupConfigChangeTaskResult.ERROR;
        } catch (TimeoutException ignored) {
            logger.info("previous cannot complete within timeout");
            return GroupConfigChangeTaskResult.TIMEOUT;
        }
    }

    /**
     * Ensure leader status
     *
     * @throws NotLeaderException if not leader
     */
    private void ensureLeader() {
        RoleNameAndLeaderId result = role.getNameAndLeaderId(context.selfId());
        if (result.getRoleName() == RoleName.LEADER) {
            return;
        }
        NodeEndpoint endpoint = result.getLeaderId() != null ? context.group().findMember(result.getLeaderId()).getEndpoint() : null;
        throw new NotLeaderException(result.getRoleName(), endpoint);
    }

    @Override
    @Nonnull
    public GroupConfigChangeTaskReference removeNode(@Nonnull NodeId id) {
        Preconditions.checkNotNull(id);
        ensureLeader();

        // await previous group config change task
        GroupConfigChangeTaskResult result = awaitPreviousGroupConfigChangeTask();
        if (result != null) {
            return new FixedResultGroupConfigTaskReference(result);
        }

        // submit group config change task
        synchronized (this) {

            // it will happen when try to remove two or more nodes at the same time
            if (!groupConfigChangeTaskHolder.isEmpty()) {
                throw new IllegalStateException("group config change concurrently");
            }

            RemoveNodeTask task = new RemoveNodeTask(groupConfigChangeTaskContext, id);
            Future<GroupConfigChangeTaskResult> future = context.groupConfigChangeTaskExecutor().submit(task);
            GroupConfigChangeTaskReference reference = new FutureGroupConfigChangeTaskReference(future);
            groupConfigChangeTaskHolder = new GroupConfigChangeTaskHolder(task, reference);
            return reference;
        }
    }

    /**
     * Cancel current group config change task
     */
    synchronized void cancelGroupConfigChangeTask() {
        if (groupConfigChangeTaskHolder.isEmpty()) {
            return;
        }
        logger.info("cancel group config change task");
        groupConfigChangeTaskHolder.cancel();
        groupConfigChangeTaskHolder = new GroupConfigChangeTaskHolder();
    }

    /**
     * Election timeout
     * <p>
     * Source: scheduler
     * </p>
     */
    void electionTimeout() {
        context.taskExecutor().submit(this::doProcessElectionTimeout, LOGGING_FUTURE_CALLBACK);
    }

    private void doProcessElectionTimeout() {
        // leader不会选举
        if (role.getName() == RoleName.LEADER) {
            logger.warn("node {}, current role is leader, ignore election timeout", context.selfId());
            return;
        }
        //当前节点不是leader节点，就开始发起选举。follower节点时发起选举，candidate节点再次发起选举
        // follower: start election
        // candidate: restart election
        int newTerm = role.getTerm() + 1;
        role.cancelTimeoutOrTask();

        if (context.group().isStandalone()) {
            if (context.mode() == NodeMode.STANDBY) {
                logger.info("starts with standby mode, skip election");
            } else {

                // become leader
                logger.info("become leader, term {}", newTerm);
                resetReplicatingStates();
                changeToRole(new LeaderNodeRole(newTerm, scheduleLogReplicationTask()));
                context.log().appendEntry(newTerm); // no-op log
            }
        } else {
            logger.info("start election");
            //变成candidate角色
            changeToRole(new CandidateNodeRole(newTerm, scheduleElectionTimeout()));

            // request vote，发送RequestVote消息
            EntryMeta lastEntryMeta = context.log().getLastEntryMeta();
            RequestVoteRpc rpc = new RequestVoteRpc();
            rpc.setTerm(newTerm);
            rpc.setCandidateId(context.selfId());
            rpc.setLastLogIndex(lastEntryMeta.getIndex());
            rpc.setLastLogTerm(lastEntryMeta.getTerm());
            context.connector().sendRequestVote(rpc, context.group().listEndpointOfMajorExceptSelf());
        }
    }

    /**
     * Become follower.
     *
     * @param term                    term
     * @param votedFor                voted for
     * @param leaderId                leader id
     * @param scheduleElectionTimeout schedule election timeout or not
     */
    private void becomeFollower(int term, NodeId votedFor, NodeId leaderId, boolean scheduleElectionTimeout) {
        role.cancelTimeoutOrTask();//取消超时或者定时器
        if (leaderId != null && !leaderId.equals(role.getLeaderId(context.selfId()))) {
            logger.info("current leader is {}, term {}", leaderId, term);
        }
        //todo 重新创建选举超时定时器或者空定时器
        ElectionTimeout electionTimeout = scheduleElectionTimeout ? scheduleElectionTimeout() : ElectionTimeout.NONE;
        changeToRole(new FollowerNodeRole(term, votedFor, leaderId, electionTimeout));
    }

    /**
     * Change role.
     *
     * @param newRole new role
     */
    private void changeToRole(AbstractNodeRole newRole) {
        if (!isStableBetween(role, newRole)) {
            logger.debug("node {}, role state changed -> {}", context.selfId(), newRole);
            RoleState state = newRole.getState();

            // update store
            NodeStore store = context.store();
            store.setTerm(state.getTerm());
            store.setVotedFor(state.getVotedFor());

            // notify listeners
            roleListeners.forEach(l -> l.nodeRoleChanged(state));
        }
        role = newRole;
    }

    /**
     * Check if stable between two roles.
     * <p>
     * It is stable when role name not changed and role state except timeout/task not change.
     * </p>
     * <p>
     * If role state except timeout/task not changed, it should not update store or notify listeners.
     * </p>
     *
     * @param before role before
     * @param after  role after
     * @return true if stable, otherwise false
     * @see AbstractNodeRole#stateEquals(AbstractNodeRole)
     */
    private boolean isStableBetween(AbstractNodeRole before, AbstractNodeRole after) {
        assert after != null;
        return before != null && before.stateEquals(after);
    }

    /**
     * Schedule election timeout.
     *
     * @return election timeout
     */
    private ElectionTimeout scheduleElectionTimeout() {
        return context.scheduler().scheduleElectionTimeout(this::electionTimeout);
    }

    /**
     * Reset replicating states.
     */
    private void resetReplicatingStates() {
        context.group().resetReplicatingStates(context.log().getNextIndex());
    }

    /**
     * Schedule log replication task.
     *
     * @return log replication task
     */
    private LogReplicationTask scheduleLogReplicationTask() {
        return context.scheduler().scheduleLogReplicationTask(this::replicateLog);
    }

    /**
     * note 成为leader节点后，必须立即发送心跳消息给其他Follower节点，重置这些Follower节点的选举超时，使得集群关系稳定下来。
     * Replicate log. 成为Leader节点后的心跳消息
     * <p>
     * Source: scheduler.
     * </p>
     */
    void replicateLog() {
        context.taskExecutor().submit(this::doReplicateLog, LOGGING_FUTURE_CALLBACK);
    }

    /**
     * Replicate log to other nodes.
     */
    private void doReplicateLog() {
        // just advance commit index if is unique node
        if (context.group().isStandalone()) {
            context.log().advanceCommitIndex(context.log().getNextIndex() - 1, role.getTerm());
            return;
        }
        logger.debug("replicate log");
        for (GroupMember member : context.group().listReplicationTarget()) {
            if (member.shouldReplicate(context.config().getLogReplicationReadTimeout())) {
                doReplicateLog(member, context.config().getMaxReplicationEntries());
            } else {
                logger.debug("node {} is replicating, skip replication task", member.getId());
            }
        }
    }

    /**
     * Replicate log to specified node.
     * <p>
     * Normally it will send append entries rpc to node. And change to install snapshot rpc if entry in snapshot.
     * </p>
     *
     * @param member     node
     * @param maxEntries max entries
     * @see EntryInSnapshotException
     */
    private void doReplicateLog(GroupMember member, int maxEntries) {
        member.replicateNow();
        try {
            AppendEntriesRpc rpc = context.log().createAppendEntriesRpc(role.getTerm(), context.selfId(), member.getNextIndex(), maxEntries);
            context.connector().sendAppendEntries(rpc, member.getEndpoint());
        } catch (EntryInSnapshotException ignored) {
            logger.debug("log entry {} in snapshot, replicate with install snapshot RPC", member.getNextIndex());
            InstallSnapshotRpc rpc = context.log().createInstallSnapshotRpc(role.getTerm(), context.selfId(), 0, context.config().getSnapshotDataLength());
            context.connector().sendInstallSnapshot(rpc, member.getEndpoint());
        }
    }

    /**
     * Receive request vote rpc. 收到投票消息
     * <p>
     * Source: connector.
     * </p>
     *
     * @param rpcMessage rpc message
     */
    @Subscribe
    public void onReceiveRequestVoteRpc(RequestVoteRpcMessage rpcMessage) {
        context.taskExecutor().submit(
                () -> context.connector().replyRequestVote(
                        doProcessRequestVoteRpc(rpcMessage),
                        rpcMessage),
                LOGGING_FUTURE_CALLBACK
        );
    }

    private RequestVoteResult doProcessRequestVoteRpc(RequestVoteRpcMessage rpcMessage) {

        // skip non-major node, it maybe removed node
        if (!context.group().isMemberOfMajor(rpcMessage.getSourceNodeId())) {
            logger.warn("receive request vote rpc from node {} which is not major node, ignore", rpcMessage.getSourceNodeId());
            return new RequestVoteResult(role.getTerm(), false);
        }

        // reply current term if result's term is smaller than current one
        // 需要比较term需要大于等于当前term
        RequestVoteRpc rpc = rpcMessage.get();
        if (rpc.getTerm() < role.getTerm()) {
            logger.debug("term from rpc < current term, don't vote ({} < {})", rpc.getTerm(), role.getTerm());
            return new RequestVoteResult(role.getTerm(), false);
        }

        // step down if result's term is larger than current term
        // 如果对象的term比自己大，则切换为Follower角色
        if (rpc.getTerm() > role.getTerm()) {
            boolean voteForCandidate = !context.log().isNewerThan(rpc.getLastLogIndex(), rpc.getLastLogTerm());
            becomeFollower(rpc.getTerm(), (voteForCandidate ? rpc.getCandidateId() : null), null, true);
            return new RequestVoteResult(rpc.getTerm(), voteForCandidate);
        }
        //本地term与消息的term一致
        assert rpc.getTerm() == role.getTerm();
        switch (role.getName()) {
            case FOLLOWER:
                FollowerNodeRole follower = (FollowerNodeRole) role;
                NodeId votedFor = follower.getVotedFor();
                // reply vote granted for
                // 1. not voted and candidate's log is newer than self
                //自己尚未投票，并且对方的日志比自己新
                // 2. voted for candidate
                //自己已经给对方投过票
                if ((votedFor == null && !context.log().isNewerThan(rpc.getLastLogIndex(), rpc.getLastLogTerm())) ||
                        Objects.equals(votedFor, rpc.getCandidateId())) {
                    becomeFollower(role.getTerm(), rpc.getCandidateId(), null, true);
                    return new RequestVoteResult(rpc.getTerm(), true);
                }
                return new RequestVoteResult(role.getTerm(), false);
            case CANDIDATE: // voted for self 已经给自己投过票，所以不会给其他节点投票
            case LEADER:
                return new RequestVoteResult(role.getTerm(), false);
            default:
                throw new IllegalStateException("unexpected node role [" + role.getName() + "]");
        }
    }

    /**
     * Receive request vote result. 收到选举结果
     * <p>
     * Source: connector.
     * </p>
     *
     * @param result result
     */
    @Subscribe
    public void onReceiveRequestVoteResult(RequestVoteResult result) {
        context.taskExecutor().submit(() -> doProcessRequestVoteResult(result), LOGGING_FUTURE_CALLBACK);
    }

    Future<?> processRequestVoteResult(RequestVoteResult result) {
        return context.taskExecutor().submit(() -> doProcessRequestVoteResult(result));
    }

    private void doProcessRequestVoteResult(RequestVoteResult result) {

        // step down if result's term is larger than current term
        if (result.getTerm() > role.getTerm()) {
            // 退化为follower
            becomeFollower(result.getTerm(), null, null, true);
            return;
        } else if (result.getTerm() < role.getTerm()) {
            // ignore stale term
            logger.debug("receive request vote result and result term is smaller, ignore");
            return;
        }

        // check role
        if (role.getName() != RoleName.CANDIDATE) {
            logger.debug("receive request vote result and current role is not candidate, ignore");
            // 如果自己不是candidate，则忽略
            return;
        }

        // do nothing if not vote granted
        if (!result.isVoteGranted()) {
            // 没有投票则忽略
            return;
        }
        // 当前票数
        int currentVotesCount = ((CandidateNodeRole) role).getVotesCount() + 1;
        // 节点数目
        int countOfMajor = context.group().getCountOfMajor();
        logger.debug("votes count {}, major node count {}", currentVotesCount, countOfMajor);
        // 取消选举超时定时器
        role.cancelTimeoutOrTask();
        if (currentVotesCount > countOfMajor / 2) {
            //票数过半
            // become leader
            logger.info("become leader, term {}", role.getTerm());
            resetReplicatingStates();
            changeToRole(new LeaderNodeRole(role.getTerm(), scheduleLogReplicationTask()));
            context.log().appendEntry(role.getTerm()); // no-op log
            context.connector().resetChannels(); // close all inbound channels
        } else {
            // 修改收到的票数，并重新创建选择超时定时器
            // update votes count
            changeToRole(new CandidateNodeRole(role.getTerm(), currentVotesCount, scheduleElectionTimeout()));
        }
    }

    /**
     * Receive append entries rpc.
     * 非leader节点收到来自Leader节点的心跳消息之后，需要重置选举超时，并记录当前Leader节点的id
     * <p>
     * Source: connector.
     * </p>
     *
     * @param rpcMessage rpc message
     */
    @Subscribe
    public void onReceiveAppendEntriesRpc(AppendEntriesRpcMessage rpcMessage) {
        context.taskExecutor().submit(() ->
                        context.connector().replyAppendEntries(
                                doProcessAppendEntriesRpc(rpcMessage),
                                rpcMessage),
                LOGGING_FUTURE_CALLBACK
        );
    }

    private AppendEntriesResult doProcessAppendEntriesRpc(AppendEntriesRpcMessage rpcMessage) {
        AppendEntriesRpc rpc = rpcMessage.get();

        // reply current term if term in rpc is smaller than current term
        if (rpc.getTerm() < role.getTerm()) {
            //如果对方的term比自己小，则回复自己的term
            return new AppendEntriesResult(rpc.getMessageId(), role.getTerm(), false);
        }

        // if term in rpc is larger than current term, step down and append entries
        if (rpc.getTerm() > role.getTerm()) {
            //如果对方term比自己大，则退化为Follower角色
            becomeFollower(rpc.getTerm(), null, rpc.getLeaderId(), true);
            //并且追加日志
            return new AppendEntriesResult(rpc.getMessageId(), rpc.getTerm(), appendEntries(rpc));
        }

        assert rpc.getTerm() == role.getTerm();
        switch (role.getName()) {
            case FOLLOWER:
                //设置leaderId并且重置选举定时器
                // reset election timeout and append entries
                becomeFollower(rpc.getTerm(), ((FollowerNodeRole) role).getVotedFor(), rpc.getLeaderId(), true);
                //追加日志
                return new AppendEntriesResult(rpc.getMessageId(), rpc.getTerm(), appendEntries(rpc));
            case CANDIDATE:
                //note 如果有两个candidate角色，并且另外一个Candidate先成为leader
                // 则当前节点退化为follower角色并且重置选举器
                // more than one candidate but another node won the election
                becomeFollower(rpc.getTerm(), null, rpc.getLeaderId(), true);
                // 追加日志
                return new AppendEntriesResult(rpc.getMessageId(), rpc.getTerm(), appendEntries(rpc));
            case LEADER:
                // leader角色收到请求，打印警告消息
                logger.warn("receive append entries rpc from another leader {}, ignore", rpc.getLeaderId());
                return new AppendEntriesResult(rpc.getMessageId(), rpc.getTerm(), false);
            default:
                throw new IllegalStateException("unexpected node role [" + role.getName() + "]");
        }
    }

    /**
     * Append entries and advance commit index if possible.
     *
     * @param rpc rpc
     * @return {@code true} if log appended, {@code false} if previous log check failed, etc
     */
    private boolean appendEntries(AppendEntriesRpc rpc) {
        boolean result = context.log().appendEntriesFromLeader(rpc.getPrevLogIndex(), rpc.getPrevLogTerm(), rpc.getEntries());
        if (result) {
            context.log().advanceCommitIndex(Math.min(rpc.getLeaderCommit(), rpc.getLastEntryIndex()), rpc.getTerm());
        }
        return result;
    }

    /**
     * Receive append entries result.
     * leader节点收到来自其他节点的append entries响应
     *
     * @param resultMessage result message
     */
    @Subscribe
    public void onReceiveAppendEntriesResult(AppendEntriesResultMessage resultMessage) {
        context.taskExecutor().submit(() -> doProcessAppendEntriesResult(resultMessage), LOGGING_FUTURE_CALLBACK);
    }

    Future<?> processAppendEntriesResult(AppendEntriesResultMessage resultMessage) {
        return context.taskExecutor().submit(() -> doProcessAppendEntriesResult(resultMessage));
    }

    private void doProcessAppendEntriesResult(AppendEntriesResultMessage resultMessage) {
        AppendEntriesResult result = resultMessage.get();

        // step down if result's term is larger than current term
        if (result.getTerm() > role.getTerm()) {
            // 如果对方term比自己大，则退化为follower角色
            becomeFollower(result.getTerm(), null, null, true);
            return;
        }

        // check role
        if (role.getName() != RoleName.LEADER) {
            logger.warn("receive append entries result from node {} but current node is not leader, ignore", resultMessage.getSourceNodeId());
            return;
        }

        // dispatch to new node catch up task by node id
        if (newNodeCatchUpTaskGroup.onReceiveAppendEntriesResult(resultMessage, context.log().getNextIndex())) {
            return;
        }

        NodeId sourceNodeId = resultMessage.getSourceNodeId();
        GroupMember member = context.group().getMember(sourceNodeId);
        if (member == null) {
            logger.info("unexpected append entries result from node {}, node maybe removed", sourceNodeId);
            return;
        }

        AppendEntriesRpc rpc = resultMessage.getRpc();
        if (result.isSuccess()) {
            if (!member.isMajor()) {  // removing node
                if (member.isRemoving()) {
                    logger.debug("node {} is removing, skip", sourceNodeId);
                } else {
                    logger.warn("unexpected append entries result from node {}, not major and not removing", sourceNodeId);
                }
                member.stopReplicating();
                return;
            }

            // peer
            // advance commit index if major of match index changed
            if (member.advanceReplicatingState(rpc.getLastEntryIndex())) {
                context.log().advanceCommitIndex(context.group().getMatchIndexOfMajor(), role.getTerm());
            }

            // node caught up
            if (member.getNextIndex() >= context.log().getNextIndex()) {
                member.stopReplicating();
                return;
            }
        } else {

            // backoff next index if failed to append entries
            if (!member.backOffNextIndex()) {
                logger.warn("cannot back off next index more, node {}", sourceNodeId);
                member.stopReplicating();
                return;
            }
        }

        // replicate log to node immediately other than wait for next log replication
        doReplicateLog(member, context.config().getMaxReplicationEntries());
    }

    /**
     * Receive install snapshot rpc.
     *
     * @param rpcMessage rpc message
     */
    @Subscribe
    public void onReceiveInstallSnapshotRpc(InstallSnapshotRpcMessage rpcMessage) {
        context.taskExecutor().submit(
                () -> context.connector().replyInstallSnapshot(doProcessInstallSnapshotRpc(rpcMessage), rpcMessage),
                LOGGING_FUTURE_CALLBACK
        );
    }

    private InstallSnapshotResult doProcessInstallSnapshotRpc(InstallSnapshotRpcMessage rpcMessage) {
        InstallSnapshotRpc rpc = rpcMessage.get();

        // reply current term if term in rpc is smaller than current term
        if (rpc.getTerm() < role.getTerm()) {
            return new InstallSnapshotResult(role.getTerm());
        }

        // step down if term in rpc is larger than current one
        if (rpc.getTerm() > role.getTerm()) {
            becomeFollower(rpc.getTerm(), null, rpc.getLeaderId(), true);
        }
        InstallSnapshotState state = context.log().installSnapshot(rpc);
        if (state.getStateName() == InstallSnapshotState.StateName.INSTALLED) {
            context.group().updateNodes(state.getLastConfig());
        }
        // TODO role check?
        return new InstallSnapshotResult(rpc.getTerm());
    }

    /**
     * Receive install snapshot result.
     *
     * @param resultMessage result message
     */
    @Subscribe
    public void onReceiveInstallSnapshotResult(InstallSnapshotResultMessage resultMessage) {
        context.taskExecutor().submit(
                () -> doProcessInstallSnapshotResult(resultMessage),
                LOGGING_FUTURE_CALLBACK
        );
    }

    private void doProcessInstallSnapshotResult(InstallSnapshotResultMessage resultMessage) {
        InstallSnapshotResult result = resultMessage.get();

        // step down if result's term is larger than current one
        if (result.getTerm() > role.getTerm()) {
            becomeFollower(result.getTerm(), null, null, true);
            return;
        }

        // check role
        if (role.getName() != RoleName.LEADER) {
            logger.warn("receive install snapshot result from node {} but current node is not leader, ignore", resultMessage.getSourceNodeId());
            return;
        }

        // dispatch to new node catch up task by node id
        if (newNodeCatchUpTaskGroup.onReceiveInstallSnapshotResult(resultMessage, context.log().getNextIndex())) {
            return;
        }

        NodeId sourceNodeId = resultMessage.getSourceNodeId();
        GroupMember member = context.group().getMember(sourceNodeId);
        if (member == null) {
            logger.info("unexpected install snapshot result from node {}, node maybe removed", sourceNodeId);
            return;
        }

        InstallSnapshotRpc rpc = resultMessage.getRpc();
        if (rpc.isDone()) {

            // change to append entries rpc
            member.advanceReplicatingState(rpc.getLastIndex());
            int maxEntries = member.isMajor() ? context.config().getMaxReplicationEntries() : context.config().getMaxReplicationEntriesForNewNode();
            doReplicateLog(member, maxEntries);
        } else {

            // transfer data
            InstallSnapshotRpc nextRpc = context.log().createInstallSnapshotRpc(role.getTerm(), context.selfId(),
                    rpc.getOffset() + rpc.getDataLength(), context.config().getSnapshotDataLength());
            context.connector().sendInstallSnapshot(nextRpc, member.getEndpoint());
        }
    }

    /**
     * Group config from leader appended.
     * <p>
     * Source: log.
     * </p>
     *
     * @param event event
     */
    @Subscribe
    public void onGroupConfigEntryFromLeaderAppend(GroupConfigEntryFromLeaderAppendEvent event) {
        context.taskExecutor().submit(() -> {
            GroupConfigEntry entry = event.getEntry();
            if (entry.getKind() == Entry.KIND_REMOVE_NODE &&
                    context.selfId().equals(((RemoveNodeEntry) entry).getNodeToRemove())) {
                logger.info("current node is removed from group, step down and standby");
                becomeFollower(role.getTerm(), null, null, false);
            }
            context.group().updateNodes(entry.getResultNodeEndpoints());
        }, LOGGING_FUTURE_CALLBACK);
    }

    /**
     * Group config entry committed.
     * <p>
     * Source: log.
     * </p>
     *
     * @param event event
     */
    @Subscribe
    public void onGroupConfigEntryCommitted(GroupConfigEntryCommittedEvent event) {
        context.taskExecutor().submit(
                () -> doProcessGroupConfigEntryCommittedEvent(event),
                LOGGING_FUTURE_CALLBACK
        );
    }

    private void doProcessGroupConfigEntryCommittedEvent(GroupConfigEntryCommittedEvent event) {
        GroupConfigEntry entry = event.getEntry();

        // dispatch to group config change task by node id
        groupConfigChangeTaskHolder.onLogCommitted(entry);
    }

    /**
     * Multiple group configs removed.
     * <p>
     * Source: log.
     * </p>
     *
     * @param event event
     */
    @Subscribe
    public void onGroupConfigEntryBatchRemoved(GroupConfigEntryBatchRemovedEvent event) {
        context.taskExecutor().submit(() -> {
            GroupConfigEntry entry = event.getFirstRemovedEntry();
            context.group().updateNodes(entry.getNodeEndpoints());
        }, LOGGING_FUTURE_CALLBACK);
    }

    /**
     * Generate snapshot.
     * <p>
     * Source: log.
     * </p>
     *
     * @param event event
     */
    @Subscribe
    public void onGenerateSnapshot(SnapshotGenerateEvent event) {
        context.taskExecutor().submit(() -> {
            context.log().generateSnapshot(event.getLastIncludedIndex(), context.group().listEndpointOfMajor());
        }, LOGGING_FUTURE_CALLBACK);
    }

    /**
     * Dead event.
     * <p>
     * Source: event-bus.
     * </p>
     *
     * @param deadEvent dead event
     */
    @Subscribe
    public void onReceiveDeadEvent(DeadEvent deadEvent) {
        logger.warn("dead event {}", deadEvent);
    }

    @Override
    public synchronized void stop() throws InterruptedException {
        if (!started) {
            throw new IllegalStateException("node not started");
        }
        context.scheduler().stop();
        context.log().close();
        context.connector().close();
        context.store().close();
        context.taskExecutor().shutdown();
        context.groupConfigChangeTaskExecutor().shutdown();
        started = false;
    }

    private class NewNodeCatchUpTaskContextImpl implements NewNodeCatchUpTaskContext {

        @Override
        public void replicateLog(NodeEndpoint endpoint) {
            context.taskExecutor().submit(
                    () -> doReplicateLog(endpoint, context.log().getNextIndex()),
                    LOGGING_FUTURE_CALLBACK
            );
        }

        @Override
        public void doReplicateLog(NodeEndpoint endpoint, int nextIndex) {
            try {
                AppendEntriesRpc rpc = context.log().createAppendEntriesRpc(role.getTerm(), context.selfId(), nextIndex, context.config().getMaxReplicationEntriesForNewNode());
                context.connector().sendAppendEntries(rpc, endpoint);
            } catch (EntryInSnapshotException ignored) {

                // change to install snapshot rpc if entry in snapshot
                logger.debug("log entry {} in snapshot, replicate with install snapshot RPC", nextIndex);
                InstallSnapshotRpc rpc = context.log().createInstallSnapshotRpc(role.getTerm(), context.selfId(), 0, context.config().getSnapshotDataLength());
                context.connector().sendInstallSnapshot(rpc, endpoint);
            }
        }

        @Override
        public void sendInstallSnapshot(NodeEndpoint endpoint, int offset) {
            InstallSnapshotRpc rpc = context.log().createInstallSnapshotRpc(role.getTerm(), context.selfId(), offset, context.config().getSnapshotDataLength());
            context.connector().sendInstallSnapshot(rpc, endpoint);
        }

        @Override
        public void done(NewNodeCatchUpTask task) {

            // remove task from group
            newNodeCatchUpTaskGroup.remove(task);
        }
    }

    private class GroupConfigChangeTaskContextImpl implements GroupConfigChangeTaskContext {

        @Override
        public void addNode(NodeEndpoint endpoint, int nextIndex, int matchIndex) {
            context.taskExecutor().submit(() -> {
                context.log().appendEntryForAddNode(role.getTerm(), context.group().listEndpointOfMajor(), endpoint);
                assert !context.selfId().equals(endpoint.getId());
                context.group().addNode(endpoint, nextIndex, matchIndex, true);
                NodeImpl.this.doReplicateLog();
            }, LOGGING_FUTURE_CALLBACK);
        }

        @Override
        public void downgradeNode(NodeId nodeId) {
            context.taskExecutor().submit(() -> {
                context.group().downgrade(nodeId);
                Set<NodeEndpoint> nodeEndpoints = context.group().listEndpointOfMajor();
                context.log().appendEntryForRemoveNode(role.getTerm(), nodeEndpoints, nodeId);
                NodeImpl.this.doReplicateLog();
            }, LOGGING_FUTURE_CALLBACK);
        }

        @Override
        public void removeNode(NodeId nodeId) {
            context.taskExecutor().submit(() -> {
                if (nodeId.equals(context.selfId())) {
                    logger.info("remove self from group, step down and standby");
                    becomeFollower(role.getTerm(), null, null, false);
                }
                context.group().removeNode(nodeId);
            }, LOGGING_FUTURE_CALLBACK);
        }

        @Override
        public void done() {

            // clear current group config change
            synchronized (NodeImpl.this) {
                groupConfigChangeTaskHolder = new GroupConfigChangeTaskHolder();
            }
        }

    }

}
