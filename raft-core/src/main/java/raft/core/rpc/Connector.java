package raft.core.rpc;

import raft.core.node.NodeEndpoint;
import raft.core.rpc.message.*;

import javax.annotation.Nonnull;
import java.util.Collection;

/**
 * Connector.
 */
public interface Connector {

    /**
     * Initialize connector. 初始化
     * <p>
     * SHOULD NOT call more than one.
     * </p>
     */
    void initialize();

    /**
     * Send request vote rpc. 发送RequestVoteRpc给单个节点
     * <p>
     * Remember to exclude self node before sending.
     * </p>
     * <p>
     * Do nothing if destination endpoints is empty.
     * </p>
     *
     * @param rpc                  rpc
     * @param destinationEndpoints destination endpoints
     */
    void sendRequestVote(@Nonnull RequestVoteRpc rpc, @Nonnull Collection<NodeEndpoint> destinationEndpoints);

    /**
     * Reply request vote result. 回复RequestVoteRpc给单个节点
     *
     * @param result     result
     * @param rpcMessage rpc message
     */
    void replyRequestVote(@Nonnull RequestVoteResult result, @Nonnull RequestVoteRpcMessage rpcMessage);

    /**
     * Send append entries rpc. 发送AppendEntries消息给单个节点
     *
     * @param rpc                 rpc
     * @param destinationEndpoint destination endpoint
     */
    void sendAppendEntries(@Nonnull AppendEntriesRpc rpc, @Nonnull NodeEndpoint destinationEndpoint);

    /**
     * Reply append entries result. 回AppendEntries消息给单个节点
     *
     * @param result result
     * @param rpcMessage rpc message
     */
    void replyAppendEntries(@Nonnull AppendEntriesResult result, @Nonnull AppendEntriesRpcMessage rpcMessage);

    /**
     * Send install snapshot rpc.
     *
     * @param rpc rpc
     * @param destinationEndpoint destination endpoint
     */
    void sendInstallSnapshot(@Nonnull InstallSnapshotRpc rpc, @Nonnull NodeEndpoint destinationEndpoint);

    /**
     * Reply install snapshot result.
     *
     * @param result result
     * @param rpcMessage rpc message
     */
    void replyInstallSnapshot(@Nonnull InstallSnapshotResult result, @Nonnull InstallSnapshotRpcMessage rpcMessage);

    /**
     * Called when node becomes leader.
     * <p>
     * Connector may use this chance to close inbound channels.
     * </p>
     */
    void resetChannels();

    /**
     * Close connector.
     */
    void close();

}