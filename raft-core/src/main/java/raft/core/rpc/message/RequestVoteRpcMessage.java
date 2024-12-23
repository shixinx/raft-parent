package raft.core.rpc.message;


import raft.core.node.NodeId;
import raft.core.rpc.Channel;
import raft.core.rpc.RequestVoteRpc;

public class RequestVoteRpcMessage extends AbstractRpcMessage<RequestVoteRpc> {

    public RequestVoteRpcMessage(RequestVoteRpc rpc, NodeId sourceNodeId, Channel channel) {
        super(rpc, sourceNodeId, channel);
    }

}