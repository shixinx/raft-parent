package raft.core.node.role;

import raft.core.node.NodeId;
import raft.core.schedule.ElectionTimeout;

import javax.annotation.concurrent.Immutable;

@Immutable
public class CandidateNodeRole extends AbstractNodeRole {

    //票数
    private final int votesCount;
    private final ElectionTimeout electionTimeout;

    //构造函数 票数默认为1
    public CandidateNodeRole(int term, ElectionTimeout electionTimeout) {
        this(term, 1, electionTimeout);
    }

    //构造函数 指定票数
    public CandidateNodeRole(int term, int votesCount, ElectionTimeout electionTimeout) {
        super(RoleName.CANDIDATE, term);
        this.votesCount = votesCount;
        this.electionTimeout = electionTimeout;
    }

    public int getVotesCount() {
        return votesCount;
    }

    @Override
    public NodeId getLeaderId(NodeId selfId) {
        return null;
    }

    @Override
    public void cancelTimeoutOrTask() {
        electionTimeout.cancel();
    }

    @Override
    public RoleState getState() {
        DefaultRoleState state = new DefaultRoleState(RoleName.CANDIDATE, term);
        state.setVotesCount(votesCount);
        return state;
    }

    @Override
    protected boolean doStateEquals(AbstractNodeRole role) {
        CandidateNodeRole that = (CandidateNodeRole) role;
        return this.votesCount == that.votesCount;
    }

    @Override
    public String toString() {
        return "CandidateNodeRole{" +
                "term=" + term +
                ", votesCount=" + votesCount +
                ", electionTimeout=" + electionTimeout +
                '}';
    }
}
