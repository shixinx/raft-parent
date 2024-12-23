package raft.core.node.role;


import raft.core.node.NodeId;

public abstract class AbstractNodeRole {

    //当前角色名称
    private final RoleName name;
    protected final int term;

    AbstractNodeRole(RoleName name, int term) {
        this.name = name;
        this.term = term;
    }

    public RoleName getName() {
        return name;
    }

    public int getTerm() {
        return term;
    }

    public RoleNameAndLeaderId getNameAndLeaderId(NodeId selfId) {
        return new RoleNameAndLeaderId(name, getLeaderId(selfId));
    }

    public abstract NodeId getLeaderId(NodeId selfId);

    //取消超时或者定时任务
    public abstract void cancelTimeoutOrTask();

    public abstract RoleState getState();

    public boolean stateEquals(AbstractNodeRole that) {
        if (this.name != that.name || this.term != that.term) {
            return false;
        }
        return doStateEquals(that);
    }

    protected abstract boolean doStateEquals(AbstractNodeRole role);

}