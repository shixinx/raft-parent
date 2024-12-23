package raft.core.node;

import raft.core.node.role.RoleState;

import javax.annotation.Nonnull;

public interface NodeRoleListener {

    /**
     * Called when node role changes. e.g FOLLOWER to CANDIDATE.
     *
     * @param roleState role state
     */
    void nodeRoleChanged(@Nonnull RoleState roleState);
}
