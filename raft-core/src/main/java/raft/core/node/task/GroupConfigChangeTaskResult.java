package raft.core.node.task;

public enum GroupConfigChangeTaskResult {

    OK,
    TIMEOUT,
    REPLICATION_FAILED,
    ERROR
}
