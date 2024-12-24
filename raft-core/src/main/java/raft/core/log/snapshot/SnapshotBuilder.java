package raft.core.log.snapshot;

import raft.core.rpc.message.InstallSnapshotRpc;

public interface SnapshotBuilder<T extends Snapshot> {

    void append(InstallSnapshotRpc rpc);

    T build();

    void close();

}
