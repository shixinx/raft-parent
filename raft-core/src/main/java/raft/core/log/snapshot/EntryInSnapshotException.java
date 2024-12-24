package raft.core.log.snapshot;

import raft.core.log.sequence.LogException;

public class EntryInSnapshotException extends LogException {

    private final int index;

    public EntryInSnapshotException(int index) {
        this.index = index;
    }

    public int getIndex() {
        return index;
    }

}