package raft.core.log.snapshot;

public class SnapshotChunk {

    private final byte[] bytes;
    private final boolean lastChunk;

    public SnapshotChunk(byte[] bytes, boolean lastChunk) {
        this.bytes = bytes;
        this.lastChunk = lastChunk;
    }

    public boolean isLastChunk() {
        return lastChunk;
    }

    public byte[] toByteArray() {
        return bytes;
    }
}
