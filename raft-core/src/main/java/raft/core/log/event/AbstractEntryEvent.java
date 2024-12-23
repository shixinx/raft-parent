package raft.core.log.event;

import raft.core.log.entry.Entry;

abstract class AbstractEntryEvent<T extends Entry> {

    protected final T entry;

    AbstractEntryEvent(T entry) {
        this.entry = entry;
    }

    public T getEntry() {
        return entry;
    }

}