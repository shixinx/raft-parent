package raft.core.log.event;

import raft.core.log.entry.GroupConfigEntry;

public class GroupConfigEntryFromLeaderAppendEvent extends AbstractEntryEvent<GroupConfigEntry> {

    public GroupConfigEntryFromLeaderAppendEvent(GroupConfigEntry entry) {
        super(entry);
    }

}