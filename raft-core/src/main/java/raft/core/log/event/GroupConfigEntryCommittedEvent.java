package raft.core.log.event;


import raft.core.log.entry.GroupConfigEntry;

public class GroupConfigEntryCommittedEvent extends AbstractEntryEvent<GroupConfigEntry> {

    public GroupConfigEntryCommittedEvent(GroupConfigEntry entry) {
        super(entry);
    }

}