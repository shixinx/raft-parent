package raft.core.log;

import com.google.common.eventbus.EventBus;
import raft.core.log.entry.Entry;
import raft.core.log.entry.EntryMeta;
import raft.core.log.sequence.EntrySequence;
import raft.core.log.sequence.FileEntrySequence;
import raft.core.log.sequence.LogException;
import raft.core.log.snapshot.*;
import raft.core.node.NodeEndpoint;
import raft.core.rpc.message.InstallSnapshotRpc;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;

@NotThreadSafe
public class FileLog extends AbstractLog {

    private final RootDir rootDir;

    public FileLog(File baseDir, EventBus eventBus) {
        super(eventBus);
        rootDir = new RootDir(baseDir);
        // 获取最新的日志代
        LogGeneration latestGeneration = rootDir.getLatestGeneration();
        snapshot = new EmptySnapshot();
        // TODO add log
        if (latestGeneration != null) {
            // 日志存在
            if (latestGeneration.getSnapshotFile().exists()) {
                snapshot = new FileSnapshot(latestGeneration);
            }
            FileEntrySequence fileEntrySequence = new FileEntrySequence(latestGeneration, snapshot.getLastIncludedIndex() + 1);
            commitIndex = fileEntrySequence.getCommitIndex();
            entrySequence = fileEntrySequence;
            // TODO apply last group config entry
            groupConfigEntryList = entrySequence.buildGroupConfigEntryList();
        } else {
            // 日志不存在
            LogGeneration firstGeneration = rootDir.createFirstGeneration();
            entrySequence = new FileEntrySequence(firstGeneration, 1);
        }
    }

    @Override
    protected Snapshot generateSnapshot(EntryMeta lastAppliedEntryMeta, Set<NodeEndpoint> groupConfig) {
        LogDir logDir = rootDir.getLogDirForGenerating();
        try (FileSnapshotWriter snapshotWriter = new FileSnapshotWriter(
                logDir.getSnapshotFile(), lastAppliedEntryMeta.getIndex(), lastAppliedEntryMeta.getTerm(), groupConfig)) {
            stateMachine.generateSnapshot(snapshotWriter.getOutput());
        } catch (IOException e) {
            throw new LogException("failed to generate snapshot", e);
        }
        return new FileSnapshot(logDir);
    }

    @Override
    protected SnapshotBuilder newSnapshotBuilder(InstallSnapshotRpc firstRpc) {
        return new FileSnapshotBuilder(firstRpc, rootDir.getLogDirForInstalling());
    }

    @Override
    protected void replaceSnapshot(Snapshot newSnapshot) {
        FileSnapshot fileSnapshot = (FileSnapshot) newSnapshot;
        int lastIncludedIndex = fileSnapshot.getLastIncludedIndex();
        int logIndexOffset = lastIncludedIndex + 1;

        List<Entry> remainingEntries = entrySequence.subView(logIndexOffset);
        EntrySequence newEntrySequence = new FileEntrySequence(fileSnapshot.getLogDir(), logIndexOffset);
        newEntrySequence.append(remainingEntries);
        newEntrySequence.commit(Math.max(commitIndex, lastIncludedIndex));
        newEntrySequence.close();

        snapshot.close();
        entrySequence.close();
        newSnapshot.close();

        LogDir generation = rootDir.rename(fileSnapshot.getLogDir(), lastIncludedIndex);
        snapshot = new FileSnapshot(generation);
        entrySequence = new FileEntrySequence(generation, logIndexOffset);
        groupConfigEntryList = entrySequence.buildGroupConfigEntryList();
        commitIndex = entrySequence.getCommitIndex();
    }

}
