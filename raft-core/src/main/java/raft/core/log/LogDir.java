package raft.core.log;

import java.io.File;

public interface LogDir {

    //初始化目录
    void initialize();

    //是否存在
    boolean exists();

    File getSnapshotFile();

    //获取EntryIndexFile
    File getEntriesFile();

    File getEntryOffsetIndexFile();

    //获取目录
    File get();

    //重命名目录
    boolean renameTo(LogDir logDir);
}
