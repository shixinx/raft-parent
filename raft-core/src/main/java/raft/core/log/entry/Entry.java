package raft.core.log.entry;

public interface Entry {

    // 日志条目类型
    int KIND_NO_OP = 0;
    int KIND_GENERAL = 1;
    int KIND_ADD_NODE = 3;
    int KIND_REMOVE_NODE = 4;

    //获取类型
    int getKind();

    //获取索引
    int getIndex();
    //获取term
    int getTerm();
    //获取元数据信息
    EntryMeta getMeta();
    //获取日志负载
    byte[] getCommandBytes();
}
