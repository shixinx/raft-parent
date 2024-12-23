package raft.core.log.sequence;


import raft.core.log.entry.Entry;
import raft.core.log.entry.EntryMeta;

import java.util.Collections;
import java.util.List;

abstract class AbstractEntrySequence implements EntrySequence {

    int logIndexOffset;
    int nextLogIndex;

    //构造函数 默认为空日志条目序列
    AbstractEntrySequence(int logIndexOffset) {
        this.logIndexOffset = logIndexOffset;
        this.nextLogIndex = logIndexOffset;
    }

    //判断是否为空
    @Override
    public boolean isEmpty() {
        return logIndexOffset == nextLogIndex;
    }

    //获取第一条日志的索引，为空抛异常
    @Override
    public int getFirstLogIndex() {
        if (isEmpty()) {
            throw new EmptySequenceException();
        }
        return doGetFirstLogIndex();
    }

    //获取日志索引偏移
    int doGetFirstLogIndex() {
        return logIndexOffset;
    }

    //获取最后一条的索引，为空抛异常
    @Override
    public int getLastLogIndex() {
        if (isEmpty()) {
            throw new EmptySequenceException();
        }
        return doGetLastLogIndex();
    }

    //最后一条日志的索引
    int doGetLastLogIndex() {
        return nextLogIndex - 1;
    }

    @Override
    public boolean isEntryPresent(int index) {
        return !isEmpty() && index >= doGetFirstLogIndex() && index <= doGetLastLogIndex();
    }

    @Override
    public Entry getEntry(int index) {
        if (!isEntryPresent(index)) {
            return null;
        }
        return doGetEntry(index);
    }

    @Override
    public EntryMeta getEntryMeta(int index) {
        Entry entry = getEntry(index);
        return entry != null ? entry.getMeta() : null;
    }

    protected abstract Entry doGetEntry(int index);

    @Override
    public Entry getLastEntry() {
        return isEmpty() ? null : doGetEntry(doGetLastLogIndex());
    }

    //获取一个子视图，不指定结束索引
    @Override
    public List<Entry> subView(int fromIndex) {
        if (isEmpty() || fromIndex > doGetLastLogIndex()) {
            return Collections.emptyList();
        }
        return subList(Math.max(fromIndex, doGetFirstLogIndex()), nextLogIndex);
    }

    // [fromIndex, toIndex)
    //获取一个子视图，指定结束索引
    @Override
    public List<Entry> subList(int fromIndex, int toIndex) {
        if (isEmpty()) {
            throw new EmptySequenceException();
        }
        if (fromIndex < doGetFirstLogIndex() || toIndex > doGetLastLogIndex() + 1 || fromIndex > toIndex) {
            throw new IllegalArgumentException("illegal from index " + fromIndex + " or to index " + toIndex);
        }
        return doSubList(fromIndex, toIndex);
    }

    protected abstract List<Entry> doSubList(int fromIndex, int toIndex);

    @Override
    public int getNextLogIndex() {
        return nextLogIndex;
    }

    //追加多条日志
    @Override
    public void append(List<Entry> entries) {
        for (Entry entry : entries) {
            append(entry);
        }
    }

    //追加1条日志
    @Override
    public void append(Entry entry) {
        if (entry.getIndex() != nextLogIndex) {
            throw new IllegalArgumentException("entry index must be " + nextLogIndex);
        }
        doAppend(entry);
        nextLogIndex++;
    }

    protected abstract void doAppend(Entry entry);

    @Override
    public void removeAfter(int index) {
        if (isEmpty() || index >= doGetLastLogIndex()) {
            return;
        }
        doRemoveAfter(index);
    }

    protected abstract void doRemoveAfter(int index);

}