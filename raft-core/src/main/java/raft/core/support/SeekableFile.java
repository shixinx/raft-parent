package raft.core.support;

import java.io.IOException;
import java.io.InputStream;

public interface SeekableFile {

    //获取当前位置
    long position() throws IOException;

    //移动到指定位置
    void seek(long position) throws IOException;

    //写入整数
    void writeInt(int i) throws IOException;

    //写入长整数
    void writeLong(long l) throws IOException;

    void write(byte[] b) throws IOException;

    int readInt() throws IOException;

    long readLong() throws IOException;

    int read(byte[] b) throws IOException;

    long size() throws IOException;

    //裁剪指定大小
    void truncate(long size) throws IOException;

    //获取从指定位置开始的输入流
    InputStream inputStream(long start) throws IOException;

    //强制刷新到磁盘
    void flush() throws IOException;

    void close() throws IOException;

}