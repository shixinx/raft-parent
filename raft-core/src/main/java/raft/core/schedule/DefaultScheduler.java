package raft.core.schedule;



import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.core.node.config.NodeConfig;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@ThreadSafe
public class DefaultScheduler implements Scheduler {

    private static final Logger logger = LoggerFactory.getLogger(DefaultScheduler.class);
    //最小选举超时时间
    private final int minElectionTimeout;
    //最大选举超时时间
    private final int maxElectionTimeout;
    //初次日志复制延迟时间
    private final int logReplicationDelay;
    //日志复制间隔
    private final int logReplicationInterval;
    //随机数生成器
    private final Random electionTimeoutRandom;
    private final ScheduledExecutorService scheduledExecutorService;

    public DefaultScheduler(NodeConfig config) {
        this(config.getMinElectionTimeout(), config.getMaxElectionTimeout(), config.getLogReplicationDelay(),
                config.getLogReplicationInterval());
    }

    public DefaultScheduler(int minElectionTimeout, int maxElectionTimeout, int logReplicationDelay, int logReplicationInterval) {
        //判断参数是否有效
        //最小和最大选举超时间隔
        if (minElectionTimeout <= 0 || maxElectionTimeout <= 0 || minElectionTimeout > maxElectionTimeout) {
            throw new IllegalArgumentException("election timeout should not be 0 or min > max");
        }
        //初次日志复制延迟以及日志复制间隔
        if (logReplicationDelay < 0 || logReplicationInterval <= 0) {
            throw new IllegalArgumentException("log replication delay < 0 or log replication interval <= 0");
        }
        this.minElectionTimeout = minElectionTimeout;
        this.maxElectionTimeout = maxElectionTimeout;
        this.logReplicationDelay = logReplicationDelay;
        this.logReplicationInterval = logReplicationInterval;
        electionTimeoutRandom = new Random();
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "scheduler"));
    }

    //定时日志复制任务
    @Override
    @Nonnull
    public LogReplicationTask scheduleLogReplicationTask(@Nonnull Runnable task) {
        Preconditions.checkNotNull(task);
        logger.debug("schedule log replication task");
        //第一次执行等待时间，执行间隔， todo 为什么要设置第一次执行等待时间
        ScheduledFuture<?> scheduledFuture = this.scheduledExecutorService.scheduleWithFixedDelay(
                task, logReplicationDelay, logReplicationInterval, TimeUnit.MILLISECONDS);
        return new LogReplicationTask(scheduledFuture);
    }

    @Override
    @Nonnull
    public ElectionTimeout scheduleElectionTimeout(@Nonnull Runnable task) {
        Preconditions.checkNotNull(task);
        logger.debug("schedule election timeout");
        //note 随机超时时间,为了减少split over的影响
        int timeout = electionTimeoutRandom.nextInt(maxElectionTimeout - minElectionTimeout) + minElectionTimeout;
        ScheduledFuture<?> scheduledFuture = scheduledExecutorService.schedule(task, timeout, TimeUnit.MILLISECONDS);
        return new ElectionTimeout(scheduledFuture);
    }

    @Override
    public void stop() throws InterruptedException {
        logger.debug("stop scheduler");
        scheduledExecutorService.shutdown();
        scheduledExecutorService.awaitTermination(1, TimeUnit.SECONDS);
    }

}