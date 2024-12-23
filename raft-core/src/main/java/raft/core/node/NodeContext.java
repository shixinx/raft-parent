package raft.core.node;


import com.google.common.eventbus.EventBus;
import raft.core.log.Log;
import raft.core.node.config.NodeConfig;
import raft.core.node.store.NodeStore;
import raft.core.rpc.Connector;
import raft.core.schedule.Scheduler;
import raft.core.support.TaskExecutor;


/**
 * Node context.
 * <p>
 * Node context should not change after initialization. e.g {@link NodeBuilder}.
 * </p>
 */
public class NodeContext {

    //当前节点
    private NodeId selfId;
    //成员列表
    private NodeGroup group;
    //日志
    private Log log;
    //RPC组件
    private Connector connector;
    private NodeStore store;
    //定时器组件
    private Scheduler scheduler;
    private NodeMode mode;
    private NodeConfig config;
    private EventBus eventBus;
    //主线程执行器
    private TaskExecutor taskExecutor;
    private TaskExecutor groupConfigChangeTaskExecutor;

    //获取自己节点的id
    public NodeId selfId() {
        return selfId;
    }

    //设置自己节点的id
    public void setSelfId(NodeId selfId) {
        this.selfId = selfId;
    }

    public NodeGroup group() {
        return group;
    }

    public void setGroup(NodeGroup group) {
        this.group = group;
    }

    public Log log() {
        return log;
    }

    public void setLog(Log log) {
        this.log = log;
    }

    public Connector connector() {
        return connector;
    }

    public void setConnector(Connector connector) {
        this.connector = connector;
    }

    public NodeStore store() {
        return store;
    }

    public void setStore(NodeStore store) {
        this.store = store;
    }

    public Scheduler scheduler() {
        return scheduler;
    }

    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    public NodeMode mode() {
        return mode;
    }

    public void setMode(NodeMode mode) {
        this.mode = mode;
    }

    public NodeConfig config() {
        return config;
    }

    public void setConfig(NodeConfig config) {
        this.config = config;
    }

    public EventBus eventBus() {
        return eventBus;
    }

    public void setEventBus(EventBus eventBus) {
        this.eventBus = eventBus;
    }

    public TaskExecutor taskExecutor() {
        return taskExecutor;
    }

    public void setTaskExecutor(TaskExecutor taskExecutor) {
        this.taskExecutor = taskExecutor;
    }

    public TaskExecutor groupConfigChangeTaskExecutor() {
        return groupConfigChangeTaskExecutor;
    }

    public void setGroupConfigChangeTaskExecutor(TaskExecutor groupConfigChangeTaskExecutor) {
        this.groupConfigChangeTaskExecutor = groupConfigChangeTaskExecutor;
    }

}