package bigdata.hermesfuxi.zookeeper.example;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

/**
 * @author hermesfuxi
 * desc:
 *
 * 主备节点的切换，是分布式应用的基本要求。现在用 Zookeeper 实现主备节点自动切换功能。
 * 基本思路：
 * 1 多个服务启动后，都尝试在 Zookeeper中创建一个  EPHEMERAL 类型的节点，Zookeeper本身会保证，只有一个 服务 会创建成功，其他 服务 抛出异常。
 * 2 成功创建节点的 服务 ，作为主节点，继续运行
 * 3 其他 服务 设置一个Watcher监控节点状态，
 * 4 如果主节点消失，其他 服务会接到通知，再次尝试创建 EPHEMERAL 类型的节点。
 */
public class ActiveStandbySwitchDemo  implements Watcher {

    private static final Logger LOG = LoggerFactory.getLogger(ActiveStandbySwitchDemo.class);

    enum MasterStates {
        /**
         * running 运行时
         */
        RUNNING,
        /**
         * elected 选举，当选
         */
        ELECTED,
        /**
         * 未选定
         */
        NOT_ELECTED
    };

    private volatile MasterStates state = MasterStates.RUNNING;

    MasterStates getState() {
        return state;
    }

    private static final int SESSION_TIMEOUT = 5000;
    private static final String CONNECTION_STRING = "10.58.69.142:2181";
    private static final String ZNODE_NAME = "/master";
    private Random random = new Random(System.currentTimeMillis());
    private ZooKeeper zk;
    private String serverId = Integer.toHexString(random.nextInt());

    private volatile boolean connected = false;
    private volatile boolean expired = false;

    public void startZk() throws IOException {
        zk = new ZooKeeper(CONNECTION_STRING, SESSION_TIMEOUT, this);
    }

    public void stopZk() {
        if (zk != null) {
            try {
                zk.close();
            } catch (InterruptedException e) {
                LOG.warn("Interrupted while closing ZooKeeper session.", e);
            }
        }
    }

    /**
     * 抢注节点
     */
    public void enroll() {
        zk.create(ZNODE_NAME,
                serverId.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                masterCreateCallBack, null);
    }

    AsyncCallback.StringCallback masterCreateCallBack = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    //网络问题，需要检查节点是否创建成功
                    checkMaster();
                    return;
                case OK:
                    state = MasterStates.ELECTED;
                    break;
                case NODEEXISTS:
                    state = MasterStates.NOT_ELECTED;
                    // 添加Watcher
                    addMasterWatcher();
                    break;
                default:
                    state = MasterStates.NOT_ELECTED;
                    LOG.error("Something went wrong when running for master.",
                            KeeperException.create(KeeperException.Code.get(rc), path));
            }
            LOG.info("I'm " + (state == MasterStates.ELECTED ? "" : "not ") + "the leader " + serverId);
        }
    };

    public void checkMaster() {
        zk.getData(ZNODE_NAME, false, masterCheckCallBack, null);
    }

    AsyncCallback.DataCallback masterCheckCallBack = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    checkMaster();
                    return;
                case NONODE:
                    // 节点未创建，再次注册
                    enroll();
                    return;
                case OK:
                    if (serverId.equals(new String(data))) {
                        state = MasterStates.ELECTED;
                    } else {
                        state = MasterStates.NOT_ELECTED;
                        addMasterWatcher();
                    }
                    break;
                default:
                    LOG.error("Error when reading data.",KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    void addMasterWatcher() {
        zk.exists(ZNODE_NAME,
                masterExistsWatcher,
                masterExistsCallback,
                null);
    }
    Watcher masterExistsWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            if(event.getType() == Event.EventType.NodeDeleted){
                assert ZNODE_NAME.equals(event.getPath());
                enroll();
            }
        }
    };
    AsyncCallback.StatCallback masterExistsCallback = new AsyncCallback.StatCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    addMasterWatcher();
                    break;
                case OK:
                    break;
                case NONODE:
                    state = MasterStates.RUNNING;
                    enroll();
                    LOG.info("It sounds like the previous master is gone, " +
                            "so let's run for master again.");
                    break;
                default:
                    checkMaster();
                    break;
            }
        }
    };

    public static void main(String[] args) throws InterruptedException, IOException {
        ActiveStandbySwitchDemo master = new ActiveStandbySwitchDemo( );
        master.startZk();

        while (!master.isConnected()) {
            Thread.sleep(100);
        }
        master.enroll();
        while (!master.isExpired()) {
            Thread.sleep(1000);
        }

        master.stopZk();
    }
    boolean isConnected() {
        return connected;
    }

    boolean isExpired() {
        return expired;
    }

    @Override
    public void process(WatchedEvent e) {
        LOG.info("Processing event: " + e.toString());
        if (e.getType() == Event.EventType.None) {
            switch (e.getState()) {
                case SyncConnected:
                    connected = true;
                    break;
                case Disconnected:
                    connected = false;
                    break;
                case Expired:
                    expired = true;
                    connected = false;
                    LOG.error("Session expiration");
                default:
                    break;
            }
        }
    }

}
