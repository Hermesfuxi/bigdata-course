package bigdata.hermesfuxi.zookeeper.client;


import org.apache.zookeeper.*;

import java.util.List;

public class ZKClient {
    public static void main(String[] args) throws Exception {
        // 构造一个zk的客户端
        ZooKeeper zooKeeper = new ZooKeeper("localhost:2181", 2000, null);
        System.out.println("0000000000" + zooKeeper.toString());
//        if (!zooKeeper.getState().isConnected()) {
//            return;
//        }

        String s = zooKeeper.create("/d", "3".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(s);


        // 测试监控功能
//        watch(zooKeeper);
        zooKeeper.close();

    }

    private static void watch(final ZooKeeper zooKeeper) throws KeeperException, InterruptedException {
        zooKeeper.getChildren("/", new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                try {
                    List<String> childrenList = zooKeeper.getChildren("/", this);
                    for (String children : childrenList) {
                        System.out.println("222222" + children);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        Thread.sleep(Integer.MAX_VALUE);
    }
}
