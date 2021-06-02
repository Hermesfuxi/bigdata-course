package bigdata.hermesfuxi.zookeeper.client;


import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class ZKClient {
    public static void main(String[] args) throws Exception {
        // 构造一个zk的客户端
        ZooKeeper zooKeeper = new ZooKeeper("localhost:2181", 2000, null);

//        String s = zooKeeper.create("/d", "3".getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
//        System.out.println(s);

        getDeepChildrenAndValue("/", zooKeeper);

        // 测试监控功能
//        watch(zooKeeper);
        zooKeeper.close();

    }

    public static void create(String path, final ZooKeeper zk, byte[] data) throws KeeperException, InterruptedException {
        zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    private static void watch(final ZooKeeper zooKeeper) throws KeeperException, InterruptedException {
        zooKeeper.getChildren("/", new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                try {
                    List<String> childrenList = zooKeeper.getChildren("/", this);
                    for (String children : childrenList) {
                        System.out.println(children);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        Thread.sleep(Integer.MAX_VALUE);
    }

    private static void getDeepChildrenAndValue(String path, final ZooKeeper zooKeeper) throws KeeperException, InterruptedException {

        //获取节点数据内容
        Stat stat = new Stat();// 传入了一个stat变量，在Zookeeper客户端的内部实现中，会从服务端的相应中获取到数据节点的最新节点状态信息，来替换这个客户端的旧状态
        byte[] data = zooKeeper.getData(path, true, stat);
        System.out.println(path + ": " + new String(data, StandardCharsets.UTF_8));

        // 数据节点的最新节点状态信息
//        System.out.println(stat.getCzxid()+","+ stat.getMzxid()+","+stat.getVersion());

        // 子节点信息
        List<String> childrenList = zooKeeper.getChildren(path, true);
        if (!path.endsWith("/")) {
            path += "/";
        }
        for (String childrenNode : childrenList) {
            getDeepChildrenAndValue(path + childrenNode, zooKeeper);
        }
    }
}
