package bigdata.hermesfuxi.zookeeper.client;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 查看zk的客户端操作
 */
public class ZKClientOperatorImpl implements ZKClientOperator {

    /**
     * 级联查看某节点下所有节点及节点值 ls -l
     */
    @Override
    public Map<String, String> getChildNodeAndValue(String path, ZooKeeper zk) throws Exception {
        HashMap<String, String> map = new HashMap<>();
        getDeepChildrenAndValue(map, path, zk);
        return map;
    }

    private boolean nodeExists(String path, final ZooKeeper zk) throws KeeperException, InterruptedException {
        Stat stat = zk.exists(path, false);
        if (stat != null) {
            System.out.println("Node exists and the node version is " + stat.getVersion());
            return true;
        } else {
            System.out.println("Node does not exists");
            return false;
        }
    }

    private void getDeepChildrenAndValue(Map<String, String> map, String path, final ZooKeeper zk) throws KeeperException, InterruptedException {

        //获取节点数据内容
        Stat stat = new Stat();// 传入了一个stat变量，在Zookeeper客户端的内部实现中，会从服务端的相应中获取到数据节点的最新节点状态信息，来替换这个客户端的旧状态
        byte[] data = zk.getData(path, false, stat);
        String dataStr = new String(data, StandardCharsets.UTF_8);
//        System.out.println(path + ": " + new String(data, StandardCharsets.UTF_8));
        map.put(path, dataStr);

        // 数据节点的最新节点状态信息
//        System.out.println(stat.getCzxid()+","+ stat.getMzxid()+","+stat.getVersion());

        System.out.println();
        System.out.println(path + ": ");
        System.out.println(dataStr);

        // 子节点信息
        List<String> childrenList = zk.getChildren(path, false);
        if (!path.endsWith("/")) {
            path += "/";
        }
        for (String childrenNode : childrenList) {
            getDeepChildrenAndValue(map, path + childrenNode, zk);
        }
    }

    /**
     * 删除一个节点，不管有有没有任何子节点
     */
    @Override
    public boolean rmrZnode(String path, ZooKeeper zk) throws Exception {
        if (nodeExists(path, zk)) {
            try {
                // 连同当前节点和其子节点一起删除
                clearChildNode(path, zk, false);
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        }
        return true;
    }

    /**
     * desc: 级联创建任意节点 /a/b/c/d/e
     */
    @Override
    public boolean createZnode(String path, String data, ZooKeeper zk) throws Exception {
        StringBuilder pathBuilder = new StringBuilder();
        String[] pathWords = path.split("/");
        boolean flag = false;
        // 最开始的空值不要
        for (int i = 1; i < pathWords.length; i++) {
            String tmpPath = pathBuilder.append("/").append(pathWords[i]).toString();
            if (flag || !nodeExists(tmpPath, zk)) {
                // 父节点不存在，其子节点一定不存在
                flag = true;
                try {
                    if (i == pathWords.length - 1) {
                        zk.create(tmpPath, data.getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    } else {
                        zk.create(tmpPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * 清空子节点
     */
    @Override
    public boolean clearChildNode(String path, ZooKeeper zk) throws Exception {
        try {
            clearChildNode(path, zk, true);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    private void clearChildNode(String path, ZooKeeper zk, boolean isFirstPath) throws Exception {
        if (nodeExists(path, zk)) {
            // 先删除当前节点的子节点
            List<String> childrenNodeList = zk.getChildren(path, false);
            for (String childrenNode : childrenNodeList) {
                String childrenNodePath;
                if (!path.endsWith("/")) {
                    childrenNodePath = path + "/" + childrenNode;
                }else {
                    childrenNodePath = path + childrenNode;
                }
                try {
                    clearChildNode(childrenNodePath, zk, false);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            // 再判断是否删除当前节点（若当前节点不是查询时根节点，则删除）
            if(!isFirstPath){
                zk.delete(path, zk.exists(path, false).getVersion());
            }
        }
    }
}
