package bigdata.hermesfuxi.zookeeper.client;

import org.apache.zookeeper.ZooKeeper;

import java.util.Map;

/**
 * @author hermesfuxi
 * @date 2021-06-02
 *
 * 功能实现
 * 1、级联查看某节点下所有节点及节点值
 * 2、删除一个节点，不管有有没有任何子节点
 * 3、级联创建任意节点
 * 4、清空子节点
 */
public interface ZKClientOperator {
    /**
     * 级联查看某节点下所有节点及节点值
     */
    public Map<String, String> getChildNodeAndValue(String path, ZooKeeper zk) throws Exception;

    /**
     * 删除一个节点，不管有有没有任何子节点
     */
    public boolean rmrZnode(String path, ZooKeeper zk) throws Exception;

    /**
     * desc: 级联创建任意节点 /a/b/c/d/e
     */
    public boolean createZnode(String znodePath, String data, ZooKeeper zk) throws Exception;

    /**
     * 清空子节点
     */
    public boolean clearChildNode(String znodePath, ZooKeeper zk) throws Exception;
}
