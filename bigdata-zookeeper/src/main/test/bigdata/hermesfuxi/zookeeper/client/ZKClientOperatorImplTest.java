package bigdata.hermesfuxi.zookeeper.client;

import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

/**
 * ZKClientOperatorImpl Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>6月 2, 2021</pre>
 */
public class ZKClientOperatorImplTest {
    private ZooKeeper zooKeeper;

    private ZKClientOperatorImpl zkClientOperator;

    @Before
    public void before() throws Exception {
        zooKeeper = new ZooKeeper("localhost:2181", 2000, null);
        zkClientOperator = new ZKClientOperatorImpl();
    }

    @After
    public void after() throws Exception {
    }


    /**
     * Method: getChildNodeAndValue(String path, ZooKeeper zk)
     */
    @Test
    public void testGetChildNodeAndValue() throws Exception {
        Map<String, String> childNodeAndValue = zkClientOperator.getChildNodeAndValue("/", zooKeeper);
    }

    /**
     * Method: rmrZnode(String path, ZooKeeper zk)
     */
    @Test
    public void testRmrZnode() throws Exception {
        boolean result = zkClientOperator.rmrZnode("/d", zooKeeper);
        Assert.assertEquals(true, result);
    }

    /**
     * Method: createZnode(String path, String data, ZooKeeper zk)
     */
    @Test
    public void testCreateZnode() throws Exception {
        boolean result = zkClientOperator.createZnode("/a/b/c/d/e", "1234567890", zooKeeper);
        Assert.assertEquals(true, result);
    }

    /**
     * Method: clearChildNode(String path, ZooKeeper zk)
     */
    @Test
    public void testClearChildNode() throws Exception {
        boolean result = zkClientOperator.clearChildNode("/a/b/c", zooKeeper);
        Assert.assertEquals(true, result);
    }

}
