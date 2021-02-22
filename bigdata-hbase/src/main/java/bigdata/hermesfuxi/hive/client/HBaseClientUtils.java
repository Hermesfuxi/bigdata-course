package bigdata.hermesfuxi.hive.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

public class HBaseClientUtils {
    /**
     * 获取Hbase的连接对象
     */
    public static Connection getHbaseConnection() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum" , "hadoop-master:2181,hadoop-master2:2181,hadoop-slave1:2181,hadoop-slave2:2181,hadoop-slave3:2181");
        return ConnectionFactory.createConnection(conf);
    }

    /**
     * 获取Hbase的管理对象: Admin 和DDL , Tools有关的操作
     */
    public static Admin getAdmin() throws IOException {
        Connection conn = getHbaseConnection();
        // DDL语言  Tools 有关的操作在  admin对象中
        return conn.getAdmin();
    }

    /**
     * 获取表对象: DML语言有关的操作
     */
    public  static Table getTable(String name) throws Exception {
        Connection conn = getHbaseConnection();
        return conn.getTable(TableName.valueOf(name));
    }
}
