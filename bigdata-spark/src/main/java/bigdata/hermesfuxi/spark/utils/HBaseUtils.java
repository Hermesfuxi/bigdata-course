package bigdata.hermesfuxi.spark.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * @author Hermesfuxi
 */
public class HBaseUtils {
    public static Connection getConnection() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hadoop-master:2181,hadoop-master2:2181,hadoop-slave1:2181,hadoop-slave2:2181,hadoop-slave3:2181");
        return ConnectionFactory.createConnection(conf);
    }
}
