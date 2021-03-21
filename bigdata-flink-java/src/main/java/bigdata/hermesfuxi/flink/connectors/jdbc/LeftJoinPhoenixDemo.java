package bigdata.hermesfuxi.flink.connectors.jdbc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Properties;

public class LeftJoinPhoenixDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "bigdata-dev1:9092,bigdata-dev2:9092,bigdata-dev3:9092");
        properties.setProperty("zookeeper.connect", "bigdata-dev1:2181,bigdata-dev2:2181,bigdata-dev3:2181");


        /*
         * kafka topic:
         * action_log,action_log_detail
         *
         * 读取两张数据表:
         * action_log action_log_detail
         * */

        tableEnv.execute("");
    }
}
