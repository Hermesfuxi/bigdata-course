package bigdata.hermesfuxi.flink.connectors.mysql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * 查询新增和发生变化的数据
 */
public class MySQLSource extends RichSourceFunction<Tuple2<String, String>> {


    private boolean flag = true;

    private String lastUpdateTime = null;

    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8", "root", "123456");
    }

    /**
     * run方法在restoreState方法之后执行
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {

        while (flag) {

            String sql = "SELECT id, name, last_update FROM t_activities WHERE last_update > ? ORDER BY last_update DESC";
            //查询数据库 SELECT * FROM t_category WHERE last_update > 上一次查询时间
            PreparedStatement prepareStatement = connection.prepareStatement(sql);
            prepareStatement.setString(1, lastUpdateTime != null ? lastUpdateTime : "2000-01-01 00:00:00");
            ResultSet resultSet = prepareStatement.executeQuery();
            int index = 0;
            while (resultSet.next()) {
                String id = resultSet.getString("id");
                String name = resultSet.getString("name");
                String time = resultSet.getString("last_update");
                //最大的数据，以后根据最大的时间作为查询条件
                if(index == 0) {
                    lastUpdateTime = time;
                }
                index++;
                System.out.println(lastUpdateTime);
                ctx.collect(Tuple2.of(id, name));
            }
            resultSet.close();
            prepareStatement.close();

            Thread.sleep(1000);
        }

    }

    @Override
    public void cancel() {
        flag = false;
    }

    @Override
    public void close() throws Exception {
        if(connection != null) {
            connection.close();
        }
    }
}