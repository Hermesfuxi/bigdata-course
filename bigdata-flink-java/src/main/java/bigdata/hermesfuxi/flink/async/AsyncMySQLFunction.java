package bigdata.hermesfuxi.flink.async;

import bigdata.hermesfuxi.flink.connectors.mysql.DBConnectUtils;
import com.alibaba.druid.pool.DruidDataSource;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class AsyncMySQLFunction extends RichAsyncFunction<String, Map<String, GeoDict>> {
    private transient DruidDataSource druidDataSource;
    private transient ScheduledExecutorService executorService;
    private int maxConnTotal; //线程池最大线程数量

    public AsyncMySQLFunction(int maxConnTotal) {
        this.maxConnTotal = maxConnTotal;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        executorService = new ScheduledThreadPoolExecutor(1,
                new BasicThreadFactory.Builder().namingPattern("async-mysql-pool-%d").daemon(true).build());

        Properties properties = new Properties();
        properties.load(DBConnectUtils.class.getClassLoader().getResourceAsStream("druid.properties"));
        druidDataSource = new DruidDataSource();
        druidDataSource.setAsyncInit(true);
        druidDataSource.setMaxActive(maxConnTotal);
        druidDataSource.setDriverClassName(properties.getProperty("driverClassName"));
        druidDataSource.setUrl(properties.getProperty("realtime.url"));
        druidDataSource.setUsername(properties.getProperty("username"));
        druidDataSource.setPassword(properties.getProperty("password"));
    }

    @Override
    public void close() throws Exception {
        //关闭数据库连接池
        druidDataSource.close();
        //关闭线程池
        executorService.shutdown();
    }

    @Override
    public void asyncInvoke(String input, ResultFuture<Map<String, GeoDict>> resultFuture) throws Exception {
        //调用线程池的submit方法，将查询请求丢入到线程池中异步执行，返回Future对象
        Future<GeoDict> future = executorService.submit(new Callable<GeoDict>() {
            @Override
            public GeoDict call() throws Exception {
                return queryFromMysql(input);
            }
        });
        CompletableFuture.supplyAsync(new Supplier<GeoDict>() {
            @Override
            public GeoDict get() {
                try {
                    return future.get();
                } catch (Exception e) {
                    e.printStackTrace();
                    return null;
                }
            }
        }).thenAccept(new Consumer<GeoDict>() {
            @Override
            public void accept(GeoDict geoDict) {
                ConcurrentHashMap<String, GeoDict> map = new ConcurrentHashMap<>();
                map.put(input, geoDict);
                resultFuture.complete(Collections.singleton(map));
            }
        });
    }

    private GeoDict queryFromMysql(String input) {
        GeoDict geoDict = null;
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            connection = druidDataSource.getConnection();
            ps = connection.prepareStatement("SELECT geo_hash, province, city, region FROM geo_hash_area WHERE geo_hash = ?");
            ps.setString(1, input);
            rs = ps.executeQuery();
            while (rs.next()){
                String geoHash = rs.getString("geo_hash");
                String province = rs.getString("province");
                String city = rs.getString("city");
                String region = rs.getString("region");
                geoDict = new GeoDict(geoHash, province, city, region);
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            DBConnectUtils.close(rs);
            DBConnectUtils.close(ps);
            DBConnectUtils.close(connection);
        }
        return geoDict;
    }
}
