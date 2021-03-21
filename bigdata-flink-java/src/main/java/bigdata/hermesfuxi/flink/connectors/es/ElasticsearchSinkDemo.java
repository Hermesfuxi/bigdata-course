package bigdata.hermesfuxi.flink.connectors.es;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Requests;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ElasticsearchSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop-slave3", 8888);

        Map<String, String> esConfig = new HashMap<>();
        esConfig.put("cluster.name", "my-cluster-name");
        //该配置表示批量写入ES时的记录条数
        esConfig.put("bulk.flush.max.actions", "1");

        List<InetSocketAddress> transportAddressesList = new ArrayList<>();
        transportAddressesList.add(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9300));
        transportAddressesList.add(new InetSocketAddress(InetAddress.getByName("10.2.3.1"), 9300));

        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("localhost", 9200, "http"));
        ElasticsearchSink.Builder<String> esSinkBuilder = new ElasticsearchSink.Builder<String>(
                httpHosts,
                new ElasticsearchSinkFunction<String>() {
                    @Override
                    public void process(String customer, RuntimeContext ctx, RequestIndexer indexer) {
                        // 数据保存在Elasticsearch中名称为index_customer的索引中，保存的类型名称为 type_customer
                        indexer.add(Requests.indexRequest().index("index_customer").type("type_customer").id(customer).source(customer));
                    }
                }
        );
        // 设置批量写数据的缓冲区大小
        esSinkBuilder.setBulkFlushMaxActions(50);
        // 把转换后的数据写入到ElasticSearch中
        socketTextStream.addSink(esSinkBuilder.build());

        env.execute();
    }
}
