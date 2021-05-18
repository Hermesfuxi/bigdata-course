package bigdata.hermesfuxi.flink.example.topn;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

/**
 * @author Hermesfuxi
 * 全局TopN
 */
public class GlobalTopNDemo {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 22222);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

/*
orderId02,1573483405000,gdsId01,500,beijing
orderId03,1573483408000,gdsId02,200,beijing
orderId03,1573483408000,gdsId03,300,beijing
orderId03,1573483408000,gdsId04,400,beijing
orderId07,1573483600000,gdsId01,600,beijing
orderId07,1573583600000,gdsId01,700,beijing
*/
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop-slave3", 8888);

        env.setParallelism(1);

        SingleOutputStreamOperator<OrderBean> beanStream = socketTextStream.process(new ProcessFunction<String, OrderBean>() {
            @Override
            public void processElement(String value, Context ctx, Collector<OrderBean> out) throws Exception {
                String[] fields = value.split(",");
                out.collect(new OrderBean(fields[0], Long.parseLong(fields[1]), fields[2], Double.valueOf(fields[3]), fields[4]));
            }
        });


        SingleOutputStreamOperator<Tuple3<String, String, Double>> result = beanStream.keyBy(OrderBean::getAreaId).process(new KeyedProcessFunction<String, OrderBean, Tuple3<String, String, Double>>() {
            private transient ValueState<Map<String, Double>> mapValueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                mapValueState = getRuntimeContext().getState(new ValueStateDescriptor<>("mapValueStateDescriptor", TypeInformation.of(new TypeHint<Map<String, Double>>() {
                })));
            }

            @Override
            public void processElement(OrderBean value, Context ctx, Collector<Tuple3<String, String, Double>> out) throws Exception {
                Map<String, Double> map = mapValueState.value();
                if (map == null) {
                    map = new HashMap<>();
                }
                String gdsId = value.getGdsId();
                map.put(gdsId, value.getAmount() + map.getOrDefault(gdsId, 0d));
                mapValueState.update(map);
                map.entrySet().stream().sorted((a, b) -> b.getValue().compareTo(a.getValue())).limit(3)
//                        .forEach(System.out::println);
                        .forEach(t->out.collect(Tuple3.of(value.getAreaId(), t.getKey(), t.getValue())));
            }
        });

        result.print();
/*
(beijing,gdsId01,2)
(beijing,gdsId04,1)
(beijing,gdsId03,1)
 */

        env.execute();
    }
}
