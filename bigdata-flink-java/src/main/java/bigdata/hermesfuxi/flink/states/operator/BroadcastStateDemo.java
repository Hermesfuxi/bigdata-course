package bigdata.hermesfuxi.flink.states.operator;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class BroadcastStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 模拟字典数据源（如数据库）
/* 测试数据集
INSERT,10,情人节活动
UPDATE,10,情人节促销
DELETE,11,元旦活动
 */
        DataStreamSource<String> source1 = env.socketTextStream("hadoop-slave3", 8888);

        // 整理维度数据
        SingleOutputStreamOperator<Tuple3<String, String, String>> mapOperator = source1.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String value) throws Exception {
                String[] split = value.split(",");
                return Tuple3.of(split[0], split[1], split[2]);
            }
        });
        MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<>("broadcastStateDemo", String.class, String.class);
        // 被广播的一般是小表或字典表
        BroadcastStream<Tuple3<String, String, String>> broadcast = mapOperator.broadcast(mapStateDescriptor);

        // 模拟业务数据源（如数据库）
/* 测试数据集
uid1,10,1234.12
uid2,11,468.95
uid3,10,1296.00
uid4,12,777.83
*/
        DataStreamSource<String> factSource = env.socketTextStream("hadoop-slave1", 8888);
        // 整理事实数据
        SingleOutputStreamOperator<Tuple3<String, String, Double>> factDataStream = factSource.map(new MapFunction<String, Tuple3<String, String, Double>>() {
            @Override
            public Tuple3<String, String, Double> map(String value) throws Exception {
                String[] split = value.split(",");
                return Tuple3.of(split[0], split[1], Double.parseDouble(split[2]));
            }
        });

        SingleOutputStreamOperator<Tuple4<String, String, String, Double>> result = factDataStream.connect(broadcast).process(new MyBroadcastProcess(mapStateDescriptor));

        result.print();

        env.execute();
    }

    private static class MyBroadcastProcess extends BroadcastProcessFunction<Tuple3<String, String, Double>, Tuple3<String, String, String>, Tuple4<String, String, String, Double>> {
        private MapStateDescriptor<String, String> mapStateDescriptor;

        public MyBroadcastProcess() {
        }

        public MyBroadcastProcess(MapStateDescriptor<String, String> mapStateDescriptor) {
            this.mapStateDescriptor = mapStateDescriptor;
        }

        // 处理主体数据：以联结键作为key，从BroadcastState取出相关的数据，并拼接成结果数据返回
        @Override
        public void processElement(Tuple3<String, String, Double> value, ReadOnlyContext ctx, Collector<Tuple4<String, String, String, Double>> out) throws Exception {

            ReadOnlyBroadcastState<String, String> ctxBroadcastState = ctx.getBroadcastState(mapStateDescriptor);
            String uid = value.f0; //用户ID
            String aid = value.f1; //活动ID
            Double money = value.f2;
            String name = ctxBroadcastState.get(aid);
            out.collect(Tuple4.of(uid, aid, name, money));
        }

        // 处理广播流的关联数据（维度数据）: 对BroadcastState进行相应的操作（BroadcastState以联结键作为key）
        @Override
        public void processBroadcastElement(Tuple3<String, String, String> value, Context ctx, Collector<Tuple4<String, String, String, Double>> out) throws Exception {
            //INSERT,UPDATE,DELETE
            String type = value.f0;
            String id = value.f1;
            String name = value.f2;

            BroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
            if ("DELETE".equals(type)) {
                broadcastState.remove(id);
            } else {
                broadcastState.put(id, name);
            }

        }
    }
}
