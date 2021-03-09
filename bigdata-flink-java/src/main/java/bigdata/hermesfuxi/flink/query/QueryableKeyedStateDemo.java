package bigdata.hermesfuxi.flink.query;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.QueryableStateOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class QueryableKeyedStateDemo {

    public static class MyQueryStateRichMapFunction extends
            RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {
        //使用flink的ValueState保存单词对应的次数
        private transient ValueState<Integer> countState;

        @Override
        public void open(Configuration parameters) throws Exception {
            //初始化状态数据或恢复历史状态数据
            ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>(
                    "wc-state", //指定状态描述器的名称
                    Integer.class //存储数据的类型
            );

            stateDescriptor.setQueryable("my-query-name"); //设置状态可以查询，并指定状态查询名称

            //获取getRuntimeContext并根据状态描述器取出对应的State
            countState = getRuntimeContext().getState(stateDescriptor);
        }

        @Override
        public Tuple2<String, Integer> map(Tuple2<String, Integer> tp) throws Exception {
            Integer current = tp.f1;  //获取当前输入的单词的次数
            Integer counts = countState.value(); //获取历史的次数
            if (counts == null) { //历史次数是否为空
                counts = 0; //如果历史床头为空，初始值设置为0
            }
            int total = current + counts; //将当前的次数和历史次数进行累加
            countState.update(total); //更新状态
            tp.f1 = total; //将累加后的次数放入到tp
            return tp; //返回tp
        }
    }

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        Configuration config = params.getConfiguration();
        //启用Queryable State服务相关参赛
        config.setInteger("rest.port", 1111);
        config.setBoolean(QueryableStateOptions.ENABLE_QUERYABLE_STATE_PROXY_SERVER, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);

        //开启checkpoint【检查点，可以将任务计算的中间结果（状态数据保存起来）】
        env.enableCheckpointing(30000);
        //设置重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5)));

        //从指定的Socket地址和端口创建DataStream
        DataStreamSource<String> lines = env.socketTextStream("hadoop-slave3", 8888);
        //将单词和1组合，放入到Tuple2中
        DataStream<Tuple2<String, Integer>> wordAndOne = lines.map(
                new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String word) throws Exception {
                        return Tuple2.of(word, 1);
                    }
                }
        );
        //按照单词进行分组
        KeyedStream<Tuple2<String, Integer>, String> keyed = wordAndOne.keyBy(t -> t.f0);
        //分完组，相同的key会进到相同的组，每一个组都维护自己的状态数据
        DataStream<Tuple2<String, Integer>> result = keyed.map(new MyQueryStateRichMapFunction());
        result.print();
        env.execute();
    }
}
