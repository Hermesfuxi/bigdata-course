package bigdata.hermesfuxi.flink.states.keyed;

import org.apache.commons.collections.map.HashedMap;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class ValueStateMyDemo {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 11111);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        environment.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.minutes(1), Time.seconds(5)));

        DataStreamSource<String> source = environment.socketTextStream("hadoop-slave3", 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMapOperator = source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] split = value.split("\\s+");
                for (String str : split) {
                    if (str.contains("error")) {
                        throw new Exception("输入数据有误");
                    }else{
                        out.collect(Tuple2.of(str, 1));
                    }
                }
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = flatMapOperator.keyBy(t -> t.f0).map(new MySumFunction());
        result.print();

        environment.execute();
    }

    private static class MySumFunction extends RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {
        private Map<String, Integer> map;
        @Override
        public void open(Configuration parameters) throws Exception {
            // 从 StateBackend 中 获取 state 数据
            int indexOfSubtask = getRuntimeContext().getIndexOfThisSubtask();
            File file = new File("D:/WorkSpaces/IdeaProjects/bigdata-course/.ck/flink/myKeyedState/" + indexOfSubtask + ".txt");
            if(!file.exists()){
                file.createNewFile();
                map = new HashMap<>();
            }else {
                ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(file));
                map = (Map<String, Integer>)objectInputStream.readObject();
            }

            //
            new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true){
                        try {
                            Thread.sleep(10000);
                            if (!file.exists()) {
                                file.createNewFile();
                            }
                            ObjectOutputStream objectOutputStream = new ObjectOutputStream(new FileOutputStream(file));
                            objectOutputStream.writeObject(map);
                            objectOutputStream.flush();
                            objectOutputStream.close();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }).start();
        }

        @Override
        public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {

            String word = value.f0;
            Integer currentCount = value.f1;

            Integer historyCount = map.getOrDefault(word, 0);
            Integer newCount = historyCount + currentCount;

            map.put(word, newCount); //更新状态

            return Tuple2.of(word, newCount); //输出结果

        }
    }
}
