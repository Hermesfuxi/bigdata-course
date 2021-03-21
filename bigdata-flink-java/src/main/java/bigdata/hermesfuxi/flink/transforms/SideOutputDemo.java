package bigdata.hermesfuxi.flink.transforms;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SideOutputDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop-slave3", 8888);

        OutputTag<String> errorTag = new OutputTag<String>("error") {
        };

        SingleOutputStreamOperator<Integer> numLines = socketTextStream.process(new ProcessFunction<String, Integer>() {
            @Override
            public void processElement(String value, Context ctx, Collector<Integer> out) throws Exception {
                try{
                    Integer num = Integer.valueOf(value);
                    out.collect(num);
                }catch (Exception e){
                    ctx.output(errorTag, value);
                }
            }
        });

        DataStream<String> errorSideOutput = numLines.getSideOutput(errorTag);
        errorSideOutput.print("error");

        // 后面必须加上"{}"，否则会报错（类型异常）
        OutputTag<Integer> evenTag = new OutputTag<Integer>("even"){};
        OutputTag<Integer> oddTag = new OutputTag<Integer>("odd"){};

        SingleOutputStreamOperator<Integer> result = numLines.process(new ProcessFunction<Integer, Integer>() {
            @Override
            public void processElement(Integer value, Context ctx, Collector<Integer> out) throws Exception {
                if (value % 2 == 0) {
                    ctx.output(evenTag, value);
                } else {
                    ctx.output(oddTag, value);
                }
                out.collect(value);
            }
        });
        DataStream<Integer> evenSideOutput = result.getSideOutput(evenTag);
        evenSideOutput.print("even");

        DataStream<Integer> oddSideOutput = result.getSideOutput(oddTag);
        oddSideOutput.print("odd");

        result.print("result");

        env.execute();
    }
}
