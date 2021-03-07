package bigdata.hermesfuxi.flink.transforms;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.StreamMap;

public class MyMapDemo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> streamSource = environment.socketTextStream("hadoop-slave3", 8888);

        SingleOutputStreamOperator<String> outputStreamOperator = streamSource.transform("Map", Types.STRING, new StreamMap<>(String::toUpperCase));

        outputStreamOperator.print();

        environment.execute();
    }
}
