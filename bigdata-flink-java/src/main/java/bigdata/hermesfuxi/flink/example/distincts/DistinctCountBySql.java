package bigdata.hermesfuxi.flink.example.distincts;

import bigdata.hermesfuxi.flink.example.distincts.functions.HLLDistinctUDAF;
import bigdata.hermesfuxi.flink.utlis.IDUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

import static jdk.nashorn.internal.objects.NativeFunction.call;
import static org.apache.flink.table.api.Expressions.*;

/*
数据：分别对应（用户ID-userId,活动ID-activeId,事件ID-eventId）
u1,A1,view
u1,A1,view
u1,A1,view
u1,A1,join
u1,A1,join
u2,A1,view
u2,A1,join
u1,A2,view
u1,A2,view
u1,A2,join

统计结果：(A1,view,2,4) (A1,join,2,3) (A2,view,1,2) (A2,join,1,1)
浏览次数：A1,view,4
浏览人数：A1,view,2
参与次数：A1,join,3
参与人数：A1,join,2
 */

/**
 * 活动事件表的用户统计（UV - count(distinct uid))与用户浏览统计（PV - count(udi)）：sql 实现
 */
public class DistinctCountBySql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //u1,A1,view
        DataStreamSource<String> lines = env.socketTextStream("hadoop-slave3", 8888);

        SingleOutputStreamOperator<Tuple3<String, String, String>> tpStream = lines.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(fields[0], fields[1], fields[2]);
            }
        });
        Table table = tableEnv.fromDataStream(tpStream, $("uid"), $("aid"), $("action"));

        // 方式一： count(distinct uid) 或 firstValue
//        tableEnv.createTemporaryView("t_event", tpStream, $("uid"), $("aid"), $("action"));
//        Table result1 = tableEnv.sqlQuery("select aid, action, count(distinct uid) as uv, count(uid) as pv from t_event group by aid,action");

        tableEnv.registerFunction("md5_id", new MD5IDFunction());
        // 使用老版正常，新版会报错，TODO 原因？
//        tableEnv.createFunction("hll_card", HyperLogLogDistinctFunction.class);
//        tableEnv.registerFunction("hll_card", new HyperLogLogDistinctFunction());
        tableEnv.registerFunction("hll_card", new HLLDistinctUDAF());
//
        // 方式二：使用 hll
        Table result = table.groupBy($("aid"), $("action"))
                .select(
                        $("aid"),
                        $("action"),
                        Expressions.call("hll_card",
                                Expressions.call("md5_id", $("uid"))
                        ).as("uv", "ids"),
                        $("uid").count().as("pv", "uids")
                );
//        Table result = tableEnv.sqlQuery("select aid, action, hll_card(md5_id(uid)) as uv, count(uid) as pv from tmp group by aid,action");


        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tableEnv.toRetractStream(result, Row.class);

        tuple2DataStream.filter(t -> t.f0).print();

        env.execute();
    }

    public static class MD5IDFunction extends ScalarFunction {
        public Long eval(String str) throws Exception {
            return IDUtils.getMD5(str);
        }
    }
}
