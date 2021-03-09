package bigdata.hermesfuxi.flink.states.operator;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink 为算子状态(operator state)提供三种基本数据结构：
 *     列表状态（List state）: 将状态表示为一组数据的列表
 *     联合列表状态（Union list state）: 也将状态表示为数据的列表。它与常规列表状态的区别在于，在发生故障时，或者从保 存点（savepoint）启动应用程序时如何恢复。 
 *     广播状态（Broadcast state） 如果一个算子有多项任务，而它的每项任务状态又都相同，那么这种特殊情况最适合应 用广播状态
 */
public class OperatorListStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.execute();
    }
}
