package bigdata.hermesfuxi.flink.example.distincts.functions;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import org.apache.flink.table.functions.AggregateFunction;

/**
 * HLL聚合 UDAF : 实际开发中优先使用
 * 依赖如下：存在依赖冲突，建议放在最后
 <!-- https://mvnrepository.com/artifact/com.addthis/stream-lib -->
 <dependency>
     <groupId>com.addthis</groupId>
     <artifactId>stream-lib</artifactId>
 </dependency>
 */
public class HyperLogLogDistinctFunction extends AggregateFunction<Long, HyperLogLog> {

    @Override public HyperLogLog createAccumulator() {
        return new HyperLogLog(0.001);
    }

    public void accumulate(HyperLogLog hll,String id){
        hll.offer(id);
    }

    @Override public Long getValue(HyperLogLog accumulator) {
        return accumulator.cardinality();
    }

//    // retract 撤销方法，与accumulate 同理，是其逆向操作，OVER Windows 时 必须，且必须非静态，可重载
//    public void retract(AggAccumulator accumulator, Long value) {
//
//    }
//
//    // 多分区结果合并，且必须非静态，可重载。许多有界聚合以及会话窗口和跃点窗口聚合是必需的
//    public void merge(AggAccumulator accumulator, Iterable<AggAccumulator> iterable) throws CardinalityMergeException {
//        for (AggAccumulator aggAccumulator : iterable) {
//            accumulator.hyperLogLog.addAll(aggAccumulator.hyperLogLog);
//        }
//    }
//
//    // 重置计算，且必须非静态，可重载
//    public void resetAccumulator(AggAccumulator accumulator) {
//    }
}
