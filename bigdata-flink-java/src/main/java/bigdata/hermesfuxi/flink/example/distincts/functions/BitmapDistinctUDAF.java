package bigdata.hermesfuxi.flink.example.distincts.functions;

import net.agkn.hll.HLL;
import org.apache.flink.table.functions.AggregateFunction;

/**
 * HLL算法的UDAF实现：需要配合 String -> Long 的算法实现
 * 依赖如下：存在依赖冲突，建议放在最后
 * <dependency>
 *   <groupId>net.agkn</groupId>
 *   <artifactId>hll</artifactId>
 * </dependency>
 */
public class BitmapDistinctUDAF extends AggregateFunction<Long, HLL> {

    //初始化 UDAF的 accumulator
    @Override
    public HLL createAccumulator() {
        return new HLL(15, 4);
    }


    //getValue提供了如何通过存放状态的 accumulator 计算 UDAF 的结果的方法
    @Override
    public Long getValue(HLL accumulator) {
        return accumulator.cardinality();
    }


    // accumulate 累积方法必须公开声明（public），且必须非静态，可重载
    //accumulate提供了如何根据输入的数据更新  UDAF存放状态的accumulator
    public void accumulate(HLL accumulator, Long value) {
        accumulator.addRaw(value);
    }

    // retract 撤销方法，与accumulate 同理，是其逆向操作，OVER Windows 时 必须，且必须非静态，可重载
    public void retract(HLL accumulator, Long value) {
    }

    // 多分区结果合并，且必须非静态，可重载。许多有界聚合以及会话窗口和跃点窗口聚合是必需的
    public void merge(HLL accumulator, Iterable<HLL> iterable) {
        for (HLL hll : iterable) {
            accumulator.union(hll);
        }
    }

    // 重置计算，且必须非静态，可重载
    public void resetAccumulator(HLL accumulator) {
        accumulator.clear();
    }
}
