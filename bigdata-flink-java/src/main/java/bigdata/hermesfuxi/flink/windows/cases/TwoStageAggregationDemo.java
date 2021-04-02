package bigdata.hermesfuxi.flink.windows.cases;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author Hermesfuxi
 * desc:
 * 问题抽象: Flink单窗口数据量过大(百万级)，导致窗口聚合过慢。数据热点/倾斜问题
 *
 * 例子: 统计某个国家，某个城市的实时订单数。
 * 痛点: 某"国家_城市"下订单数过大，比如百万级甚至千万级，导致窗口聚合时间过长，过慢。
 *
 * 解决方案: 两阶段聚合解决 KeyBy 热点
 * a. 将需要分组的 key 打散，例如添加随机的后缀
 * b. 对打散后的数据进行聚合
 * c. 将被打散的 key 还原为原始的 key
 * d. 二次 KeyBy 来统计最终结果并输出给下游
 *
 * 总之：先拆分，然后分别计算，最后汇总。拆分可以按照订单尾号拆分(选择尾号可以达到负载均衡，解决数据倾斜问题)。
 */
public class TwoStageAggregationDemo {
    public static void main(String[] args) {
//        SingleOutputStreamOperator<OrderModel> inStream = null;// todo 自定义源
//        inStream.flatMap(new FlatMapFunction<OrderModel, Tuple2<String, OrderModel>>() {
//                    @Override
//                    public void flatMap(OrderModel oneOrder, Collector<Tuple2<String, OrderModel>> out) throws Exception {
//                        // todo 数据拆分:   "国家_城市_订单尾号" 需要从oneOrder提取
//                        out.collect(new Tuple2("国家_城市_订单尾号", oneOrder));
//                    }
//                })// 拆分逻辑
//                .assignTimestampsAndWatermarks("todo 自己实现")//添加水印
//                .keyBy(0)//按照key聚合 即国家_城市_订单尾号
//                .timeWindow(Time.seconds(60L))//60s一个窗口
//                .process(new ProcessWindowFunction() {
//                    @Override
//                    public void process(Object o, Context context, Iterable elements, Collector out) throws Exception {
//                        // todo 第一个窗口:计算某个尾号该分钟总订单数
//                    }
//                })
//                .keyBy("国家_城市")   // 国家_城市
//                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5L)))//session 窗口,5秒钟容忍时间
//                .process(new ProcessWindowFunction() {
//                    @Override
//                    public void process(Object o, Context context, Iterable elements, Collector out) throws Exception {
//                        // todo 第二个窗口:做汇总。实时订单总数=0号尾号订单数量+1号尾号订单数量+...+9号尾号订单数量
//                    }
//                });
    }
}
