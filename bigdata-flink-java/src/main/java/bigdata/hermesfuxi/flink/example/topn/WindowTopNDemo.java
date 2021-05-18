package bigdata.hermesfuxi.flink.example.topn;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.TreeSet;

/**
 * @author Hermesfuxi
 * TopN 的需求场景不管是在离线计算还是实时计算都是比较常见的，例如电商中计算热门销售商品、广告计算中点击数前N的广告、搜索中计算搜索次数前N的搜索词。
 * topN又分为全局topN、分组topN, 比喻说热门销售商品可以直接按照各个商品的销售总额排序，也可以先按照地域分组然后对各个地域下各个商品的销售总额排序。
 * 本篇以热门销售商品为例，实时统计每10min内各个地域维度下销售额top10的商品。
 * <p>
 * 这个需求可以分解为以下几个步骤：
 * 1、提取数据中订单时间为事件时间
 * 2、按照区域+商品为维度，统计每个10min中的销售额
 * 3、按照区域为维度，统计该区域的top10 销售额的商品
 */
public class WindowTopNDemo {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 22222);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

/*
orderId02,1573483405000,gdsId01,500,beijing
orderId03,1573483408000,gdsId02,200,beijing
orderId03,1573483408000,gdsId03,300,beijing
orderId03,1573483408000,gdsId04,400,beijing
orderId07,1573483600000,gdsId01,600,beijing
orderId07,1573583600000,gdsId01,700,beijing //触发
*/
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop-slave3", 8888);

        env.setParallelism(1);

        SingleOutputStreamOperator<OrderBean> beanStream = socketTextStream.process(new ProcessFunction<String, OrderBean>() {
            @Override
            public void processElement(String value, Context ctx, Collector<OrderBean> out) throws Exception {
                String[] fields = value.split(",");
                out.collect(new OrderBean(fields[0], Long.parseLong(fields[1]), fields[2], Double.valueOf(fields[3]), fields[4]));
            }
        });

        SingleOutputStreamOperator<OrderBean> beanStreamWithWatermark = beanStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderBean>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((element, time) -> element.getOrderTime())
        );

        SingleOutputStreamOperator<Tuple3<String, String, Long>> aggregatedWindow = beanStreamWithWatermark.keyBy(new KeySelector<OrderBean, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(OrderBean value) throws Exception {
                return Tuple2.of(value.getAreaId(), value.getGdsId());
            }
        })
                .window(TumblingEventTimeWindows.of(Time.minutes(10)))
                .aggregate(new AggregateFunction<OrderBean, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(OrderBean value, Long accumulator) {
                        return accumulator + 1;
                    }

                    @Override
                    public Long getResult(Long accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Long merge(Long a, Long b) {
                        return a + b;
                    }
                }, new ProcessWindowFunction<Long, Tuple3<String, String, Long>, Tuple2<String, String>, TimeWindow>() {
                    @Override
                    public void process(Tuple2<String, String> key, Context context, Iterable<Long> elements, Collector<Tuple3<String, String, Long>> out) throws Exception {
                        Long accumulator = elements.iterator().next();
                        out.collect(Tuple3.of(key.f0, key.f1, accumulator));
                    }
                });

        SingleOutputStreamOperator<Tuple3<String, String, Long>> result = aggregatedWindow.keyBy(t -> t.f0)
                // 划分两个相同的窗口：同一个窗口的输出数据具有相同的数据时间endTime, 这些数据正好可以在下游窗口被分配到同一个窗口中。在上一个窗口触发之后输出watermark正好可以触发下游窗口的窗口操作
                .window(TumblingEventTimeWindows.of(Time.minutes(10)))
                .process(new ProcessWindowFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Tuple3<String, String, Long>> elements, Collector<Tuple3<String, String, Long>> out) throws Exception {

                        // 排充: Sorted的数据结构TreeSet或者是优先级队列PriorityQueue
                        // TreeSet 实现原理是红黑树，优先队列实现原理就是最大/最小堆，这两个都可以满足需求，但是需要选择哪一个呢？
                        // 红黑树的时间复杂度是logN，而堆的构造复杂度是N, 读取复杂度是1，
                        // 但是这里需要不断的做数据插入，那么就涉及不断的构造过程，相对而言选择红黑树比较好
                        // (其实flink sql内部做topN也是选择红黑树类型的TreeMap), 注意TreeSet会去重（值相同）
                        TreeSet<Tuple3<String, String, Long>> treeSet = new TreeSet<>((o1, o2) -> {
                            if(o1.f2.equals(o2.f2)){
                                return o2.f1.compareTo(o1.f1);
                            }else {
                                return (int) (o2.f2 - o1.f2);
                            }
                        });

                        for (Tuple3<String, String, Long> element : elements) {
                            treeSet.add(element);
                            if (treeSet.size() > 3) {
                                treeSet.pollLast(); // 降序需要将最后舍弃掉
                            }
                        }
//                        treeSet.forEach(System.out::println);
                        treeSet.forEach(t->out.collect(Tuple3.of(t.f0, t.f1, t.f2)));
                    }
                });

        result.print();
/*
(beijing,gdsId01,2)
(beijing,gdsId04,1)
(beijing,gdsId03,1)
 */

        env.execute();
    }
}
