package bigdata.hermesfuxi.flink.query;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.queryablestate.client.QueryableStateClient;

import java.util.concurrent.CompletableFuture;

public class QueryStateClientDemo {
    public static void main(String[] args) throws Exception {
        QueryableStateClient client = new QueryableStateClient("localhost", 9069);
        //初始化状态数据或恢复历史状态数据
        ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>(
                "wc-state", //指定状态描述器的名称
                Integer.class //存储数据的类型
        );
        CompletableFuture<ValueState<Integer>> resultFuture = client.getKvState(
                JobID.fromHexString("98d3e410a143d8b9441f4e5e7f1940b6"), //job的ID
                "my-query-name", //可查询的state的名称
                "flink", //查询的key
                BasicTypeInfo.STRING_TYPE_INFO,
                stateDescriptor);
        resultFuture.thenAccept(response -> {
            try {
                Integer res = response.value();
                System.out.println(res);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        Thread.sleep(5000);
    }
}
