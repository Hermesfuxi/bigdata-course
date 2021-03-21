package bigdata.hermesfuxi.flink.sources.parquet;

import bigdata.hermesfuxi.flink.async.GeoDict;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.formats.parquet.ParquetPojoInputFormat;

/**
 * @author Hermesfuxi
 * desc: 从集合中创建一个数据流，集合中所有元素的类型是一致的。 都是有限的数据流
 */
public class ParquetSourceDemo2 {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String filePath = "hdfs://hadoop-master:9000/datayi/dicts/geodict/part-00000-ac206f6b-fd35-4c87-bf80-8897a825db44-c000.snappy.parquet";
        ParquetPojoInputFormat<GeoDict> parquetPojoInputFormat = ParquetUtils.getParquetPojoInputFormat(filePath, GeoDict.class);
        DataSource<GeoDict> dataSource = env.createInput(parquetPojoInputFormat, TypeInformation.of(GeoDict.class));
        dataSource.map(new MapFunction<GeoDict, String>() {
            @Override
            public String map(GeoDict value) throws Exception {
                return value.toString();
            }
        }).print();
        env.execute();
    }
}
