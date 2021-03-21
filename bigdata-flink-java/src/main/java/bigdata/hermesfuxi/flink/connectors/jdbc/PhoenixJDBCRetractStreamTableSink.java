package bigdata.hermesfuxi.flink.connectors.jdbc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcOutputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import java.io.Serializable;

public class PhoenixJDBCRetractStreamTableSink implements RetractStreamTableSink<Row>, Serializable {
    private TableSchema tableSchema;
    private JdbcOutputFormat outputformat;
    public PhoenixJDBCRetractStreamTableSink(){}

    public PhoenixJDBCRetractStreamTableSink(String[] fieldNames, TypeInformation[] typeInformations, JdbcOutputFormat outputformat){
        this.tableSchema=new TableSchema(fieldNames,typeInformations);
        this.outputformat = outputformat;
    }
    //重载
    public PhoenixJDBCRetractStreamTableSink(String[] fieldNames, DataType[] dataTypes){
        this.tableSchema=TableSchema.builder().fields(fieldNames,dataTypes).build();
    }

    //Table sink must implement a table schema.
    @Override
    public TableSchema getTableSchema() {
        return tableSchema;
    }
    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        JdbcOutputFormat phoenixOutputformat = this.outputformat;

        return dataStream.addSink(new PhoenixSinkFunction<Tuple2<Boolean, Row>>(phoenixOutputformat) {
            @Override
            public void invoke(Tuple2<Boolean, Row> value) throws Exception {
                //自定义Sink
                // f0==true :插入新数据
                // f0==false:删除旧数据
                if(value.f0){
                    //数据写入phoenix
                    phoenixOutputformat.writeRecord(value.f1);
                }
            }
        });
    }

    //接口定义的方法
    @Override
    public TypeInformation<Row> getRecordType() {
        return new RowTypeInfo(tableSchema.getFieldTypes(),tableSchema.getFieldNames());
    }
    //接口定义的方法
    @Override
    public TableSink<Tuple2<Boolean, Row>> configure(String[] strings, TypeInformation<?>[] typeInformations) {
        return null;
    }

    @Override
    public TypeInformation<Tuple2<Boolean, Row>> getOutputType() {
        return null;
    }

}