package bigdata.hermesfuxi.flink.connectors.jdbc;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcOutputFormat;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

class PhoenixSinkFunction<IN> extends RichSinkFunction<IN> {

    final JdbcOutputFormat outputFormat;

    PhoenixSinkFunction(JdbcOutputFormat outputFormat) {
        this.outputFormat = outputFormat;
    }

    @Override
    public void invoke(IN value) throws Exception {
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        RuntimeContext ctx = getRuntimeContext();
        outputFormat.setRuntimeContext(ctx);
        outputFormat.open(ctx.getIndexOfThisSubtask(), ctx.getNumberOfParallelSubtasks());
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

}