package bigdata.hermesfuxi.flink.connectors.mysql;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MySQLTwoPhaseCommitSink extends TwoPhaseCommitSinkFunction<Tuple2<String, Integer>, Connection, Void> {
    public static final Logger logger = LoggerFactory.getLogger(MySQLTwoPhaseCommitSink.class);

    /**
     * 事务数据是保存在state状态里，checkpoint后可回复，所以需要序列化
     */
    public MySQLTwoPhaseCommitSink() {
        super(new KryoSerializer<>(Connection.class, new ExecutionConfig()), VoidSerializer.INSTANCE);
    }

    @Override
    protected void invoke(Connection transaction, Tuple2<String, Integer> value, Context context) throws Exception {
        PreparedStatement ps = transaction.prepareStatement("insert into `test_word_count` (`word`,`count`) values (?,?,?)");
        ps.setString(1, value.f0);
        ps.setInt(2, value.f1);
        String sqlStr = ps.toString().substring(ps.toString().indexOf(":") + 2);
        logger.error("执行的SQL语句:{}", sqlStr);
        //执行insert语句
        ps.execute();
    }

    @Override
    protected Connection beginTransaction() throws Exception {
        return DBConnectUtils.getConnection();
    }

    @Override
    protected void preCommit(Connection transaction) throws Exception {
        logger.info("start preCommit...");
    }

    @Override
    protected void commit(Connection transaction) {
        try {
            transaction.commit();
        } catch (SQLException throwables) {
            logger.error(throwables.getMessage());
        }
    }

    @Override
    protected void abort(Connection transaction) {
        try {
            transaction.rollback();
        } catch (SQLException throwables) {
            logger.error(throwables.getMessage());
        }
    }
}
