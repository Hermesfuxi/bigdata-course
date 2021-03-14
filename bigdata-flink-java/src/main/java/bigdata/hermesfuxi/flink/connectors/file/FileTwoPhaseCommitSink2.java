package bigdata.hermesfuxi.flink.connectors.file;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;

import java.util.*;

/**
 * @author Hermesfuxi
 * 自定义分两步提交Sink(保证Exactly-Once)
 * TwoPhaseCommitSinkFunction保留了5个函数需要子类去实现：
 * invoke：定义了作为sink如何写数据到外部系统。每一个sink都需要定义invoke函数，sink算子每收到一条数据都会触发一次invoke函数，这里的sink函数只是多了一个transaction入参。
 * beginTransaction、preCommit、commit、abort：两阶段提交协议的几个步骤。如果外部系统本身支持两阶段提交（如Kafka），这些函数的实现就是调用外部系统两阶段提交协议对应的函数。
 */
public class FileTwoPhaseCommitSink2 extends TwoPhaseCommitSinkFunction<String, List<String>, Void> {
    /**
     * Example:
     *
     * <pre>{@code
     * TwoPhaseCommitSinkFunction(TypeInformation.of(new TypeHint<State<TXN, CONTEXT>>() {}));
     * }</pre>
     *
     * @param transactionSerializer {@link TypeSerializer} for the transaction type of this sink
     * @param contextSerializer     {@link TypeSerializer} for the context type of this sink
     */
    public FileTwoPhaseCommitSink2(TypeSerializer<List<String>> transactionSerializer, TypeSerializer<Void> contextSerializer) {
        super(transactionSerializer, contextSerializer);
    }

    public FileTwoPhaseCommitSink2() {
        super(new KryoSerializer(Collections.<String>emptyList().getClass(), new ExecutionConfig()), VoidSerializer.INSTANCE);
    }

    @Override
    protected void invoke(List<String> transaction, String value, Context context) throws Exception {
        transaction.add(value);
    }

    @Override
    protected List<String> beginTransaction() throws Exception {
        return new ArrayList<>();
    }

    @Override
    protected void preCommit(List<String> transaction) throws Exception {
        System.out.println("preCommit transaction: " + transaction);
    }

    @Override
    protected void commit(List<String> transaction) {
        System.out.println("commit transaction: " + transaction);
        for (String s : transaction) {
            System.out.println(s);
        }
    }

    @Override
    protected void abort(List<String> transaction) {
        System.out.println("abort transaction: " + transaction);
    }
}
