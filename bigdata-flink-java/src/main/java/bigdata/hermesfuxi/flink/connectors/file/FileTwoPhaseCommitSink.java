package bigdata.hermesfuxi.flink.connectors.file;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;

import java.util.*;

/**
 * @author Hermesfuxi
 * 自定义分两步提交Sink(保证Exactly-Once)
 * TwoPhaseCommitSinkFunction<IN, TXN, CONTEXT> IN：输入数据的类型； TXN：事务数据类型；CONTEXT: 上下文环境类型
 * TwoPhaseCommitSinkFunction保留了5个函数需要子类去实现：
 * invoke：定义了作为sink如何写数据到外部系统。每一个sink都需要定义invoke函数，sink算子每收到一条数据都会触发一次invoke函数，这里的sink函数只是多了一个transaction入参。
 * beginTransaction、preCommit、commit、abort：两阶段提交协议的几个步骤。如果外部系统本身支持两阶段提交（如Kafka），这些函数的实现就是调用外部系统两阶段提交协议对应的函数。
 */
public class FileTwoPhaseCommitSink extends TwoPhaseCommitSinkFunction<String, String, Void> {
    private final HashMap<String, List<String>> hashMap = new HashMap<>();

//    public ExactlyOnceParallelFileSink(TypeSerializer<String> transactionSerializer, TypeSerializer<Void> contextSerializer) {
//        super(transactionSerializer, contextSerializer);
//    }

    /**
     *  事务数据是保存在state状态里，checkpoint后可回复，所以需要序列化
     */
    public FileTwoPhaseCommitSink(){
        super(new KryoSerializer<>(String.class, new ExecutionConfig()), VoidSerializer.INSTANCE);
    }

    // 开启事物: 返回 transaction 对象（这里直接使用 UUID生成）
    @Override
    protected String beginTransaction() throws Exception {
        String transactionId = UUID.randomUUID().toString().replace("-", "");
        System.out.println("beginTransaction run, transaction: " + transactionId);
        return transactionId;
    }

    // task初始化的时候调用
    @Override
    protected void invoke(String transaction, String value, Context context) throws Exception {
        System.out.println("invoke transaction: "  + transaction);
        List<String> list = hashMap.getOrDefault(transaction, new ArrayList<String>());
        if(StringUtils.isNoneBlank(value)){
            list.add(value);
        }
        hashMap.put(transaction, list);
    }

    // 预提交: preCommit是在snapshotState方法中调用的，而snapshotState方法是在算子Checkpoint的时候触发的。
    // 这样就保证了算子在做Checkpoint时，所有该Checkpoint之前的数据都已经安全的发送到了下游（而不是在缓存中）
    // 这里不做处理
    @Override
    protected void preCommit(String transaction) throws Exception {
        System.out.println("preCommit transaction: " + transaction);
    }

    // 如果invoke方法执行正常，则提交事务
    // 这里是直接显示，所以当故障重启后，hashMap里为空，所以显示重复，但若加上Thread.sleep(10000)再显示，就存在数据丢失问题
    @Override
    protected void commit(String transaction) {
        System.out.println("commit transaction: " + transaction);
        List<String> list = hashMap.getOrDefault(transaction, new ArrayList<>());
        for (String s : list) {
            System.out.println(s);
        }
    }

    // 如果invoke执行异常则回滚事物，下一次的checkpoint操作也不会执行
    @Override
    protected void abort(String transaction) {
        System.out.println("abort transaction: " + transaction);
    }
    // abort 过程分析
    // preCommit transaction: ae817021092741c191d418e03f1556fa               预提交旧事务
    // beginTransaction run, transaction: e473c1570e5f4d028de78e6f3455cd70   开启新事务 currentTransaction
    // commit transaction: ae817021092741c191d418e03f1556fa                  提交旧事务
    // 1.txt : (flink,21)
    // 4.txt : (spark,33)
    // ---------------------  发生故障  ---------------------
    // abort transaction: e473c1570e5f4d028de78e6f3455cd70                   生命周期方法 close() 调用 abort(currentTransactionHolder.handle)
    // commit transaction: ae817021092741c191d418e03f1556fa                  重启后恢复数据时，调用 recoverAndCommitInternal 再提交旧事务
    // abort transaction: e473c1570e5f4d028de78e6f3455cd70                   恢复数据后时，调用 recoverAndAbort 再回滚新事务，表明新事务被丢弃
    // beginTransaction run, transaction: 13380fbe5f3b4c8aa55dab76b010db20   再次开启全事务
}
