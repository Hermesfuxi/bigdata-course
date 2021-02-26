package bigdata.hermesfuxi.kafka.client;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class TopicJavaDemo {
    public static void main(String[] args) {
        listTopics("hadoop-master:9092");
    }

    public static Properties getKafkaProperties(String bootstrapServers){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("connections.max.idle.ms", 10000);
        properties.put("request.timeout.ms", 5000);
        return properties;
    }

    /**
     * 创建 Topic
     */
    public static void createTopics(String bootstrapServers) {
        Properties properties = getKafkaProperties(bootstrapServers);
        try (AdminClient client = AdminClient.create(properties)) {
            CreateTopicsResult result = client.createTopics(Arrays.asList(
                    new NewTopic("topic1", 1, (short) 1),
                    new NewTopic("topic2", 1, (short) 1),
                    new NewTopic("topic3", 1, (short) 1)
            ));
            try {
                result.all().get();
            } catch (InterruptedException | ExecutionException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    /**
     * 查询 Topic 列表
     */
    public static void listTopics(String bootstrapServers) {
        Properties properties = getKafkaProperties(bootstrapServers);
        try (AdminClient client = AdminClient.create(properties)) {
            ListTopicsResult result = client.listTopics();
            try {
                result.listings().get().forEach(topic -> {
                    System.out.println(topic);
                });
            } catch (InterruptedException | ExecutionException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    /**
     * 查询 Topic 详情
     */
    public static void describeTopicByName(String bootstrapServers, Collection<String> topicNames) {
        Properties properties = getKafkaProperties(bootstrapServers);
        try (AdminClient client = AdminClient.create(properties)) {
            DescribeTopicsResult result = client.describeTopics(topicNames);
            try {
                Map<String, KafkaFuture<TopicDescription>> resultMap =  result.values();
                for (String key : resultMap.keySet()) {
                    KafkaFuture<TopicDescription> kafkaFuture = resultMap.get(key);
                    System.out.println("kafkaFuture = " + kafkaFuture);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 增加分区
     */
    public static void createPartition(String bootstrapServers){
        Properties properties = getKafkaProperties(bootstrapServers);
        try (AdminClient client = AdminClient.create(properties)) {
            Map newPartitions = new HashMap<>();
            // 增加到2个
            newPartitions.put("topic1", NewPartitions.increaseTo(2));
            CreatePartitionsResult rs = client.createPartitions(newPartitions);
            try {
                rs.all().get();
            } catch (InterruptedException | ExecutionException e) {
                throw new IllegalStateException(e);
            }
        }
    }
}
