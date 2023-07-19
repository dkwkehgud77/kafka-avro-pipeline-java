import com.exam.worker.AvroConsumer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class Main {

    private static final ResourceBundle RESOURCE_BUNDLE = ResourceBundle.getBundle("config");
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        logger.info("kafka consumer application start...");

        // AdminClient 설정을 구성하고, config.properties 에서 컨슈밍 할 Kafka 토픽을 가져옵니다.
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, RESOURCE_BUNDLE.getString("kafka.bootstrap.servers"));
        String[] topics = RESOURCE_BUNDLE.getString("kafka.topics").split(",");
        Map<String, Integer> topicPartitions  = new HashMap<>();

        int nThreads = 0;
        // AdminClient 를 통해 Topic 명으로 Partition 개수 를 구해서 Map 자료구조에 담습니다.
        try (AdminClient adminClient = AdminClient.create(properties)) {
            for(String topicName: topics) {
                DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Collections.singletonList(topicName));
                int partitionCount = describeTopicsResult.all().get().get(topicName).partitions().size();
                topicPartitions.put(topicName, partitionCount);
                // 각 Partition 개수를 합하여 스레드 풀 내부에서 생성될 스레드의 개수를 구합니다.
                nThreads += partitionCount;
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        // 모든 Topic 의 Partition 개수를 합한 만큼 Thread 를 생성하는 스레드 풀을 구성합니다.
        ExecutorService executor_topic = Executors.newFixedThreadPool(nThreads);
        for (Map.Entry<String, Integer> entry : topicPartitions.entrySet()) {
            String topicName = entry.getKey();
            int partitionCount = entry.getValue();

            // 각 Topic 의 Partition 개수 만큼 loop 를 돌아 각 스레드에 할당하여...
            for (int i = 0; i < partitionCount; i++) {
                int partitionId = i;
                // 각 스레드 에서 AvroConsumer 객체를 생성하고 메시지를 소비를 시작합니다.
                executor_topic.execute(() -> {
                    try {
                        AvroConsumer consumer = new AvroConsumer(topicName, partitionId, partitionCount);
                        consumer.consumeMesseages();
                        logger.info(String.format("kafka consumer started successfully.. topic: %s, patition: %s", topicName, partitionId));

                    } catch (Exception e){
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                });
            }
        }

    }
}

