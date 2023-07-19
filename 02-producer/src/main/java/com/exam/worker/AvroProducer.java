package com.exam.worker;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;
import java.util.ResourceBundle;

public class AvroProducer {
    private final ResourceBundle RESOURCE_BUNDLE = ResourceBundle.getBundle("config");
    private final Logger logger = LoggerFactory.getLogger(AvroProducer.class);

    // Kafka Producer 설정
    private final KafkaProducer<String, GenericRecord> producer;
    private final AdminClient adminClient;

    // Message 설정
    private final int MESSAGE_COUNT_PER_TOPIC;  // 생성할 메시지 수

    private static final Random RANDOM = new Random();

    private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    public AvroProducer() {

        // AdminClient 설정
        Properties props = new Properties();
        String bootstrapServers = RESOURCE_BUNDLE.getString("kafka.bootstrap.servers");
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        this.adminClient = AdminClient.create(props);

        // Producer 설정
        props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url", RESOURCE_BUNDLE.getString("kafka.schema.registry.url"));
        this.producer = new KafkaProducer<>(props);

        // Message 설정
        this.MESSAGE_COUNT_PER_TOPIC = Integer.parseInt(RESOURCE_BUNDLE.getString("message.count.per.topic"));
    }

    public void createMessages(int ThreadId, Schema schema) {

        String topicName = schema.getName();
        int partitionCount = 0;
        // AdminClient 를 통해 Topic 명으로 Partition 개수 를 구헙니다.
        try {
            DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Collections.singletonList(topicName));
            partitionCount = describeTopicsResult.all().get().get(topicName).partitions().size();
            if(partitionCount < 1)
                throw new RuntimeException("topic partition is empty..");
        } catch (Exception e) {
            logger.error("Error occurred in Admin client", e);
        }

        // Avro 스키마 포맷의 메시지를 Topic 당 정해진 개수만큼 생성합니다.
        try{
            for (int i = 0; i < MESSAGE_COUNT_PER_TOPIC; i++) {
                GenericRecord message = new GenericData.Record(schema);
                // Field 타입에 따라 랜덤한 값을 생성하여 생산할 메시지를 만듭니다.
                for(Schema.Field field : schema.getFields()){
                    message.put(field.name(), generateValue(field.schema()));
                }
                // 메시지의 Key 값에 따라 동일한 Partition 에 분배되어 들어가도록 partitionId를 구합니다. -> 해싱함수 사용
                // 동일한 Key 의 데이터는 동일한 Partition 에 저장되어야 파티션 병렬 처리하여도 데이터의 순서가 보장됩니다.
                String key = message.get(0).toString();
                int partitionId = Math.abs(key.hashCode() % partitionCount);
                ProducerRecord<String, GenericRecord> producerRecord = new ProducerRecord<>(topicName, partitionId, key, message);
                producer.send(producerRecord);
            }
            logger.info(String.format("thread %s kafka producer messeage send successfully... %s", ThreadId, topicName));

            // 메시지 생산 작업이 완료되면 프로듀서 객체를 종료합니다.
            producer.flush();
            producer.close();

        } catch (Exception e) {
            logger.error("Error occurred in Producing messages", e);
        }
    }

    private static Object generateValue(Schema fieldSchema) {
        // Field 스키마 타입에 따라 랜덤한 값을 생성하여 반환합니다.
        switch (fieldSchema.getType()) {
            case INT:
                int integerValue = RANDOM.nextInt(100) + 1;
                return integerValue;
            case LONG:
                return System.currentTimeMillis();
            case FLOAT:
                float floatValue = Float.parseFloat(new DecimalFormat("#.###").format(RANDOM.nextFloat()));
                return floatValue;
            case DOUBLE:
                double doubleValue = Double.parseDouble(new DecimalFormat("#.######").format(RANDOM.nextDouble()));
                return doubleValue;
            case STRING:
                StringBuilder stringValue = new StringBuilder();
                for (int i = 0; i < 6; i++) {
                    int index = new Random().nextInt(CHARACTERS.length());
                    char ch = CHARACTERS.charAt(index);
                    stringValue.append(ch);
                }
                return stringValue.toString();

            default:
                throw new IllegalArgumentException("Unsupported type: " + fieldSchema.getType());
        }
    }


}
