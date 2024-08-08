package wiki.xyh.job;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerProducerExample {

    public static void main(String[] args) {
        String bootstrapServers = "xyh-01:9092,xyh-02:9092,xyh-03:9092"; // 修改为你的Kafka服务器地址
        String groupId = "group-03"; // 修改为你的消费组ID
        String sourceTopic = "gkws-es";
        String targetTopic = "test";
        int maxRecords = 10000;

        // 配置消费者
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(sourceTopic));

        // 配置生产者
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        try {
            int recordCount = 0;

            while (recordCount < maxRecords) {
                ConsumerRecords<String, String> records = consumer.poll(1000);

                for (ConsumerRecord<String, String> record : records) {
                    producer.send(new ProducerRecord<>(targetTopic, record.key(), record.value()));
                    recordCount++;

                    if (recordCount >= maxRecords) {
                        break;
                    }
                }
            }
        } finally {
            consumer.close();
            producer.close();
        }
    }
}
