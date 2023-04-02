import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Application {
    public static final String TOPIC="events";

    public static final String BOOTSTRAP_SEVERS="localhost:9091,localhost:9092,localhost:9093";

    public static void main(String[] args) {
        String consumerGroup="defaultConsumerGroup";

        if(args.length==1){
            consumerGroup=args[0];
        }

        System.out.println("Consumer is part of consumer group " + consumerGroup);

        Consumer<Long,String>kafkaConsumer=createKafkaConsumer(BOOTSTRAP_SEVERS,consumerGroup);

        consumeMessages(TOPIC,kafkaConsumer);
    }

    public static void consumeMessages(String topic, Consumer<Long, String> kafkaConsumer) {
        kafkaConsumer.subscribe(Collections.singletonList(topic));

        while (true) {
            ConsumerRecords<Long, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));

            if (consumerRecords.isEmpty()) {
                //do something else!
            }

            for (ConsumerRecord<Long, String> record : consumerRecords) {
                System.out.println(String.format("Received record (key: %d, value: %s, partition: %d, offset: %d",
                        record.key(), record.value(), record.partition(), record.offset()));
            }

            //do something with the records

            kafkaConsumer.commitAsync();//컨슈머가 해당 메시지를 소비했다는 것을 카프카에게 알려줌.
        }
    }

    public static Consumer<Long, String> createKafkaConsumer(String bootstrapSevers, String consumerGroup) {
        Properties properties = new Properties();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapSevers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        return new KafkaConsumer<Long, String>(properties);
    }
}
