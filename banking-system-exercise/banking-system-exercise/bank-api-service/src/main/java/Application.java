/*
 *  MIT License
 *
 *  Copyright (c) 2019 Michael Pogrebinsky - Distributed Systems & Cloud Computing with Java
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a copy
 *  of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in all
 *  copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  SOFTWARE.
 */

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Banking API Service
 */
public class Application {
    private static final String SUSPICIOUS_TRANSACTIONS_TOPIC = "suspicious-transactions_1";
    private static final String VALID_TRANSACTIONS_TOPIC = "valid-transactions";

    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";

    public static void main(String[] args) {
        Producer<String, Transaction> kafkaProducer = createKafkaProducer(BOOTSTRAP_SERVERS);

        try {
            processTransactions(new IncomingTransactionsReader(), new UserResidenceDatabase(), kafkaProducer);
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            kafkaProducer.flush();
            kafkaProducer.close();
        }
    }

    public static void processTransactions(IncomingTransactionsReader incomingTransactionsReader,
                                           UserResidenceDatabase userResidenceDatabase,
                                           Producer<String, Transaction> kafkaProducer) throws ExecutionException, InterruptedException {

        while (incomingTransactionsReader.hasNext()) {
            Transaction transaction = incomingTransactionsReader.next();

            /**
             * Fill in you code here.
             * Send the transaction to the right topic based on the origin of the transaction and the user's residence data
             */

            String user = transaction.getUser();

            String realResidence = userResidenceDatabase.getUserResidence(user);

            ProducerRecord<String, Transaction> record;

            if (transaction.getTransactionLocation().equals(realResidence)) {
                record = new ProducerRecord<>(VALID_TRANSACTIONS_TOPIC, user, transaction);

            } else {
                record = new ProducerRecord<>(SUSPICIOUS_TRANSACTIONS_TOPIC, user, transaction);
            }

            RecordMetadata recordMetadata = kafkaProducer.send(record).get();

            System.out.println(String.format("Record with (key: %s, value: %s), was sent to (partition: %d, offset: %d",
                    record.key(), record.value(), recordMetadata.partition(), recordMetadata.offset()));

        }
    }

    public static Producer<String, Transaction> createKafkaProducer(String bootstrapServers) {
        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "banking-api-service");
        // 사용자 이름을 키로 해서, 해시(사용자 이름)대로 파티션에 집어 넣는다. 그래야 파티션 내 동일한 사용자 요청의 순서를 지킬 수 있다.
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Transaction.TransactionSerializer.class.getName());

        return new KafkaProducer<>(properties);
    }

}
