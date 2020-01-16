package core.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;


import java.util.*;
import java.util.concurrent.ExecutorService;

public class KConsumer implements Runnable{

    private String id;
    private KafkaConsumer<String, String> consumer;
    private Set<KConsumerListener> listeners;
    private Set<String> topics;
    private boolean isRunning = false;


    public KConsumer(String id,
                     List<String> topics, Properties props) {
        this.id = id;
        this.consumer = new KafkaConsumer(props);
        this.listeners = new HashSet<>();
        this.topics = new HashSet<>(topics);
    }

    public KConsumer(String id,
                     List<String> topics, Properties props,KConsumerListener listener) {
        this(id, topics, props);
        addListener(listener);
    }

    public void addListener(KConsumerListener listener) {
        this.listeners.add(listener);
    }

    public void removeListener(KConsumerListener listener) {
        listeners.remove(listener);
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(topics);

            while (true) {
                isRunning = true;
                ConsumerRecords<String,String> records = consumer.poll(1000);
                for (ConsumerRecord<String,String> record: records) {
                    for (KConsumerListener listener : this.listeners) {
                        listener.onMessageIsReady(this.id, record.topic(), record.key(),record.partition(), record.offset(),record.value());
                    }
                }
            }
        } catch (WakeupException e) {
            e.printStackTrace();
        } finally {
            isRunning = false;
            consumer.close();
        }
    }


    public String getId() {
        return id;
    }


    public KafkaConsumer<String, String> getConsumer() {
        return consumer;
    }


    public Set<KConsumerListener> getListeners() {
        return listeners;
    }

    public Set<String> getTopics() {
        return topics;
    }

    public boolean isRunning() {
        return isRunning;
    }

    public void shutdown() {
        System.out.println("In the consumer shutdown: "+this.id);
        consumer.wakeup();
    }
}
