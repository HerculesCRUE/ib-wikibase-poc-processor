package core.kafka;

import core.Config.Config;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class KafkaHandler  {

    final Logger logger = LogManager.getLogger(KafkaHandler.class);
    private static KafkaHandler instance = null;
    private static HashMap<String, KProducer> producers;
    private static HashMap<String,KConsumerObj> consumers;
    private static Set<KafkaListener> listeners;
    private static String kafkaHost;
    private static String kafkaPort;
    private static boolean isStarted;


    public static KafkaHandler getInstance(KafkaListener listener) {
        if (instance == null) {
            instance = new KafkaHandler();
        }
        listeners.add(listener);
        return instance;
    }

    private KafkaHandler() {
        isStarted = false;
        consumers = new HashMap<String, KConsumerObj>();
        producers = new HashMap<String, KProducer>();
        listeners = new HashSet<KafkaListener>();
        kafkaHost = (String) Config.getInstance().getValue("kafka.host", "localhost");
        kafkaPort = (String) Config.getInstance().getValue("kafka.port", "localhost");
    }

    // CONSUMERS
    public  void addConsumer(String id, String groupId, List<String> topics, boolean start,KConsumerListener listener) {

        KConsumerObj kConsumerObj;
        if (!consumers.containsKey(id)) {
            kConsumerObj = new KConsumerObj(id,kafkaHost,kafkaPort,groupId,topics, listener);
            consumers.put(id,kConsumerObj);
        } else {
            kConsumerObj = consumers.get(id);
        }
        if (start) {
            kConsumerObj.init();
        }
    }

    public  void addConsumer(String id, String groupId, List<String> topics, boolean start) {
        KConsumerObj kConsumerObj;
        if (!consumers.containsKey(id)) {
            kConsumerObj = new KConsumerObj(id,kafkaHost,kafkaPort,groupId,topics);
            consumers.put(id,kConsumerObj);
        } else {
            kConsumerObj = consumers.get(id);
        }
        if (start) {
            kConsumerObj.init();
        }
    }

    public  void startConsumer(String id) {
        if (!consumers.containsKey(id)) {
            consumers.get(id).init();
        } else {
            logger.warn("Kafka Consumer ("+id+") Not exist");
        }
    }

    public  void stopConsumer(String id) {
        if (!consumers.containsKey(id)) {
            consumers.get(id).stop();
        } else {
            logger.warn("Kafka Consumer ("+id+") Not exist");
        }
    }

    public  void removeConsumer(String id) {
        stopConsumer(id);
        consumers.remove(id);
    }

    // PRODUCERS
    public  KProducer addProducer(String id ,KProducerListener listener) {

        KProducer producer;
        if (!producers.containsKey(id)) {
            Properties props = getDefaultProducerProperties(kafkaHost,kafkaPort);
            producer = new KProducer(id,props,listener);
            producers.put(id,producer);
        } else {
            producer = producers.get(id);
            producer.addListener(listener);
        }
        return producer;
    }

    public  KProducer addProducer(String id) {

        KProducer producer;
        if (!producers.containsKey(id)) {
            Properties props = getDefaultProducerProperties(kafkaHost,kafkaPort);
            producer = new KProducer(id,props);
            producers.put(id,producer);
        } else {
            producer = producers.get(id);
        }
        return producer;
    }

    public void sendMessage(String id,String topic, String key, String message) {
        if (!producers.containsKey(id)) {
            logger.warn("Kafka Producer ("+id+") not Exist");
        } else {
            producers.get(id).send(topic,key,message);
        }
    }

    public void sendMessage(String id,String topic, String message) {
        if (!producers.containsKey(id)) {
            logger.warn("Kafka Producer ("+id+") not Exist");
        } else {
            producers.get(id).send(topic,message);
        }
    }

    public  void removeProducer(String id) {
        if (producers.containsKey(id)) {
            try {
                producers.get(id).close();
                logger.warn("Kafka Producer ("+id+") Closed");
                producers.remove(id);
            } catch (Exception e) {
                logger.warn("Kafka Producer ("+id+") Close Error");
                e.printStackTrace();
                producers.remove(id);
            }
        } else {
            logger.warn("Kafka Producer ("+id+") Not exits");
        }
    }

    // ANY
    public void addListener(String id, KListener listener) {
        if (listener instanceof KProducerListener) {
            producers.get(id).addListener((KProducerListener) listener);
        } else if (listener instanceof KConsumerListener) {
            consumers.get(id).consumer.addListener((KConsumerListener) listener);
        }
    }

    public void removeListener(String id, KListener listener) {
        if (listener instanceof KProducerListener) {
            producers.get(id).removeListener((KProducerListener) listener);
        } else if (listener instanceof KConsumerListener) {
            consumers.get(id).consumer.removeListener((KConsumerListener) listener);
        }
    }

    public Properties getDefaultProducerProperties(String host,String port) {
        Properties props = new Properties();
        props.put("bootstrap.servers", host+":"+port);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    public Properties getDefaultConsumerProperties(String host,String port,String groupId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", host+":"+port);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        return props;
    }


    class KConsumerObj {
        private ExecutorService executorService;
        private KConsumer consumer;

        public KConsumerObj(String id,String host, String port, String groupId,
                            List<String> topics, KConsumerListener listener) {
            Properties props = getDefaultConsumerProperties(kafkaHost,kafkaPort,groupId);
            this.consumer = new KConsumer(id,topics, props, listener);
            this.executorService = Executors.newSingleThreadExecutor();
        }

        public KConsumerObj(String id,String host, String port, String groupId,
                            List<String> topics) {
            Properties props = getDefaultConsumerProperties(kafkaHost,kafkaPort,groupId);
            this.consumer = new KConsumer(id,topics, props);
            this.executorService = Executors.newSingleThreadExecutor();
        }

        public void init() {
            if (!this.consumer.isRunning()) {
                this.executorService.submit(this.consumer);
                logger.info("Kafka Consumer ("+this.consumer.getId()+") Will be Started");
            } else {
                logger.warn("Kafka Consumer ("+this.consumer.getId()+") Initialized yet");
            }
        }

        public void stop() {
            if (this.consumer.isRunning()) {
                this.executorService.shutdown();
                try {
                    if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                        executorService.shutdownNow();
                    }
                } catch (InterruptedException ex) {
                    executorService.shutdownNow();
                    Thread.currentThread().interrupt();
                }
                logger.info("Kafka Consumer ("+this.consumer.getId()+") Will be Stopped");
            } else {
                logger.warn("Kafka Consumer ("+this.consumer.getId()+") Stopped yet");
            }
        }

    }
}
