package core.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;

public class KProducer implements AutoCloseable{

    private String id;
    private Properties props;
    private KafkaProducer producer;

    private Set<KProducerListener> listeners;

    public KProducer(String id, Properties props, KProducerListener listener) {
        this(id, props);
        if (listener!=null)
            this.listeners.add(listener);
    }

    public KProducer( String id, Properties props) {
        this.id = id;
        this.props = props;
        this.producer = new KafkaProducer<>(props);
        this.listeners = new HashSet<>();
    }

    public void addListener(KProducerListener listener) {
        listeners.add(listener);
    }

    public void removeListener(KProducerListener listener) {
        listeners.remove(listener);
    }

    public void send(String topic, String key, String message) {
        try {
            producer.send(new ProducerRecord(topic, key, message));
        } catch (Exception e) {
            e.printStackTrace();
            for (KProducerListener listener : this.listeners) {
                listener.onMessageFail(getId(), e);
            }
        }
        for (KProducerListener listener : this.listeners) {
            listener.onMessageIsSend(getId());
        }
    }

    public void send(String topic, String message) {
        try {
            producer.send(new ProducerRecord(topic, message));
        } catch (Exception e) {
            e.printStackTrace();
            for (KProducerListener listener : this.listeners) {
                listener.onMessageFail(getId(), e);
            }
        }
        for (KProducerListener listener : this.listeners) {
            listener.onMessageIsSend(getId());
        }
    }


    @Override
    public void close() throws Exception {
        producer.close();
    }

    @Override
    protected void finalize() throws Throwable {
        producer.close();
        super.finalize();
    }

    public String getId() {
        return id;
    }

    public Properties getProps() {
        return props;
    }

    public KafkaProducer getProducer() {
        return producer;
    }

    public Set<KProducerListener> getListeners() {
        return listeners;
    }
}
