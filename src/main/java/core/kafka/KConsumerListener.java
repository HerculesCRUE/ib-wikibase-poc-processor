package core.kafka;

public interface KConsumerListener extends KListener{
    void onMessageIsReady(String id,String topic, String key,int partition, long offset, String message);
}
