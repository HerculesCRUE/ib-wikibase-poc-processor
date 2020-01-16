package core.kafka;

public interface KProducerListener extends KListener{
    void onMessageIsSend(String id);
    void onMessageFail(String id,Exception e);
}
