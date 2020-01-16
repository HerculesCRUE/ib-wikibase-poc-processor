import core.kafka.KConsumerListener;
import core.kafka.KProducerListener;
import core.kafka.KafkaHandler;
import org.apache.log4j.Level;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.Assert.assertEquals;

public class KafkaTest {

    @Test
    public void testSimpleKakfa() throws ExecutionException, InterruptedException {

        org.apache.log4j.Logger.getLogger("org").setLevel(Level.WARN);
        org.apache.log4j.Logger.getLogger("akka").setLevel(Level.WARN);
        org.apache.log4j.Logger.getLogger("kafka").setLevel(Level.WARN);

        List<String> topics = Arrays.asList("consumer-tutorial");
        KafkaHandler kh = KafkaHandler.getInstance(null);

        final String sentMessage = "Test Msg 2";
        CompletableFuture<String> future = new CompletableFuture<>();
        ExecutorService executorService  = Executors.newSingleThreadExecutor();
        executorService.submit(new Runnable() {
            @Override
            public void run() {

                kh.addProducer("1", new KProducerListener() {
                    @Override
                    public void onMessageIsSend(String id) {
                        System.out.println("Producer["+id+"] sent message success");
                    }

                    @Override
                    public void onMessageFail(String id, Exception e) {
                        System.out.println("Producer["+id+"] sent message fail");
                    }
                });

                kh.sendMessage("1","consumer-tutorial",sentMessage);

                kh.addConsumer(Integer.toString(1), "consumer-tutorial-group", topics, true, new KConsumerListener() {
                    @Override
                    public void onMessageIsReady(String id, String topic, String key, int partition, long offset, String message) {
                        System.out.println("Id["+id+"], Topic["+topic+"], Key["+key+"], Partition["+partition+"], offset["+offset+"], message["+message+"]");
                        future.complete(message);
                    }
                });
            }
        });

        assertEquals(sentMessage, future.get());

    }
}
