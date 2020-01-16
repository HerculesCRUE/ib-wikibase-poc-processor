
import core.kafka.*;
import org.apache.log4j.Level;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


public class Main implements KProducerListener{
    public static void main(String[] args) throws InterruptedException {
        //final Logger logger = LogManager.getLogger(Main.class);
        // Config config = Config.getInstance();

        /*
        org.apache.log4j.Logger.getLogger("org").setLevel(Level.WARN);
        org.apache.log4j.Logger.getLogger("akka").setLevel(Level.WARN);
        org.apache.log4j.Logger.getLogger("kafka").setLevel(Level.WARN);

        List<String> topics = Arrays.asList("consumer-tutorial");
        KafkaHandler kh = KafkaHandler.getInstance(null);

        String text = "En un lugar de la mancha de cuyo nombre no quiero acordarme vivia el ingenioso hidalgo don quijote de la mancha";

        KProducerListener kProducerListener = new KProducerListener() {
            @Override
            public void onMessageIsSend(String id) {

                System.out.println("Producer["+id+"] sent message success");
            }

            @Override
            public void onMessageFail(String id, Exception e) {
                System.out.println("Producer["+id+"] sent message fail");
            }
        };

        int count = 0;
        for (String word :text.split(" ")) {
            kh.addProducer(Integer.toString(count % 3), kProducerListener);
            kh.sendMessage(Integer.toString(count % 3),"consumer-tutorial",word);
            count++;
        }

        for (int i = 0 ; i < 3 ; i++) {
            kh.addConsumer(Integer.toString(i), "consumer-tutorial-group", topics, true, new KConsumerListener() {
                @Override
                public void onMessageIsReady(String id, String topic, String key, int partition, long offset, String message) {
                    System.out.println("Id["+id+"], Topic["+topic+"], Key["+key+"], Partition["+partition+"], offset["+offset+"], message["+message+"]");
                }
            });
        }
        */

        /*
        int numConsumers = 3;
        String groupId = "consumer-tutorial-group";
        List<String> topics = Arrays.asList("consumer-tutorial");
        final ExecutorService executor = Executors.newFixedThreadPool(numConsumers);


        final List<KConsumer> consumers = new ArrayList<>();

        for (int i = 0; i < numConsumers ; i++) {
            KConsumer consumer = new KConsumer(Integer.toString(i),"localhost","9092",groupId,topics);
            consumers.add(consumer);
            executor.submit(consumer);
        }
        */
        /*
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                for (KConsumer consumer  : consumers) {
                    System.out.println("Estoy aqui");
                    consumer.shutdown();
                }
                executor.shutdown();
                try {
                    executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
         */
    }

    @Override
    public void onMessageIsSend(String id) {
        System.out.println("Message ["+id+"] sent");
    }

    @Override
    public void onMessageFail(String id, Exception e) {
        System.out.println("Message ["+id+"] Fail");
    }
}
