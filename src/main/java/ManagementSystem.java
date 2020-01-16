import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import core.kafka.KProducerListener;
import core.kafka.KafkaHandler;
import core.utils.CSVHandler;
import core.utils.LoggerHandler;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import scala.Int;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.stream.Stream;

public class ManagementSystem {


    static KafkaHandler kh = KafkaHandler.getInstance(null);
    final static LoggerHandler loggerHandler = LoggerHandler.getInstance();
    final static Logger logger = LogManager.getLogger(ManagementSystem.class);
    final static Semaphore semaphore = new Semaphore(1);

    public static void main(String[] args) throws ClassNotFoundException, URISyntaxException, InterruptedException {
        //getJsonDataFromCSVResourcesFile("academic_institution.csv");
        org.apache.log4j.Logger.getLogger("org").setLevel(Level.WARN);
        org.apache.log4j.Logger.getLogger("akka").setLevel(Level.WARN);
        org.apache.log4j.Logger.getLogger("kafka").setLevel(Level.WARN);
        semaphore.acquire();
        semaphore.acquire();
        System.out.println("Fin colega");
        hadleSendAcademicData();
    }

    private static void hadleSendAcademicData() {

        logger.info("*** Starting send Academic Data by Kafka to Processor ***");

        JsonArray jAcademicData = getJsonDataFromCSVResourcesFile("academic_institution.csv");

        Map<String,String> sended = new HashMap<>();
        for (int i = 0; i < jAcademicData.size() ; i++) {
            sended.put(String.valueOf(i),jAcademicData.get(i).toString());
        }

        KProducerListener kProducerListener = new KProducerListener() {
            @Override
            public void onMessageIsSend(String id) {
                logger.info("\t--- Sending DONE Academic Data by Kafka id: ["+id+"], content: "+sended.get(id) );
            }

            @Override
            public void onMessageFail(String id, Exception e) {
                logger.error("\t--- Sending ERROR Academic Data by Kafka id: ["+id+"], content: "+sended.get(id) );
            }
        };

        logger.info("\t--- Processing ("+jAcademicData.size()+") instances");
        for (int i = 0; i < jAcademicData.size() ; i++) {
            kh.addProducer(Integer.toString(i), kProducerListener);
            kh.sendMessage(Integer.toString(i),"data-queue",jAcademicData.get(i).toString());
        }
    }

    private static JsonArray getJsonDataFromCSVResourcesFile(String name){
        try {
            URL res = Class.forName("ManagementSystem").getClassLoader().getResource(name);
            File file = Paths.get(res.toURI()).toFile();

            JsonArray jData = CSVHandler.parseCSV(file.getAbsolutePath());
            return jData;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return null;
    }

}
