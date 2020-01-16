import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import core.kafka.KConsumerListener;
import core.kafka.KProducerListener;
import core.kafka.KafkaHandler;
import core.utils.LoggerHandler;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.wikidata.wdtk.datamodel.helpers.Datamodel;
import org.wikidata.wdtk.datamodel.interfaces.*;
import services.WikibaseHandlerImpl;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;

public class WikiBaseProcessor {

    static KafkaHandler kh = KafkaHandler.getInstance(null);
    final static LoggerHandler loggerHandler = LoggerHandler.getInstance();
    final static Logger logger = LogManager.getLogger(ManagementSystem.class);
    final static Semaphore semaphore = new Semaphore(1);
    final static WikibaseHandlerImpl wbhi = WikibaseHandlerImpl.getInstance();

    final static String ENTITIES_SITE_URI = "http://localhost:8181/entity/";


    public static void main(String[] args) {

        org.apache.log4j.Logger.getLogger("org").setLevel(Level.WARN);
        org.apache.log4j.Logger.getLogger("akka").setLevel(Level.WARN);
        org.apache.log4j.Logger.getLogger("kafka").setLevel(Level.WARN);

        KProducerListener kProducerListener = new KProducerListener() {
            @Override
            public void onMessageIsSend(String id) {

            }

            @Override
            public void onMessageFail(String id, Exception e) {
            }
        };

        kh.addProducer("success-queue", new KProducerListener() {
            @Override
            public void onMessageIsSend(String id) {

            }

            @Override
            public void onMessageFail(String id, Exception e) {

            }
        });

        kh.addConsumer("success-queue", "data-queue", Collections.singletonList("data-queue"), true, new KConsumerListener() {
            @Override
            public void onMessageIsReady(String id, String topic, String key, int partition, long offset, String message) {
                JsonObject jMessage = new JsonParser().parse(message).getAsJsonObject();
                String idMessage = jMessage.get("id").getAsString();
                String type = jMessage.get("type").getAsString();
                JsonObject jData = jMessage.get("data").getAsJsonObject();
                System.out.println("recived TYPE: ["+type+"]\tID: ["+id+"]\tCONTENT: "+jData.toString());
                handleWikidataUpdate(jData);
                kh.sendMessage("success-queue","success-queue",message);
            }
        });

    }

    private static void handleWikidataUpdate(JsonObject jData) {
        Map<String,String> labels = null;
        Map<String,String> descriptions = null;
        if (jData.has("Label_es") || jData.has("Label_en")) {
            labels = new HashMap<>();
            if (jData.has("Label_es")) {
                labels.put("es",jData.get("Label_es").getAsString());
            }
            if (jData.has("Label_en")) {
                labels.put("en",jData.get("Label_en").getAsString());
            }
        }

        if (jData.has("Description_es") || jData.has("Description_en")) {
            descriptions = new HashMap<>();
            if (jData.has("Description_es")) {
                descriptions.put("es",jData.get("Description_es").getAsString());
            }
            if (jData.has("Description_en")) {
                descriptions.put("en",jData.get("Description_en").getAsString());
            }
        }
        if (labels.size()>0) {
            ItemDocument insertedItem = null;
            for (Map.Entry<String, String> l : labels.entrySet()) {
                insertedItem = wbhi.doUpsertItem(null, l.getValue(),l.getKey(),labels,descriptions,null,ENTITIES_SITE_URI);
                if (insertedItem!=null) {
                    break;
                }
            }
            if (insertedItem!=null) {
                for (Map.Entry<String, JsonElement> att:jData.entrySet()) {
                    String key = att.getKey();
                    String value = att.getValue().getAsString();
                    if (key.trim().equals("Label_es") || key.trim().equals("Label_en")
                            || key.trim().equals("Description_es") || key.trim().equals("Description_en")
                            || key.trim().equals("Aliases_es") || key.trim().equals("Aliases_en")
                    ) {
                        continue;
                    } else {
                        String []splitedKey = key.split(":");
                        if (splitedKey.length == 3) {
                            PropertyDocument pd = (PropertyDocument) wbhi.getDocumentById(splitedKey[1],ENTITIES_SITE_URI);
                            Statement statement;
                            if (splitedKey[splitedKey.length-1].trim().toLowerCase().equals("wikibase-item")) { // Es una entidad
                                if (value.toUpperCase().contains("I:")) {
                                    value = value.replace("I:","");
                                    statement = wbhi.generateStatement(insertedItem.getEntityId(),pd.getEntityId(), Datamodel.makeItemIdValue(value,ENTITIES_SITE_URI));
                                } else {
                                    String []values = value.split(":");
                                    EntityDocument ed = wbhi.searchFirstInItem(values[1],values[0],ENTITIES_SITE_URI);
                                    statement = wbhi.generateStatement(insertedItem.getEntityId(),pd.getEntityId(), Datamodel.makeItemIdValue(ed.getEntityId().getId(),ENTITIES_SITE_URI));
                                }
                            } else {
                                statement = wbhi.generateStatement(insertedItem.getEntityId(),pd.getEntityId(), Datamodel.makeStringValue(value));
                            }
                            if (statement!=null)
                                insertedItem = wbhi.updateStatementInItem(insertedItem,statement,ENTITIES_SITE_URI);
                        }
                    }
                }
            }

        }
    }
}
