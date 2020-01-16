import core.Config.Config;
import core.utils.LoggerHandler;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.wikidata.wdtk.datamodel.helpers.Datamodel;
import org.wikidata.wdtk.datamodel.implementation.DatatypeIdImpl;
import org.wikidata.wdtk.datamodel.interfaces.*;
import org.wikidata.wdtk.wikibaseapi.ApiConnection;
import org.wikidata.wdtk.wikibaseapi.LoginFailedException;
import services.WikibaseHandlerImpl;

import javax.xml.datatype.DatatypeFactory;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class WikiBaseTest {
    ApiConnection con;
    Config config;
    Logger logger;
    WikibaseHandlerImpl wbhi;


    // Logger logger = LogManager.getLogger(WikiBaseTest.class);

    public WikiBaseTest() {
        LoggerHandler.getInstance().getRootLogger();
        logger = LogManager.getLogger(WikiBaseTest.class);
        wbhi = WikibaseHandlerImpl.getInstance();
    }


    @Test
    public void checkIfLoginSuccessTest() {
        assertTrue(wbhi.doConnection());
    }

    @Test
    public void checkItemById() {
        ItemDocument item = (ItemDocument) wbhi.getDocumentById("Q17","http://localhost:8181/entity/");
        // Check Label (es)
        assertEquals(item.getLabels().get("es").getText(),"Investigador");
        // Check Label (en)
        assertEquals(item.getLabels().get("en").getText(),"Researcher");
        // Check description (es)
        assertEquals(item.getDescriptions().get("es").getText(),"Persona que Investiga");
        // Check description (en)
        assertEquals(item.getDescriptions().get("en").getText(),"Research Person");

        Map<String,List<String>> aliases = new HashMap<>();
        String []es_aliases = {"académico","científico","descubridor","erudito","estudioso"};
        String []en_aliases = {"academic","scientist","discoverer","scholar"};

        // Check aliases (es)
        assertArrayEquals(item.getAliases().get("es").stream().map(it->it.getText()).collect(Collectors.toList()).toArray(),es_aliases);
        // Check description (en)
        assertArrayEquals(item.getAliases().get("en").stream().map(it->it.getText()).collect(Collectors.toList()).toArray(),en_aliases);
    }


    @Test
    public void checkPropertyById() {
        PropertyDocument item = (PropertyDocument) wbhi.getDocumentById("P4","http://localhost:8181/entity/");
        // Check Label
        assertEquals(item.getLabels().get("es").getText(),"Instancia de");
        // Check description
        assertEquals(item.getDescriptions().get("es").getText(),"Es instancia de");
        // Check
        assertEquals(item.getAliases().get("es").get(0).getText(),"Subclase de, Pertenece a");
    }


    @Test
    public void checkSearchItem() {
        List<EntityDocument> items =  wbhi.searchInItems("Daniel Ruiz Santamaría","es","http://localhost:8181/entity/");
        ItemDocument item = ((ItemDocument)items.get(0));
        // Check Size
        assertEquals(items.size(),1);
        // Check Label
        assertEquals(item.getLabels().get("es").getText(),"Daniel Ruiz Santamaría");
        // Check description
        assertEquals(item.getDescriptions().get("es").getText(),"Arquitecto de Software Izertis");
        // Check
        assertEquals(item.getAliases().get("es").get(0).getText(),"Arquitecto de Software Izertis, Data Science Izertis");
    }

    @Test
    public void checkUpsertItem() {

        Map<String,String> labels = new HashMap<>();
        labels.put("es","Investigador 1");
        labels.put("en","Researcher 1");

        Map<String,String> descriptions = new HashMap<>();
        descriptions.put("es","Prueba con Investigador 1");
        descriptions.put("en","Test with Researcher 1");

        Map<String,List<String>> aliases = new HashMap<>();
        String []es_aliases = {"Investigacion 1","Persona que investiga 1"};
        String []en_aliases = {"Investigation 1","Research Person 1"};
        aliases.put("es", Arrays.asList(es_aliases));
        aliases.put("en", Arrays.asList(en_aliases));



        PropertyDocument pd = (PropertyDocument) wbhi.getDocumentById("P7","http://localhost:8181/entity/");

        // Investigador
        ItemIdValue itResearch = Datamodel.makeItemIdValue("Q17","http://localhost:8181/entity/");

        // Investigador 1
        ItemDocument id = (ItemDocument) wbhi.getDocumentById("Q16","http://localhost:8181/entity/");

        //Statement statement = wbhi.generateStatement(id.getEntityId(),pd.getEntityId(), Datamodel.makeItemIdValue("Q17","http://localhost:8181/entity/"));

        //PropertyDocument pd2 = (PropertyDocument) wbhi.getDocumentById("P42","http://localhost:8181/entity/");
        Statement statement1 = wbhi.generateStatement(
                id,
                (PropertyDocument) wbhi.getDocumentById("P42","http://localhost:8181/entity/"),
                Datamodel.makeStringValue("03-12-1971"));


        ItemDocument item1 = wbhi.updateStatementInItem(id,statement1,"http://localhost:8181/entity/");

        PropertyDocument pdd = (PropertyDocument) wbhi.getDocumentById("P43","http://localhost:8181/entity/");

        Statement statement2 = wbhi.generateStatement(
                item1,
                (PropertyDocument) wbhi.getDocumentById("P43","http://localhost:8181/entity/"),
                Datamodel.makeStringValue("Calle 1, Ciudad 1, Provincia 1, Country 1 ,Cp1"));

        ItemDocument item2 = wbhi.updateStatementInItem(id,statement2,"http://localhost:8181/entity/");

        //Check Label (es)
        assertEquals(item2.getLabels().get("es").getText(),"Investigador 1");
        //Check Label (es)
        assertEquals(item2.getLabels().get("en").getText(),"Researcher 1");
        // Check description (es)
        assertEquals(item2.getDescriptions().get("es").getText(),"Prueba con Investigador 1");
        // Check description (en)
        assertEquals(item2.getDescriptions().get("en").getText(),"Test with Researcher 1");
        // Check aliases (es)
        assertArrayEquals(item2.getAliases().get("es").stream().map(it->it.getText()).collect(Collectors.toList()).toArray(),es_aliases);
        // Check aliases (en)
        assertArrayEquals(item2.getAliases().get("en").stream().map(it->it.getText()).collect(Collectors.toList()).toArray(),en_aliases);
    }

    @Test
    public void checkUpsertProperty() {

        Map<String,String> labels = new HashMap<>();
        labels.put("es","Instancia de");
        // labels.put("en","Instance of");

        System.out.println(DatatypeIdValue.DT_ITEM);
        // System.exit(0);
        Map<String,List<String>> aliases = null;

        Map<String,String> descriptions = new HashMap<>();
        descriptions.put("es","Es una instáncia de");
        descriptions.put("en","Is a instance of");
        /*
        Map<String,List<String>> aliases = new HashMap<>();
        String []es_aliases = {"Subclase de","Pertenece a"};
        String []en_aliases = {"Subclass of","Is element of"};
        */

        PropertyDocument item = wbhi.doUpsertProperty(
                null,
                "Instancia de",
                "es",
                labels,
                descriptions,
                aliases,
                DatatypeIdValue.DT_STRING,
                "http://localhost:8181/property/"
        );
        String []es_aliases_no = {"Investigacion 3","Persona que investiga 2"};
        //Check Label (es)
        //assertEquals(item.getLabels().get("es").getText(),"Investigador 2");
        //Check Label (es)
        //assertEquals(item.getLabels().get("en").getText(),"Researcher 2");
        // Check description (es)
        //assertEquals(item.getDescriptions().get("es").getText(),"Prueba con Investigador 2");
        // Check description (en)
        //assertEquals(item.getDescriptions().get("en").getText(),"Test with Researcher 2");
        // Check aliases (es)
        //assertEquals(item.getAliases().get("es").stream().map(it->it.getText()).collect(Collectors.toList()).toArray(),es_aliases);
        // Check aliases (en)
        //assertEquals(item.getAliases().get("en").stream().map(it->it.getText()).collect(Collectors.toList()).toArray(),en_aliases);
    }


    /*
    @Test
    public void checkItemLabelTest() {
        String id = "Q4";
        String [] expectedValues = {"Julio Verne","Escritor ciencia ficción frances"};
        WikibaseDataFetcher wbdf = new WikibaseDataFetcher(con,"http://localhost:8181/entity/");
        System.out.println("*** Fetching data for one entity:");
        try {
            EntityDocument ed = wbdf.getEntityDocument(id);
            System.out.println("The current revision of the data for entity Q2 is " + ed.getRevisionId());
            if (ed instanceof ItemDocument) {
                System.out.println("The English name for entity "+id+" is "
                        + ((ItemDocument) ed).getLabels().get("es").getText());
                String []results = new String[2];
                results[0] = ((ItemDocument) ed).getLabels().get("es").getText();
                results[1] = ((ItemDocument) ed).getDescriptions().get("es").getText();
                assertArrayEquals(expectedValues,results);
                return;
            }

        } catch (MediaWikiApiErrorException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        assertTrue("No results with id: "+ id,false);
    }


    @Test
    public void checkEditOnlineTest() throws IOException, MediaWikiApiErrorException {
        final String siteIri = "http://www.test.wikidata.org/entity/";
        WikibaseDataEditor wbde = new WikibaseDataEditor(con,siteIri);
        WebResourceFetcherImpl
                .setUserAgent("Wikidata Toolkit EditOnlineDataExample");

        ItemDocument id1 = ItemDocumentBuilder.forItemId(ItemIdValue.NULL).build();
        id1 = id1.withLabel(new MonolingualTextValueImpl("Izertis","es"));
        id1 = id1.withDescription(new MonolingualTextValueImpl("Consultora de Software","es"));

        ItemDocument newItemDocument = wbde.createItemDocument(id1,
                "Wikidata Toolkit example test item creation", Collections.emptyList());
        ItemIdValue newItemId = newItemDocument.getEntityId();

        System.out.println("*** Successfully created a new item "
                + newItemId.getId()
                + " (see https://test.wikidata.org/w/index.php?title="
                + newItemId.getId() + "&oldid="
                + newItemDocument.getRevisionId() + " for this version)");






        System.out.println("*** Creating a new entity ...");
        ItemIdValue noid = ItemIdValue.NULL; // used when creating new items
    }
     */

    public static void printDocumentation() {
        System.out.println("********************************************************************");
        System.out.println("*** Wikidata Toolkit: FetchOnlineDataExample");
        System.out.println("*** ");
        System.out.println("*** This program fetches individual data using the wikidata.org API.");
        System.out.println("*** It does not download any dump files.");
        System.out.println("********************************************************************");
    }

}
