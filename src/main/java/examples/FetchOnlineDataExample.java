package examples;

import examples.helpers.ExampleHelpers;
import org.wikidata.wdtk.datamodel.helpers.Datamodel;
import org.wikidata.wdtk.datamodel.interfaces.*;
import org.wikidata.wdtk.util.WebResourceFetcherImpl;
import org.wikidata.wdtk.wikibaseapi.ApiConnection;
import org.wikidata.wdtk.wikibaseapi.BasicApiConnection;
import org.wikidata.wdtk.wikibaseapi.LoginFailedException;
import org.wikidata.wdtk.wikibaseapi.WikibaseDataFetcher;
import org.wikidata.wdtk.wikibaseapi.apierrors.MediaWikiApiErrorException;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;


public class FetchOnlineDataExample {
    public static void main(String[] args) throws MediaWikiApiErrorException, IOException, LoginFailedException {
        ExampleHelpers.configureLogging();
        printDocumentation();

        WebResourceFetcherImpl
                .setUserAgent("Wikidata Toolkit EditOnlineDataExample");
        // ApiConnection connection = ApiConnection.getTestWikidataApiConnection();
        System.out.println("TEST:" + BasicApiConnection.getTestWikidataApiConnection());
        ApiConnection con = new ApiConnection("http://localhost:8181/api.php");
        con.login("WikibaseAdmin","WikibaseDockerAdminPass");

        WikibaseDataFetcher wbdf = new WikibaseDataFetcher(con,"http://localhost:8181/entity/");
        System.out.println("*** Fetching data for one entity:");
        EntityDocument q42 = wbdf.getEntityDocument("Q2");
        System.out.println("The current revision of the data for entity Q2 is "
                + q42.getRevisionId());
        if (q42 instanceof ItemDocument) {
            System.out.println("The English name for entity Q2 is "
                    + ((ItemDocument) q42).getLabels().get("es").getText());
        }

       // Filter data ¿Complex?
        // wbdf = WikibaseDataFetcher.getWikidataDataFetcher();
        System.out.println("*** Fetching data using filters to reduce data volume:");
        // Only site links from English Wikipedia:
        //wbdf.getFilter().setSiteLinkFilter(Collections.singleton("enwiki"));
        // Only labels in French:
        wbdf.getFilter().setLanguageFilter(Collections.singleton("es"));
        // No statements at all:
        wbdf.getFilter().setPropertyFilter(Collections.<PropertyIdValue>emptySet());
        EntityDocument q8 = wbdf.getEntityDocument("Q3");
        if (q8 instanceof ItemDocument) {
            for ( Map.Entry<String, MonolingualTextValue> item : ((ItemDocument) q8).getLabels().entrySet() ) {
                System.out.println("key: "+item.getKey() );
                System.out.println("value: "+item.getValue().getText() );
            }
            System.out.println("Sites links: "+((ItemDocument) q8).getSiteLinks().size());
            for (Map.Entry<String, SiteLink> item : ((ItemDocument) q8).getSiteLinks().entrySet()) {
                System.out.println("key: "+item.getKey() );
                System.out.println("Site Link: "+item.getValue() );
            }

            System.out.println("The Spanish label for entity Q3 is "
                    + ((ItemDocument) q8).getLabels().get("es").getText()
                    + "\nand its English Wikipedia page has the title "
                    //+ ((ItemDocument) q8).getSiteLinks().get("enwiki").getPageTitle() + "."
            );
        }



        wbdf = WikibaseDataFetcher.getWikidataDataFetcher();
        System.out.println("*** Fetching data based on page title:");
        EntityDocument edPratchett = wbdf.getEntityDocumentByTitle("enwiki",
                "Terry Pratchett");
        System.out.println("The Qid of Terry Pratchett is "
                + edPratchett.getEntityId().getId());

        System.out.println("*** Fetching data based on several page titles:");
        Map<String, EntityDocument> results;
        results = wbdf.getEntityDocumentsByTitle("enwiki", "Wikidata",
                "Wikipedia");
        // In this case, keys are titles rather than Qids
        for (Map.Entry<String, EntityDocument> entry : results.entrySet()) {
            System.out.println("Successfully retrieved data for page entitled \""
                    + entry.getKey() + "\": "
                    + entry.getValue().getEntityId().getId());
        }

        System.out.println("*** Done.");
    }

    /**
     * Prints some basic documentation about this program.
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
