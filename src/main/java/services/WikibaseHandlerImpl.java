package services;

import core.Config.Config;
import jdk.nashorn.internal.objects.annotations.Property;
import org.wikidata.wdtk.datamodel.helpers.Datamodel;
import org.wikidata.wdtk.datamodel.helpers.ItemDocumentBuilder;
import org.wikidata.wdtk.datamodel.helpers.PropertyDocumentBuilder;
import org.wikidata.wdtk.datamodel.helpers.StatementBuilder;
import org.wikidata.wdtk.datamodel.implementation.MonolingualTextValueImpl;
import org.wikidata.wdtk.datamodel.implementation.PropertyIdValueImpl;
import org.wikidata.wdtk.datamodel.interfaces.*;
import org.wikidata.wdtk.util.WebResourceFetcherImpl;
import org.wikidata.wdtk.wikibaseapi.*;
import org.wikidata.wdtk.wikibaseapi.apierrors.MediaWikiApiErrorException;

import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

public class WikibaseHandlerImpl {

    private static WikibaseHandlerImpl instance;
    private Config config;
    private Logger logger;
    private String apiUrl;
    private ApiConnection connection;

    public static WikibaseHandlerImpl getInstance() {
        if (instance == null) {
            instance = new WikibaseHandlerImpl();
        }
        return instance;
    }

    public WikibaseHandlerImpl() {
        this.config = Config.getInstance();
        this.apiUrl = (String) config.getValue("wikibase.host");
        this.doConnection();
    }


    public boolean doConnection() {
        if (connection == null || !connection.isLoggedIn()) {
            String user = (String) config.getValue("wikibase.username");
            String password = (String) config.getValue("wikibase.password");
            WebResourceFetcherImpl
                    .setUserAgent(this.getClass().getName());
            connection = new ApiConnection(apiUrl);
            try {
                connection.login(user,password);
                return true;
            } catch (LoginFailedException e) {
                e.printStackTrace();
                return false;
            }
        } else { // Si esta conectado
            return true;
        }
    }


    public EntityDocument getDocumentById(String id,String siteUri) {
        WikibaseDataFetcher wbdf = new WikibaseDataFetcher(connection,siteUri);
        try {
            EntityDocument ed = wbdf.getEntityDocument(id);
            if (ed instanceof ItemDocument)
                return (ItemDocument) ed;
            else if (ed instanceof PropertyDocument)
                return (PropertyDocument) ed;
        } catch (MediaWikiApiErrorException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    public List<EntityDocument> searchInItems(String searchedString,String lenguage, String siteUri){
        List<EntityDocument> results = new ArrayList<>();

        DocumentDataFilter documentDataFilter = new DocumentDataFilter();
        documentDataFilter.setLanguageFilter(Collections.singleton("es"));
        WikibaseDataFetcher wbdf = new WikibaseDataFetcher(connection, "http://localhost:8181/entity/");
        try {
            for (WbSearchEntitiesResult result: wbdf.searchEntities(searchedString,lenguage)) {
                EntityDocument item = getDocumentById(result.getEntityId(),siteUri);
                results.add(item);
            }
            return results;
        } catch (MediaWikiApiErrorException e) {
            e.printStackTrace();
        }
        return results;
    }

    public EntityDocument searchFirstInItem(String searchedString,String lenguage, String siteUri){
        EntityDocument res = null;

        WikibaseDataFetcher wbdf = new WikibaseDataFetcher(connection, "http://localhost:8181/entity/");
        try {
            for (WbSearchEntitiesResult result: wbdf.searchEntities(searchedString,lenguage,1l)) {
                EntityDocument item = getDocumentById(result.getEntityId(),siteUri);
                res = item;
                break;
            }
            return res;
        } catch (MediaWikiApiErrorException e) {
            e.printStackTrace();
        }
        return res;
    }

    public ItemDocument doUpsertItem(
            ItemDocument item,
            String searchCriteria,
            String lenguageSearchCriteria,
            Map<String,String> labels,
            Map<String,String> descriptions,
            Map<String,List<String>> aliases,
            String siteUri ) {
        if (item == null) {
            if (searchCriteria != null) {
                item = (ItemDocument) searchFirstInItem(searchCriteria,lenguageSearchCriteria,siteUri);
            }
        }

        if (item == null) {
            item = doCreateItem(labels,descriptions,aliases,siteUri);
        } else {
            item = doUpdateItem(item, labels, descriptions, aliases,null,siteUri);
        }

        return item;
    }

    public PropertyDocument doUpsertProperty(
            PropertyDocument prop,
            String searchCriteria,
            String lenguageSearchCriteria,
            Map<String,String> labels,
            Map<String,String> descriptions,
            Map<String,List<String>> aliases,
            String dataType,
            String siteUri ) {
        if (prop == null) {
            if (searchCriteria != null) {
                prop = (PropertyDocument) searchFirstInItem(searchCriteria,lenguageSearchCriteria,siteUri);
            }
        }

        if (prop == null) {
            prop = doCreateProperty(labels,descriptions,aliases,dataType,siteUri);
        } else {
            prop = doUpdateProperty(prop, labels, descriptions, aliases,siteUri);
        }

        return prop;
    }


    public ItemDocument doCreateItem(Map<String,String> labels,
                                 Map<String,String> descriptions,
                                 Map<String,List<String>> aliases,
                                 String siteUri) {
        WikibaseDataEditor wbde = new WikibaseDataEditor(connection, siteUri);
        ItemDocument item = ItemDocumentBuilder.forItemId(ItemIdValue.NULL).build();
        if (labels!=null && labels.size()>0) {
            for (Map.Entry<String, String> entry : labels.entrySet()) {
                item = item.withLabel(new MonolingualTextValueImpl(entry.getValue(),entry.getKey()));
            }
        }
        if (descriptions!=null && descriptions.size()>0) {
            for (Map.Entry<String, String> entry : descriptions.entrySet()) {
                item = item.withDescription(new MonolingualTextValueImpl(entry.getValue(),entry.getKey()));
            }
        }
        if (aliases!=null && aliases.size()>0) {
            for (Map.Entry<String, List<String>> entry : aliases.entrySet()) {
                List<MonolingualTextValue> aliasesList = new ArrayList<>();
                for (String alias : entry.getValue()) {
                    aliasesList.add(new MonolingualTextValueImpl(alias,entry.getKey()));
                }
                item = item.withAliases(entry.getKey(),aliasesList);
            }
        }
        ItemDocument newItem = null;
        try {
            newItem = wbde.createItemDocument(item,
                    "Create new item", Collections.emptyList());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (MediaWikiApiErrorException e) {
            e.printStackTrace();
        }
        return newItem;
    }

    public PropertyDocument doCreateProperty(Map<String,String> labels,
                                     Map<String,String> descriptions,
                                     Map<String,List<String>> aliases,
                                     String dataType,
                                     String siteUri) {
        WikibaseDataEditor wbde = new WikibaseDataEditor(connection, siteUri);

        /*
        PropertyDocumentBuilder propertyDocumentBuilder  = PropertyDocumentBuilder
                .forPropertyIdAndDatatype(
                        PropertyIdValue.NULL,
                        DatatypeIdValue.DT_PROPERTY
                );
        propertyDocumentBuilder.withLabel("Label text " + 1, "fr");
        propertyDocumentBuilder.withDescription("Description " + 1 , "fr");
        propertyDocumentBuilder.withAlias("My alias " + 1, "fr");
        */

        PropertyDocumentBuilder propertyDocumentBuilder  = PropertyDocumentBuilder
                .forPropertyIdAndDatatype(
                        PropertyIdValue.NULL,
                        dataType
                );

        PropertyDocument prop = propertyDocumentBuilder.build();
        if (labels!=null && labels.size()>0) {
            for (Map.Entry<String, String> entry : labels.entrySet()) {
                prop = prop.withLabel(new MonolingualTextValueImpl(entry.getValue(),entry.getKey()));
            }
        }
        if (descriptions!=null && descriptions.size()>0) {
            for (Map.Entry<String, String> entry : descriptions.entrySet()) {
                prop = prop.withDescription(new MonolingualTextValueImpl(entry.getValue(),entry.getKey()));
            }
        }
        if (aliases!=null && aliases.size()>0) {
            for (Map.Entry<String, List<String>> entry : aliases.entrySet()) {
                List<MonolingualTextValue> aliasesList = new ArrayList<>();
                for (String alias : entry.getValue()) {
                    aliasesList.add(new MonolingualTextValueImpl(alias,entry.getKey()));
                }
                prop = prop.withAliases(entry.getKey(),aliasesList);
            }
        }

        PropertyDocument newProp = null;
        try {
            newProp = wbde.createPropertyDocument(prop,
                    "Create new property", Collections.emptyList());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (MediaWikiApiErrorException e) {
            e.printStackTrace();
        }
        return newProp;
    }

    public ItemDocument doUpdateItem(ItemDocument item,
                                 Map<String,String> labels,
                                 Map<String,String> descriptions,
                                 Map<String,List<String>> aliases,
                                 Statement statement,
                                 String siteUri) {
        WikibaseDataEditor wbde = new WikibaseDataEditor(connection, siteUri);
        if (labels!=null && labels.size()>0) {
            for (Map.Entry<String, String> entry : labels.entrySet()) {
                item = item.withLabel(new MonolingualTextValueImpl(entry.getValue(),entry.getKey()));
            }
        }
        if (descriptions!=null && descriptions.size()>0) {
            for (Map.Entry<String, String> entry : descriptions.entrySet()) {
                item = item.withDescription(new MonolingualTextValueImpl(entry.getValue(),entry.getKey()));
            }
        }
        if (aliases!=null && aliases.size()>0) {
            for (Map.Entry<String, List<String>> entry : aliases.entrySet()) {
                List<MonolingualTextValue> aliasesList = new ArrayList<>();
                for (String alias : entry.getValue()) {
                    aliasesList.add(new MonolingualTextValueImpl(alias,entry.getKey()));
                }
                item = item.withAliases(entry.getKey(),aliasesList);
            }
        }
        if (statement!=null) {
            item = item.withStatement(statement);
        }
        ItemDocument newItem = null;
        try {
            newItem = wbde.editItemDocument(item,true,
                    "Update item", Collections.emptyList());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (MediaWikiApiErrorException e) {
            e.printStackTrace();
        }
        return newItem;
    }

    public PropertyDocument doUpdateProperty(PropertyDocument prop,
                                     Map<String,String> labels,
                                     Map<String,String> descriptions,
                                     Map<String,List<String>> aliases,
                                     String siteUri) {
        WikibaseDataEditor wbde = new WikibaseDataEditor(connection, siteUri);
        if (labels!=null && labels.size()>0) {
            for (Map.Entry<String, String> entry : labels.entrySet()) {
                prop = prop.withLabel(new MonolingualTextValueImpl(entry.getValue(),entry.getKey()));
            }
        }
        if (descriptions!=null && descriptions.size()>0) {
            for (Map.Entry<String, String> entry : descriptions.entrySet()) {
                prop = prop.withDescription(new MonolingualTextValueImpl(entry.getValue(),entry.getKey()));
            }
        }
        if (aliases!=null && aliases.size()>0) {
            for (Map.Entry<String, List<String>> entry : aliases.entrySet()) {
                List<MonolingualTextValue> aliasesList = new ArrayList<>();
                for (String alias : entry.getValue()) {
                    aliasesList.add(new MonolingualTextValueImpl(alias,entry.getKey()));
                }
                prop = prop.withAliases(entry.getKey(),aliasesList);
            }
        }
        PropertyDocument newItem = null;
        try {
            newItem = wbde.editPropertyDocument(prop,false,
                    "Update property", Collections.emptyList());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (MediaWikiApiErrorException e) {
            e.printStackTrace();
        }
        return newItem;
    }



    public ItemDocument doUpdateItem2(ItemDocument item,
                                     Map<String,String> labels,
                                     Map<String,String> descriptions,
                                     Map<String,List<String>> aliases,
                                     Statement statement,
                                     String siteUri) {
        WikibaseDataEditor wbde = new WikibaseDataEditor(connection, siteUri);
        if (labels!=null && labels.size()>0) {
            for (Map.Entry<String, String> entry : labels.entrySet()) {
                item = item.withLabel(new MonolingualTextValueImpl(entry.getValue(),entry.getKey()));
            }
        }
        if (descriptions!=null && descriptions.size()>0) {
            for (Map.Entry<String, String> entry : descriptions.entrySet()) {
                item = item.withDescription(new MonolingualTextValueImpl(entry.getValue(),entry.getKey()));
            }
        }
        if (aliases!=null && aliases.size()>0) {
            for (Map.Entry<String, List<String>> entry : aliases.entrySet()) {
                List<MonolingualTextValue> aliasesList = new ArrayList<>();
                for (String alias : entry.getValue()) {
                    aliasesList.add(new MonolingualTextValueImpl(alias,entry.getKey()));
                }
                item = item.withAliases(entry.getKey(),aliasesList);
            }
        }
        if (statement!=null) {
            item = item.withStatement(statement);
        }
        ItemDocument newItem = null;
        try {
            newItem = wbde.editItemDocument(item,true,
                    "Update item", Collections.emptyList());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (MediaWikiApiErrorException e) {
            e.printStackTrace();
        }
        return newItem;
    }

    public Statement generateStatement(ItemDocument item,PropertyDocument prop,Value value) {
        return generateStatement(item.getEntityId(),prop.getEntityId(), value);
    }


    public Statement generateStatement(ItemIdValue itemId,PropertyIdValue propId, Value value) {

        ItemDocument item = (ItemDocument) getDocumentById(itemId.getId(),itemId.getSiteIri());

        Class<? extends Value> ok = value.getClass();

        boolean isItem = value instanceof ItemIdValue;

        Statement statement = null;
        boolean isPresent = false;
        boolean propertyExist = false;
        for (StatementGroup sg : item.getStatementGroups()) {
            if (sg.getProperty().equals(propId)) {
                propertyExist = true;
                for(Statement s : sg.getStatements()) {
                    statement = s;
                    Iterator<Snak> iterator = s.getAllQualifiers();
                    while (iterator.hasNext() && !isPresent) {
                        Snak qua = iterator.next();
                        Value innerValue = qua.getValue();
                        isPresent = value.equals(innerValue);
                        System.out.println();
                    }
                }
            }
        }
        if (!isPresent && !propertyExist) {
            return StatementBuilder
                    .forSubjectAndProperty(itemId, propId)
                    .withQualifierValue(propId, value)
                    .build();
        } else if (propertyExist) {
            return StatementBuilder
                    .forSubjectAndProperty(itemId, propId)
                    .withId(statement.getStatementId())
                    .withQualifierValue(propId, value)
                    .build();
        } else {
            return null;
        }
    }

    public ItemDocument updateStatementInItem(ItemDocument item,Statement statement,String siteUri) {
        return doUpdateItem(item,null,null,null,statement,siteUri);
    }


}
