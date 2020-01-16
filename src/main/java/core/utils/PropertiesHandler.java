package core.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import java.util.*;

public class PropertiesHandler {

    private final String PROPS_PATH = ClassLoader.getSystemClassLoader().getResource("config.properties").getPath();//"src/resources/config.properties";

    private static PropertiesHandler instance = null;
    private static Set<PropertiesListener> listeners;
    private Properties prop;

    public static PropertiesHandler getInstance(PropertiesListener listener) {
        if(instance == null) {
            instance = new PropertiesHandler();
        }
        if (listener!=null)
            listeners.add(listener);
        return instance;
    }

    private PropertiesHandler() {
        System.out.println(PROPS_PATH);
        listeners = new HashSet<PropertiesListener>();
        prop = new Properties();
        try {
            InputStream input = new FileInputStream(PROPS_PATH);
            prop = new Properties();
            prop.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public  void getAsyncProperty(String propertyName, String defaultValue) {
        String value = prop.getProperty(propertyName);
        for (PropertiesListener listener: listeners) {
            if (value != null)
                listener.onPropertyIsReady(value);
            else
                listener.onPropertyIsReady(defaultValue);
        }
    }

    public  String getSyncProperty(String propertyName, String defaultValue) {
        String value = prop.getProperty(propertyName);
        return value;
    }

    public  List<String> getSyncAllPropertiesNames() {
        List<String> keys = new ArrayList<String>();
        for (Object key : prop.keySet()) {
            keys.add((String) key);
        }
        return keys;
    }

}
