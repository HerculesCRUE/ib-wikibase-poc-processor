package core.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import java.util.*;

import org.apache.commons.lang3.StringUtils;

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
    	String propertiesPath = PROPS_PATH;
    	
    	String dataPath = System.getenv("DATA_PATH");
    	
    	if(StringUtils.isNotBlank(dataPath)) {
    		propertiesPath = new File(dataPath, PROPS_PATH).getAbsolutePath();
    	}
    	
        System.out.println(PROPS_PATH);
        listeners = new HashSet<PropertiesListener>();
        prop = new Properties();
        try {
            InputStream input = new FileInputStream(propertiesPath);
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
