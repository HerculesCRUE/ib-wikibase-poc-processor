package core.Config;


import core.utils.PropertiesHandler;


import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Config {

    private static Config instance;
    private Map<String,Object> values;

    public static Config getInstance() {
        if (instance == null)
            instance = new Config();
        return instance;
    }

    public Config() {
        this.values = new HashMap<String, Object>();
        populateConfigFromProperties();
    }

    private void populateConfigFromProperties() {
        PropertiesHandler propertiesHandler = PropertiesHandler.getInstance(null);
        List<String> keys = propertiesHandler.getSyncAllPropertiesNames();
        for (String key : keys) {
            Object value = propertiesHandler.getSyncProperty(key,null);
            values.put(key,value);
        }
    }

    public Object getValue(String key, Object defaultValue) {
        if (!values.containsKey(key))
            return defaultValue;
        else
            return values.get(key);
    }

    public Object getValue(String key) {
        if (!values.containsKey(key))
            return null;
        else
            return values.get(key);
    }
}
