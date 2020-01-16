package core.utils;




import com.google.gson.JsonObject;
import com.google.gson.JsonArray;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CSVHandler {

    public static final String DEFAULT_SEPARATOR = ",";

    public static JsonArray parseCSV(String csvPath) {
        return parseCSV(csvPath,DEFAULT_SEPARATOR);
    }

    public static JsonArray parseCSV(String csvPath, String separator) {
        JsonArray jDataArray = new JsonArray();
        try (BufferedReader br = new BufferedReader(new FileReader(csvPath))) {
            String line = "";
            int couter = 0;
            Map<Integer,String> headers = new HashMap<>();
            while ((line = br.readLine()) != null) {
                String[] fields = line.split(separator);
                if (couter == 0) {
                    for (int i = 0 ; i < fields.length ; i++) {
                        headers.put(i,fields[i]);
                    }
                } else {
                    JsonObject jData = new JsonObject();
                    for (int i = 0 ; i < fields.length ; i++) {
                        String header = headers.get(i);
                        String content = fields[i];
                        jData.addProperty(headers.get(i),fields[i]);
                    }
                    jDataArray.add(jData);
                }
                ++couter;
            }


        } catch (IOException e) {
            e.printStackTrace();
        }
        return jDataArray;
    }
}
