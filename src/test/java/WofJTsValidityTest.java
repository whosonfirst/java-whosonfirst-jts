import com.google.gson.Gson;
import org.junit.Test;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.context.jts.JtsSpatialContextFactory;
import org.locationtech.spatial4j.io.ShapeIO;
import org.locationtech.spatial4j.io.ShapeReader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;

public class WofJTsValidityTest {

    @Test
    public void testJtsValidity() throws IOException, ParseException {
        List<String> placetypes = new ArrayList();

        // add more placetypes to read more lits
        placetypes.add("region");
        for(String placetype : placetypes) {
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(this.getClass().getResourceAsStream(format("data/%s.txt", placetype))));
            System.out.println(format("Checking %s", placetype));
            String line;
            int count = 0;
            Gson gson = new Gson();
            JtsSpatialContextFactory factory = new JtsSpatialContextFactory();
            ShapeReader shapeReader = new JtsSpatialContext(factory).getFormats().getReader(ShapeIO.GeoJSON);
            while ((line = bufferedReader.readLine()) != null) {
                BufferedReader buffer = new BufferedReader(new InputStreamReader(new URL(line).openConnection().getInputStream()));
                StringBuilder content = new StringBuilder();
                String fileLine;
                while ((fileLine = buffer.readLine()) != null) {
                    content.append(fileLine + "\n");
                }
                buffer.close();
                String json = content.toString();
                Map geojson = gson.fromJson(json, Map.class);
                Map geometryObj = (Map) geojson.get("geometry");
                Map properties = (Map) geojson.get("properties");
                System.out.println(format("%s, %d, %s", count, ((Double) properties.get("wof:id")).intValue(), properties.get("wof:name")));
                try {
                    Map geomMap = new LinkedHashMap();
                    geomMap.put("type", geometryObj.get("type"));
                    geomMap.put("coordinates", geometryObj.get("coordinates"));
                    String geometry = gson.toJson(geomMap);
                    shapeReader.read(geometry);
                    count++;
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

    }
}
