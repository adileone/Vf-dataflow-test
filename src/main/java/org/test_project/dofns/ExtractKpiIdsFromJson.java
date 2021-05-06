package org.test_project.dofns;

import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;

public class ExtractKpiIdsFromJson extends DoFn<FileIO.ReadableFile, KV<String, List<String>>> {

    @ProcessElement
    public void processElement(@Element FileIO.ReadableFile element, OutputReceiver<KV<String, List<String>>> out) {
        try {
            InputStream inputStream = Channels.newInputStream(element.open());
            Reader reader = new InputStreamReader(inputStream);
            JSONArray jsonArray = (JSONArray) new JSONParser().parse(reader);
            jsonArray.forEach(object -> {
                JSONObject technologyJsonObject = (JSONObject) object;
                String technologyKey = technologyJsonObject.keySet().iterator().next().toString();
                JSONObject categoryJsonObject = (JSONObject) technologyJsonObject.get(technologyKey);
                for (Object value : categoryJsonObject.keySet()) {
                    String categoryKey = value.toString();
                    JSONArray jsonArrayKpiIds = (JSONArray) categoryJsonObject.get(categoryKey);
                    // logger.info(String.format("Technology: |%s| - Category: |%s| - KPIIds: %s", technologyKey, categoryKey, jsonArrayKpiIds.toString()));
                    String key = technologyKey + "." + categoryKey;
                    out.output(KV.of(key, convertArrayJsonToList(jsonArrayKpiIds)));
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private List<String> convertArrayJsonToList(JSONArray jsonArray) {
        List<String> list = new ArrayList<>();

        jsonArray.forEach(o -> {
            list.add(o.toString());
        });

        return list;
    }
}