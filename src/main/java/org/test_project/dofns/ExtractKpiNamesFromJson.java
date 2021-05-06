package org.test_project.dofns;

import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.channels.Channels;

public class ExtractKpiNamesFromJson extends DoFn<FileIO.ReadableFile, KV<String, String>> {

    final static Logger logger = LoggerFactory.getLogger(ExtractKpiNamesFromJson.class);

    @ProcessElement
    public void processElement(@Element FileIO.ReadableFile element, OutputReceiver<KV<String, String>> out) {

        try {
            InputStream inputStream = Channels.newInputStream(element.open());
            Reader reader = new InputStreamReader(inputStream);
            JSONArray jsonArray = (JSONArray) new JSONParser().parse(reader);
            jsonArray.forEach(object -> {
                JSONObject typologyJsonObject = (JSONObject) object;
                String typology = typologyJsonObject.keySet().iterator().next().toString();
                JSONObject kpiIdJsonObject = (JSONObject) typologyJsonObject.get(typology);
                kpiIdJsonObject.keySet().forEach(key ->{
                    String compoundKey = typology + "." + key;
                    String kpiName = kpiIdJsonObject.get(key).toString();
                    out.output(KV.of(compoundKey, kpiName));
                });
            });

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}