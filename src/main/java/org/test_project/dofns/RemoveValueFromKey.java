package org.test_project.dofns;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoveValueFromKey extends DoFn<KV<String, Row>, KV<String, Row>> {

    final static Logger logger = LoggerFactory.getLogger(RemoveValueFromKey.class);

    private int position;

    public RemoveValueFromKey(int position) {
        this.position = position;
    }

    @ProcessElement
    public void processElement(@Element KV<String, Row> element, OutputReceiver<KV<String, Row>> out) {
        String[] keys = element.getKey().split(";", -1);

        StringBuilder newKey = new StringBuilder();

        for(int i = 0, size = keys.length; i < size; i++){
            if(i != position){
                newKey.append(keys[i]).append(";");
            }
        }
        newKey.deleteCharAt(newKey.length() - 1); // remove last ";"

        out.output(KV.of(newKey.toString(), element.getValue()));
    }
}
