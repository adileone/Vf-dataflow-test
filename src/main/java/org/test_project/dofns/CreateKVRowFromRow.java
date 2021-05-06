package org.test_project.dofns;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateKVRowFromRow extends DoFn<Row, KV<String, Row>> {

    final static Logger logger = LoggerFactory.getLogger(CreateKVRowFromRow.class);

    private String[] keys;

    public CreateKVRowFromRow(String[] keys) {
        this.keys = keys;
    }

    @ProcessElement
    public void processElement(@Element Row element, OutputReceiver<KV<String, Row>> out) {
        StringBuilder key = new StringBuilder();
        for (String k : keys) {
            Object value = element.getValue(k);
            if(value != null){
                key.append(value).append(";");
            } else {
                key.append("null").append(";");
            }
        }
        key.deleteCharAt(key.length() - 1); // remove last ";"
        out.output(KV.of(key.toString(), element));
    }
}
