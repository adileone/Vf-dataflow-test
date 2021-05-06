package org.test_project.dofns;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateStringFromRow extends DoFn<KV<String, Row>, String> {

    final static Logger logger = LoggerFactory.getLogger(CreateStringFromRow.class);

    @ProcessElement
    public void processElement(@Element KV<String, Row> element, OutputReceiver<String> out) {

        StringBuilder stringBuilder = new StringBuilder();

        element.getValue().getValues().forEach(value -> {
            stringBuilder.append(value);
            stringBuilder.append(";");
        });

        stringBuilder.deleteCharAt(stringBuilder.length() - 1); // remove last ";"

        out.output(stringBuilder.toString());
    }
}