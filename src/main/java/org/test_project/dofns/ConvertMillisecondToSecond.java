package org.test_project.dofns;

import org.test_project.constants.Field;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConvertMillisecondToSecond extends DoFn<KV<String, Row>, KV<String, Row>> {

    final static Logger logger = LoggerFactory.getLogger(ConvertMillisecondToSecond.class);

    public ConvertMillisecondToSecond(){

    }

    @ProcessElement
    public void processElement(@Element KV<String, Row> element, OutputReceiver<KV<String, Row>> out) {

        Double valueConverted = element.getValue().getDouble(Field.Custom.VALUE) / 1000;

        Row.FieldValueBuilder rowModified = Row.fromRow(element.getValue())
                .withFieldValue(Field.Custom.VALUE, valueConverted);

        out.output(KV.of(element.getKey(), rowModified.build()));
    }
}