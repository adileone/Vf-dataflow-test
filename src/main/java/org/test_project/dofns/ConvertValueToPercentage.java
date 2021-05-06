package org.test_project.dofns;

import org.test_project.constants.Field;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConvertValueToPercentage extends DoFn<KV<String, Row>, KV<String, Row>> {

    final static Logger logger = LoggerFactory.getLogger(ConvertValueToPercentage.class);

    public ConvertValueToPercentage(){

    }

    @ProcessElement
    public void processElement(@Element KV<String, Row> element, OutputReceiver<KV<String, Row>> out) {

        Double valuePercentage = element.getValue().getDouble(Field.Custom.VALUE) * 100;

        Row.FieldValueBuilder rowModified = Row.fromRow(element.getValue())
                .withFieldValue(Field.Custom.VALUE, valuePercentage);

        out.output(KV.of(element.getKey(), rowModified.build()));
    }
}