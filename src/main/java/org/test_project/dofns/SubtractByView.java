package org.test_project.dofns;

import org.test_project.constants.Field;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.NoSuchElementException;

public class SubtractByView extends DoFn<KV<String, Row>, KV<String, Row>> {

    final static Logger logger = LoggerFactory.getLogger(SubtractByView.class);

    private PCollectionView<Map<String, Row>> sub;

    public SubtractByView(PCollectionView<Map<String, Row>> sub) {
        this.sub = sub;
    }

    @ProcessElement
    public void processElement(@Element KV<String, Row> element, OutputReceiver<KV<String, Row>> out, ProcessContext c) {
        try {
            Map<String, Row> subtrahend = c.sideInput(sub);

            Double result = element.getValue().getDouble(Field.Custom.VALUE) - subtrahend.get(element.getKey()).getDouble(Field.Custom.VALUE);

            Row.FieldValueBuilder rowModified = Row.fromRow(subtrahend.get(element.getKey()))
                    .withFieldValue(Field.Custom.VALUE, result);

            out.output(KV.of(element.getKey(), rowModified.build()));
        } catch (NoSuchElementException noSuchElementException) {
            logger.info(String.format("Exception: [%s]", this.getClass().getSimpleName()));
            // out.output(KV.of(element.getKey(), element.getValue()));
        }
    }


}
