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

public class DivideByView extends DoFn<KV<String, Row>, KV<String, Row>> {

    final static Logger logger = LoggerFactory.getLogger(DivideByView.class);

    private PCollectionView<Map<String, Row>> den;

    public DivideByView(PCollectionView<Map<String, Row>> den) {
        this.den = den;
    }

    @ProcessElement
    public void processElement(@Element KV<String, Row> element, OutputReceiver<KV<String, Row>> receiver, ProcessContext c) {
        try {
            Map<String ,Row> denominator = c.sideInput(den);

            Double result = element.getValue().getDouble(Field.Custom.VALUE) / denominator.get(element.getKey()).getDouble(Field.Custom.VALUE);

            Row.FieldValueBuilder rowModified = Row.fromRow(denominator.get(element.getKey()))
                    .withFieldValue(Field.Custom.VALUE, result);

            receiver.output(KV.of(element.getKey(), rowModified.build()));
        } catch (NoSuchElementException noSuchElementException){
            logger.info(String.format("Exception: [%s]", this.getClass().getSimpleName()));
        }
    }
}
