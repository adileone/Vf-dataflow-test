package org.test_project.ptransforms;

import org.test_project.constants.Field;
import org.test_project.constants.SchemaRow;
import org.test_project.constants.Typology;
import org.test_project.dofns.RemoveValueFromKey;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class AddEmptyKpiName extends PTransform<PCollection<KV<String, Row>>, PCollection<KV<String, Row>>> {

    final static Logger logger = LoggerFactory.getLogger(AddEmptyKpiName.class);

    private final int EMPTY_VOICE_SIZE = 13;
    private final int EMPTY_PACKET_SIZE = 0;

    private String serviceCategory;
    private PCollectionView<Map<String, String>> pCollectionView;

    public AddEmptyKpiName(String serviceCategory, PCollectionView<Map<String, String>> pCollectionView) {
        this.serviceCategory = serviceCategory;
        this.pCollectionView = pCollectionView;
    }

    @Override
    public PCollection<KV<String, Row>> expand(PCollection<KV<String, Row>> pCollection) {

        return pCollection
                .apply(Filter.by((SerializableFunction<KV<String, Row>, Boolean>) input -> input.getValue().getString(Field.SERVICE_CATEGORY).equals(this.serviceCategory)))
                .apply(ParDo.of(new RemoveValueFromKey(7)))
                .apply(Deduplicate.keyedValues())
                .apply(ParDo.of(new DoFn<KV<String, Row>, KV<String, Row>>() {
                    @ProcessElement
                    public void processElement(@Element KV<String, Row> element, OutputReceiver<KV<String, Row>> out, ProcessContext c) {

                        Map<String, String> map = c.sideInput(pCollectionView);

                        if (map != null && element.getValue() != null) {

                            Row.FieldValueBuilder modifiedRow = Row.fromRow(element.getValue());

                            if (serviceCategory.equals("Voice")) {

                                for (int i = 0; i < EMPTY_VOICE_SIZE; i++) {
                                    modifiedRow
                                            .withFieldValue(Field.Custom.KPI_NAME, map.get(Typology.EMPTY_VOICE + "." + i))
                                            .withFieldValue(Field.Custom.VALUE, null)
                                            .withFieldValue(Field.LATITUDE, null)
                                            .withFieldValue(Field.LONGITUDE, null);
                                    out.output(KV.of(element.getKey(), modifiedRow.build()));
                                }

                            } else {

                                for (int i = 0; i < EMPTY_PACKET_SIZE; i++) {
                                    modifiedRow
                                            .withFieldValue(Field.Custom.KPI_NAME, map.get(Typology.EMPTY_PACKET + "." + i))
                                            .withFieldValue(Field.Custom.VALUE, null)
                                            .withFieldValue(Field.LATITUDE, null)
                                            .withFieldValue(Field.LONGITUDE, null);
                                    out.output(KV.of(element.getKey(), modifiedRow.build()));
                                }
                            }
                        }
                    }
                }).withSideInputs(pCollectionView)).setCoder(KvCoder.of(StringUtf8Coder.of(), RowCoder.of(SchemaRow.SCHEMA_KPI_RESULT)));
    }
}