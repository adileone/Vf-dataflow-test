package org.test_project.ptransforms;

import org.test_project.constants.Field;
import org.test_project.dofns.CreateKVRowFromRow;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class FilterFromRowSubTest extends PTransform<PCollection<Row>, PCollection<KV<String, Row>>> {

    final static Logger logger = LoggerFactory.getLogger(FilterFromRowSubTest.class);

    private String[] keys;
    private String typology;
    private PCollectionView<Map<String, String>> pCollectionView;
    private Schema schema;

    public FilterFromRowSubTest(String[] keys, String typology, PCollectionView<Map<String, String>> pCollectionView, Schema schema) {
        this.keys = keys;
        this.typology = typology;
        this.pCollectionView = pCollectionView;
        this.schema = schema;
    }

    @Override
    public PCollection<KV<String, Row>> expand(PCollection<Row> pCollection) {

        return pCollection
                .apply(ParDo.of(new DoFn<Row, Row>() {

                    @ProcessElement
                    public void processElement(@Element Row row, OutputReceiver<Row> out, ProcessContext c) {

                        Map<String, String> map = c.sideInput(pCollectionView);

                        String compoundKey = typology + "." + row.getString(Field.KPI_ID);
                        String test = row.getString(Field.TEST);

                        if (map != null &&
                                (test.equals("DL CE") ||
                                        test.equals("UL CE") ||
                                        test.equals("WB") ||
                                        test.equals("Streaming HD Static")) &&
                                map.containsKey(compoundKey)) {

                            Row.FieldValueBuilder modifiedRow = Row.fromRow(row)
                                    .withFieldValue(Field.SUB_TEST, "ALL")
                                    .withFieldValue(Field.KPI_ID, map.get(compoundKey));

                            out.output(modifiedRow.build());
                        }

                    }
                }).withSideInputs(pCollectionView)).setRowSchema(schema)
                .apply(ParDo.of(new CreateKVRowFromRow(keys))).setCoder(KvCoder.of(StringUtf8Coder.of(), RowCoder.of(schema)));
    }
}
