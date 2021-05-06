package org.test_project.ptransforms;

import org.test_project.constants.Field;
import org.test_project.constants.SchemaRow;
import org.test_project.constants.Technology;
import org.test_project.dofns.CreateKVRowFromRow;
import org.test_project.dofns.RemoveRowWithoutJoin;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class CreateKVRowAccessoryParameters4G5GCap extends PTransform<PCollection<Row>, PCollection<KV<String, Row>>> {

    final static Logger logger = LoggerFactory.getLogger(CreateKVRowAccessoryParameters4G5GCap.class);

    private final TupleTag<Row> _4G_TAG = new TupleTag<>();
    private final TupleTag<Row> _5G_CAP_TAG = new TupleTag<>();

    private String[] keys;
    private String[] accessory_parameters;
    private String[] typologies;
    private PCollectionView<Map<String, String>> pCollectionView;
    private Schema schema;

    public CreateKVRowAccessoryParameters4G5GCap(String[] keys, String[] accessory_parameters, String[] typologies, PCollectionView<Map<String, String>> pCollectionView, Schema schema) {
        this.keys = keys;
        this.accessory_parameters = accessory_parameters;
        this.typologies = typologies;
        this.pCollectionView = pCollectionView;
        this.schema = schema;
    }

    @Override
    public PCollection<KV<String, Row>> expand(PCollection<Row> pCollection) {

        PCollection<KV<String, Row>> pCollection4G = pCollection
                .apply(ParDo.of(new DoFn<Row, Row>() {

                    @ProcessElement
                    public void processElement(@Element Row row, OutputReceiver<Row> out, ProcessContext c) {

                        Map<String, String> map = c.sideInput(pCollectionView);

                        for (String typo : typologies) {

                            String compoundKey = typo + "." + row.getString(Field.KPI_ID);
                            String technology = row.getString(Field.TECHNOLOGY);

                            if (map != null && technology.equals(Technology._4G) && map.containsKey(compoundKey)) {

                                Row.FieldValueBuilder modifiedRow = Row.fromRow(row)
                                        .withFieldValue(Field.TECHNOLOGY, Technology._4G_5G_CAP)
                                        .withFieldValue(Field.KPI_ID, map.get(compoundKey));

                                out.output(modifiedRow.build());
                            }
                        }
                    }
                }).withSideInputs(pCollectionView)).setRowSchema(schema)
                .apply(ParDo.of(new CreateKVRowFromRow(keys))).setCoder(KvCoder.of(StringUtf8Coder.of(), RowCoder.of(schema)));

        PCollection<KV<String, Row>> pCollection5GCap = pCollection
                .apply(ParDo.of(new DoFn<Row, Row>() {

                    @ProcessElement
                    public void processElement(@Element Row row, OutputReceiver<Row> out, ProcessContext c) {

                        Map<String, String> map = c.sideInput(pCollectionView);

                        for (String typo : typologies) {
                            String compoundKey = typo + "." + row.getString(Field.KPI_ID);
                            String technology = row.getString(Field.TECHNOLOGY);

                            if (map != null && technology.equals(Technology._5G_CAP) && map.containsKey(compoundKey)) {

                                Row.FieldValueBuilder modifiedRow = Row.fromRow(row)
                                        .withFieldValue(Field.TECHNOLOGY, Technology._4G_5G_CAP)
                                        .withFieldValue(Field.KPI_ID, map.get(compoundKey));

                                out.output(modifiedRow.build());

                            }
                        }
                    }
                }).withSideInputs(pCollectionView)).setRowSchema(schema)
                .apply(ParDo.of(new CreateKVRowFromRow(keys))).setCoder(KvCoder.of(StringUtf8Coder.of(), RowCoder.of(schema)));

        PCollection<KV<String, Row>> pCollection4G5GCap = KeyedPCollectionTuple
                .of(_4G_TAG, pCollection4G)
                .and(_5G_CAP_TAG, pCollection5GCap)
                .apply(CoGroupByKey.create())
                .apply(ParDo.of(new RemoveRowWithoutJoin(_4G_TAG, _5G_CAP_TAG)))
                .apply(ParDo.of(new DoFn<KV<String, CoGbkResult>, KV<String, Row>>() {
                    @ProcessElement
                    public void processElement(@Element KV<String, CoGbkResult> element, OutputReceiver<KV<String, Row>> out) {

                        Row row4G = element.getValue().getAll(_4G_TAG).iterator().next();
                        Row row5GCap = element.getValue().getAll(_5G_CAP_TAG).iterator().next();

                        Row.Builder rowPair = Row.withSchema(SchemaRow.SCHEMA_ACCESSORY_PARAMETERS);

                        for (String p : accessory_parameters) {

                            Object valueRow4G = row4G.getValue(p);
                            Object valueRow5GCap = row5GCap.getValue(p);

                            if (p.equals(Field.A_SIDE_DEVICE) || p.equals(Field.FIRMWARE_V) || p.equals(Field.DB_NAME) && valueRow4G != null && !valueRow4G.equals(valueRow5GCap)) {
                                rowPair.addValue(valueRow4G + " , " + valueRow5GCap);
                            } else {
                                rowPair.addValue(valueRow4G);
                            }
                        }

                        out.output(KV.of(element.getKey(), rowPair.build()));
                    }
                })).setCoder(KvCoder.of(StringUtf8Coder.of(), RowCoder.of(SchemaRow.SCHEMA_ACCESSORY_PARAMETERS)));

        return pCollection4G5GCap;
    }
}
