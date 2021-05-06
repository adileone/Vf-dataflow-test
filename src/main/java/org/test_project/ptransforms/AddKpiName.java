package org.test_project.ptransforms;

import org.test_project.constants.Field;
import org.test_project.constants.SchemaRow;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
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

public class AddKpiName extends PTransform<PCollection<KV<String, Row>>, PCollection<KV<String, Row>>> {

    final static Logger logger = LoggerFactory.getLogger(AddKpiName.class);

    private String typology;
    private PCollectionView<Map<String, String>> pCollectionView;

    public AddKpiName(String typology, PCollectionView<Map<String, String>> pCollectionView) {
        this.typology = typology;
        this.pCollectionView = pCollectionView;
    }

    @Override
    public PCollection<KV<String, Row>> expand(PCollection<KV<String, Row>> pCollection) {

        return pCollection.apply(ParDo.of(new DoFn<KV<String, Row>, KV<String, Row>>() {
            @ProcessElement
            public void processElement(@Element KV<String, Row> element, OutputReceiver<KV<String, Row>> out, ProcessContext c) {

                Map<String, String> map = c.sideInput(pCollectionView);

                String[] keys = element.getKey().split(";", -1);
                String subTest = keys[6];
                String kpiId = keys[7];

                String key = typology + "." + kpiId;
                String compoundKey = typology + "." + subTest + ";" + kpiId;

                Row.Builder rowBuilder = Row.withSchema(SchemaRow.SCHEMA_KPI_NAME_AND_VALUES);

                if (map != null && map.containsKey(key)) {
                    rowBuilder
                            .addValue(map.get(key))
                            .addValue(element.getValue().getValue(Field.Custom.VALUE))
                            .addValue(element.getValue().getValue(Field.LATITUDE))
                            .addValue(element.getValue().getValue(Field.LONGITUDE));

                } else if (map != null && map.containsKey(compoundKey)) {
                    rowBuilder
                            .addValue(map.get(compoundKey))
                            .addValue(element.getValue().getValue(Field.Custom.VALUE))
                            .addValue(element.getValue().getValue(Field.LATITUDE))
                            .addValue(element.getValue().getValue(Field.LONGITUDE));
                } else {
//                    logger.info(key);
                    rowBuilder
                            .addValue(null)
                            .addValue(element.getValue().getValue(Field.Custom.VALUE))
                            .addValue(element.getValue().getValue(Field.LATITUDE))
                            .addValue(element.getValue().getValue(Field.LONGITUDE));
                }



                out.output(KV.of(element.getKey(), rowBuilder.build()));

//                map.forEach((k, v) -> {
//                    // logger.info(k);
//                    if(k.contains(typology)){
//
//                        logger.info(k);
//
//                        String[] items = k.split(".", -1);
//
//                        if(items[1].equals(kpiId)){
//                            rowBuilder
//                                    .addValue(v)
//                                    .addValue(element.getValue().getValue(Field.Custom.VALUE))
//                                    .addValue(element.getValue().getValue(Field.LATITUDE))
//                                    .addValue(element.getValue().getValue(Field.LONGITUDE));
//                        } else if(items[1].equals(compoundKey)){
//                            rowBuilder
//                                    .addValue(v)
//                                    .addValue(element.getValue().getValue(Field.Custom.VALUE))
//                                    .addValue(element.getValue().getValue(Field.LATITUDE))
//                                    .addValue(element.getValue().getValue(Field.LONGITUDE));
//                        } else {
//                            rowBuilder
//                                    .addValue(null)
//                                    .addValue(element.getValue().getValue(Field.Custom.VALUE))
//                                    .addValue(element.getValue().getValue(Field.LATITUDE))
//                                    .addValue(element.getValue().getValue(Field.LONGITUDE));
//                        }
//
//                        out.output(KV.of(element.getKey(), rowBuilder.build()));
//
//                    }
//                });
            }
        }).withSideInputs(pCollectionView)).setCoder(KvCoder.of(StringUtf8Coder.of(), RowCoder.of(SchemaRow.SCHEMA_KPI_NAME_AND_VALUES)));
    }
}