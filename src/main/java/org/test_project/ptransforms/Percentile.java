package org.test_project.ptransforms;


import org.test_project.constants.Field;
import org.test_project.constants.SchemaRow;
import org.test_project.dofns.ConvertBytePerSecToKbps;
import org.test_project.dofns.ConvertMillisecondToSecond;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Percentile extends PTransform<PCollection<KV<String,Row>>, PCollection<KV<String, Row>>> {

    final static Logger logger = LoggerFactory.getLogger(Percentile.class);

    private final static Schema SCHEMA_LIST_LAT_LON;
    static {
        Schema.Builder schemaBuilder = new Schema.Builder();
        schemaBuilder.addNullableField(Field.Custom.VALUES, Schema.FieldType.array(Schema.FieldType.DOUBLE));
        schemaBuilder.addNullableField(Field.LATITUDE, Schema.FieldType.DOUBLE);
        schemaBuilder.addNullableField(Field.LONGITUDE, Schema.FieldType.DOUBLE);
        SCHEMA_LIST_LAT_LON = schemaBuilder.build();
    }

    private String field;
    private double nthPercentile;
    private boolean hasConverting;

    public Percentile(String field, double nthPercentile) {
        this(field, nthPercentile, false);
    }

    public Percentile(String field, double nthPercentile, boolean hasConverting) {
        this.field = field;
        this.nthPercentile = nthPercentile / 100;
        this.hasConverting = hasConverting;
    }

    @Override
    public PCollection<KV<String, Row>> expand(PCollection<KV<String,Row>> pCollection) {

        PCollection<KV<String, Row>> percentileResult = pCollection
                .apply(Combine.perKey(new CreateListPerKeyFn(field, SCHEMA_LIST_LAT_LON))).setCoder(KvCoder.of(StringUtf8Coder.of(), RowCoder.of(SCHEMA_LIST_LAT_LON)))
                .apply(ParDo.of(
                        new DoFn<KV<String, Row>, KV<String, Row>>() {
                            @ProcessElement
                            public void processElement(@Element KV<String, Row> element, OutputReceiver<KV<String, Row>> out) {

                                List<Double> list = new ArrayList<>(element.getValue().getArray(0));

                                double num = list.size();
                                double step = 1 / (num - 1);
                                double posApprox = nthPercentile / step;
                                int pos1 = (int) Math.floor(posApprox);
                                int pos2 = (int) Math.ceil(posApprox);

                                double x1 = step * pos1;
                                double x2 = step * pos2;

                                double value1 = list.get(pos1);
                                double value2 = list.get(pos2);

                                double result;

                                if (x1 == x2) {
                                    result = value1;
                                } else {
                                    result = (((value2 - value1) / (x2 - x1)) * (nthPercentile - x1)) + value1;
                                }

                                Row.Builder rowBuilder = Row
                                        .withSchema(SchemaRow.SCHEMA_VALUE_LAT_LON)
                                        .addValue(result)
                                        .addValue(element.getValue().getDouble(Field.LATITUDE))
                                        .addValue(element.getValue().getDouble(Field.LONGITUDE));

                                out.output(KV.of(element.getKey(), rowBuilder.build()));
                            }
                        }
                )).setCoder(KvCoder.of(StringUtf8Coder.of(), RowCoder.of(SchemaRow.SCHEMA_VALUE_LAT_LON)));

        if (hasConverting && field.equals(Field.VALUE_1)) {
            return percentileResult
                    .apply(ParDo.of(new ConvertBytePerSecToKbps()));

        } else if (hasConverting && field.equals(Field.DURATION)) {
            return percentileResult
                    .apply(ParDo.of(new ConvertMillisecondToSecond()));

        } else {
            return percentileResult;
        }

    }
}

class CreateListPerKeyFn extends Combine.CombineFn<Row, CreateListPerKeyFn.Accum, Row> {

    final static Logger logger = LoggerFactory.getLogger(CreateListPerKeyFn.class);

    private String field;
    private Schema schema;

    public CreateListPerKeyFn(String field, Schema schema) {
        this.field = field;
        this.schema = schema;
    }

    public static class Accum implements Serializable {
        List<Double> list = new ArrayList<>();
        double latitude = 0;
        double longitude = 0;
        double count = 0;
    }

    public Accum createAccumulator() {
        return new Accum();
    }

    public Accum addInput(Accum accum, Row row) {
        if(row.getDouble(field) != null){
            accum.list.add(row.getDouble(field));
            accum.latitude += row.getDouble(Field.LATITUDE);
            accum.longitude += row.getDouble(Field.LONGITUDE);
            accum.count++;
        } else {
//                logger.info(String.format("KPIID: %s", row.getString(Field.KPI_ID)));
        }

        return accum;
    }

    public Accum mergeAccumulators(Iterable<Accum> accums) {
        Accum merged = createAccumulator();
        for (Accum accum : accums) {
            merged.list.addAll(accum.list);
            merged.latitude += accum.latitude;
            merged.longitude += accum.longitude;
            merged.count += accum.count;
        }
        return merged;
    }

    public Row extractOutput(Accum accum) {
        Collections.sort(accum.list);
        Row.Builder rowBuilder = Row
                .withSchema(schema)
                .addValue(accum.list)
                .addValue(accum.latitude / accum.count)
                .addValue(accum.longitude / accum.count);
        return rowBuilder.build();
    }
}