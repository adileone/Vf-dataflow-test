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
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class Avg extends PTransform<PCollection<KV<String, Row>>, PCollection<KV<String, Row>>> {

    final static Logger logger = LoggerFactory.getLogger(Avg.class);

    private String field;
    private boolean hasConverting;
    private Schema schema;

    public Avg(String field, Schema schema) {
        this(field, false, schema);
    }

    public Avg(String field, boolean hasConverting, Schema schema) {
        this.field = field;
        this.hasConverting = hasConverting;
        this.schema = schema;
    }

    @Override
    public PCollection<KV<String, Row>> expand(PCollection<KV<String, Row>> pCollection) {

        PCollection<KV<String, Row>> pCollectionAvg = pCollection
                .apply(Combine.perKey(new AvgPerKeyFn(field))).setCoder(KvCoder.of(StringUtf8Coder.of(), RowCoder.of(SchemaRow.SCHEMA_VALUE_LAT_LON)));

        if (hasConverting && field.equals(Field.VALUE_1)) {
            return pCollectionAvg
                    .apply(ParDo.of(new ConvertBytePerSecToKbps()));

        } else if (hasConverting && field.equals(Field.DURATION)) {
            return pCollectionAvg
                    .apply(ParDo.of(new ConvertMillisecondToSecond()));

        } else {
            return pCollectionAvg;
        }

    }
}

class AvgPerKeyFn extends Combine.CombineFn<Row, AvgPerKeyFn.Accum, Row> {

    final static Logger logger = LoggerFactory.getLogger(AvgPerKeyFn.class);

    private String field;

    public AvgPerKeyFn(String field) {
        this.field = field;
    }

    public static class Accum implements Serializable {
        double value = 0;
        double latitude = 0;
        double longitude = 0;
        double count = 0;
    }

    public Accum createAccumulator() {
        return new Accum();
    }

    public Accum addInput(Accum accum, Row row) {
        if (row.getDouble(field) != null) {
            accum.value += row.getDouble(field);
            accum.latitude += row.getDouble(Field.LATITUDE);
            accum.longitude += row.getDouble(Field.LONGITUDE);
            accum.count++;
        } else {
//            logger.info(String.format("KPIID: %s", row.getString(Field.KPI_ID)));
        }

        return accum;
    }

    public Accum mergeAccumulators(Iterable<Accum> accums) {
        Accum merged = createAccumulator();
        for (Accum accum : accums) {
            merged.value += accum.value;
            merged.latitude += accum.latitude;
            merged.longitude += accum.longitude;
            merged.count += accum.count;
        }
        return merged;
    }

    public Row extractOutput(Accum accum) {
        if (accum.count != 0) {
            Row.Builder rowBuilder = Row
                    .withSchema(SchemaRow.SCHEMA_VALUE_LAT_LON)
                    .addValue(accum.value / accum.count)
                    .addValue(accum.latitude / accum.count)
                    .addValue(accum.longitude / accum.count);
            return rowBuilder.build();
        }
        return null;
    }
}
