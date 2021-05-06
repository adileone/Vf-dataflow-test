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

public class StDev extends PTransform<PCollection<KV<String, Row>>, PCollection<KV<String, Row>>> {

    final static Logger logger = LoggerFactory.getLogger(StDev.class);

    private String field;
    private boolean hasConverting;
    private Schema schema;

    public StDev(String field, Schema schema) {
        this(field, false, schema);
    }

    public StDev(String field, boolean hasConverting, Schema schema) {
        this.field = field;
        this.hasConverting = hasConverting;
        this.schema = schema;
    }

    @Override
    public PCollection<KV<String, Row>> expand(PCollection<KV<String, Row>> pCollection) {

        PCollection<KV<String, Row>> stDevResult = pCollection
                .apply(Combine.perKey(new StDevPerKeyFn(field))).setCoder(KvCoder.of(StringUtf8Coder.of(), RowCoder.of(SchemaRow.SCHEMA_VALUE_LAT_LON)));

        if (hasConverting && field.equals(Field.VALUE_1)) {
            return stDevResult
                    .apply(ParDo.of(new ConvertBytePerSecToKbps()));

        } else if (hasConverting && field.equals(Field.DURATION)) {
            return stDevResult
                    .apply(ParDo.of(new ConvertMillisecondToSecond()));

        } else {
            return stDevResult;
        }

    }
}

class StDevPerKeyFn extends Combine.CombineFn<Row, StDevPerKeyFn.Accum, Row> {

    final static Logger logger = LoggerFactory.getLogger(StDevPerKeyFn.class);

    private String field;

    public StDevPerKeyFn(String field){
        this.field = field;
    }

    public static class Accum implements Serializable {
        double sum = 0;
        double square = 0;
        double latitude = 0;
        double longitude = 0;
        double count = 0;
    }

    public Accum createAccumulator() {
        return new Accum();
    }

    public Accum addInput(Accum accum, Row row) {
        if(row.getDouble(field) != null){
            double value = row.getDouble(Field.VALUE_1);
            accum.sum += value;
            accum.square = accum.square + (value * value);
            accum.count++;
            accum.latitude += row.getDouble(Field.LATITUDE);
            accum.longitude += row.getDouble(Field.LONGITUDE);
        } else {
//            logger.info(String.format("KPIID: %s", row.getString(Field.KPI_ID)));
        }

        return accum;
    }

    public Accum mergeAccumulators(Iterable<Accum> accums) {
        Accum merged = createAccumulator();
        for (Accum accum : accums) {
            merged.sum += accum.sum;
            merged.square += accum.square;
            merged.count += accum.count;
            merged.latitude += accum.latitude;
            merged.longitude += accum.longitude;
        }

        return merged;
    }

    public Row extractOutput(Accum accum) {

        if (accum.count != 0) {
            double meanOfSumPow = Math.pow(accum.sum, 2) / accum.count;
            double variance = (accum.square - meanOfSumPow) / (accum.count - 1);
            double stDev = Math.sqrt(variance);
            Row.Builder rowBuilder = Row
                    .withSchema(SchemaRow.SCHEMA_VALUE_LAT_LON)
                    .addValue(stDev)
                    .addValue(accum.latitude / accum.count)
                    .addValue(accum.longitude / accum.count);
            return rowBuilder.build();
        }

        return null;
    }
}
