package org.test_project.ptransforms;

import org.test_project.constants.Field;
import org.test_project.constants.SchemaRow;
import org.test_project.dofns.ConvertBytePerSecToKbps;
import org.test_project.dofns.ConvertMillisecondToSecond;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class Maximum extends PTransform<PCollection<KV<String, Row>>, PCollection<KV<String, Row>>> {
    final static Logger logger = LoggerFactory.getLogger(Maximum.class);

    private String field;
    private boolean hasConverting;

    public Maximum(String field) {
        this(field, false);
    }

    public Maximum(String field, boolean hasConverting) {
        this.field = field;
        this.hasConverting = hasConverting;
    }

    @Override
    public PCollection<KV<String, Row>> expand(PCollection<KV<String, Row>> pCollection) {

        PCollection<KV<String, Row>> maximumResult = pCollection
                .apply(Combine.perKey(new MaxPerKeyFn(field))).setCoder(KvCoder.of(StringUtf8Coder.of(), RowCoder.of(SchemaRow.SCHEMA_VALUE_LAT_LON)));

        if (hasConverting && field.equals(Field.VALUE_1)) {
            return maximumResult
                    .apply(ParDo.of(new ConvertBytePerSecToKbps()));

        } else if (hasConverting && field.equals(Field.DURATION)) {
            return maximumResult
                    .apply(ParDo.of(new ConvertMillisecondToSecond()));

        } else {
            return maximumResult;
        }

    }
}

class MaxPerKeyFn extends Combine.CombineFn<Row, MaxPerKeyFn.Accum, Row> {

    final static Logger logger = LoggerFactory.getLogger(MaxPerKeyFn.class);

    private String field;

    public MaxPerKeyFn(String field) {
        this.field = field;
    }

    public static class Accum implements Serializable {
        double max = 0;
        double latitude = 0;
        double longitude = 0;
        double count = 0;
    }

    public Accum createAccumulator() {
        return new Accum();
    }

    public Accum addInput(Accum accum, Row row) {
        double value = row.getDouble(field);
        if (value > accum.max) {
            accum.max = value;
        }
        accum.latitude += row.getDouble(Field.LATITUDE);
        accum.longitude += row.getDouble(Field.LONGITUDE);
        accum.count++;
        return accum;
    }

    public Accum mergeAccumulators(Iterable<Accum> accums) {
        Accum merged = createAccumulator();
        for (Accum accum : accums) {
            if (accum.max > merged.max) {
                merged.max = accum.max;
            }
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
                    .addValue(accum.max)
                    .addValue(accum.latitude / accum.count)
                    .addValue(accum.longitude / accum.count);
            return rowBuilder.build();
        }
        return null;
    }
}



