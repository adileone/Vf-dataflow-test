package org.test_project.ptransforms;

import org.test_project.constants.Field;
import org.test_project.constants.SchemaRow;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class CountRows extends PTransform<PCollection<KV<String, Row>>, PCollection<KV<String, Row>>> {

    final static Logger logger = LoggerFactory.getLogger(CountRows.class);

    private String field;
    private String operator;
    private Object constant;
    private Schema schema;

    public CountRows(Schema schema) {
        this.schema = schema;
    }

    public CountRows(String field, String operator, Object constant, Schema schema) {
        this.field = field;
        this.operator = operator;
        this.constant = constant;
        this.schema = schema;
    }

    @Override
    public PCollection<KV<String, Row>> expand(PCollection<KV<String, Row>> pCollection) {
        return pCollection
                .apply(Combine.perKey(new CountPerKeyFn(field, operator, constant, schema))).setCoder(KvCoder.of(StringUtf8Coder.of(), RowCoder.of(SchemaRow.SCHEMA_VALUE_LAT_LON)));
    }
}

class CountPerKeyFn extends Combine.CombineFn<Row, CountPerKeyFn.Accum, Row> {

    final static Logger logger = LoggerFactory.getLogger(CountPerKeyFn.class);

    private String field;
    private String operator;
    private Object constant;
    private Schema schema;

    public CountPerKeyFn(String field, String operator, Object constant, Schema schema) {
        this.field = field;
        this.operator = operator;
        this.constant = constant;
        this.schema = schema;
    }

    public static class Accum implements Serializable {
        double count = 0;
        double latitude = 0;
        double longitude = 0;
    }

    public Accum createAccumulator() {
        return new Accum();
    }

    public Accum addInput(Accum accum, Row row) {
        double latitude = row.getDouble(Field.LATITUDE);
        double longitude = row.getDouble(Field.LONGITUDE);

        if (field == null) {
            exeCalculate(accum, latitude, longitude);
//            logger.info("Count: " + accum.count);
        } else if (schema != null && schema.getField(field).getType().getTypeName().isNumericType()) {
            try {
                if (row.getDouble(field) != null) { // ?
                    double rowValue = row.getDouble(field);
                    double value = ((Number) constant).doubleValue();
                    switch (operator) {
                        case ">":
                            if (rowValue > value) {
//                        logger.info(String.format("Field: %s - Operator: %s - Value: %s - Count: %s", field, operator, value, accum.count));
                                exeCalculate(accum, latitude, longitude);
                            }
                            break;
                        case "<":
                            if (rowValue < value) {
//                        logger.info(String.format("Field: %s - Operator: %s - Value: %s - Count: %s", field, operator, value, accum.count));
                                exeCalculate(accum, latitude, longitude);
                            }
                            break;
                        case ">=":
                            if (rowValue >= value) {
//                        logger.info(String.format("Field: %s - Operator: %s - Value: %s - Count: %s", field, operator, value, accum.count));
                                exeCalculate(accum, latitude, longitude);
                            }
                            break;
                        case "<=":
                            if (rowValue <= value) {
//                        logger.info(String.format("Field: %s - Operator: %s - Value: %s - Count: %s", field, operator, value, accum.count));
                                exeCalculate(accum, latitude, longitude);
                            }
                            break;
                        case "!=":
                            if (rowValue != value) {
//                        logger.info(String.format("Field: %s - Operator: %s - Value: %s - Count: %s", field, operator, value, accum.count));
                                exeCalculate(accum, latitude, longitude);
                            }
                            break;
                        case "==":
                            if (rowValue == value) {
//                        logger.info(String.format("Field: %s - Operator: %s - Value: %s - Count: %s", field, operator, value, accum.count));
                                exeCalculate(accum, latitude, longitude);
                            }
                            break;
                    }
                }
            } catch (NullPointerException e) {
                e.printStackTrace();
            }
        } else if (schema != null && schema.getField(field).getType().getTypeName().isStringType()) {
            try {
                String rowValue = row.getValue(field);
                String value = constant.toString();
                switch (operator) {
                    case "!=":
                        if (rowValue != null && !rowValue.equals(value)) {
//                            logger.info(String.format("Field: %s - Operator: %s - Value: %s - Count: %s", field, operator, value, accum.count));
                            exeCalculate(accum, latitude, longitude);
                        }
                        break;
                    case "==":
                        if (rowValue != null && rowValue.equals(value)) {
//                        logger.info(String.format("Field: %s - Operator: %s - Value: %s - Count: %s", field, operator, value, accum.count));
                            exeCalculate(accum, latitude, longitude);
                        }
                        break;
                }
            } catch (NullPointerException e) {
                e.printStackTrace();
            }
        }
        return accum;
    }

    private void exeCalculate(Accum accum, double latitude, double longitude) {
        try {
            accum.count++;
            accum.latitude += latitude;
            accum.longitude += longitude;
        } catch (NullPointerException e) {
            e.printStackTrace();
        }
    }

    public Accum mergeAccumulators(Iterable<Accum> accums) {
        Accum merged = createAccumulator();
        for (Accum accum : accums) {
            merged.count += accum.count;
            merged.latitude += accum.latitude;
            merged.longitude += accum.longitude;
        }
        return merged;
    }

    public Row extractOutput(Accum accum) {

        Row.Builder rowBuilder = Row.withSchema(SchemaRow.SCHEMA_VALUE_LAT_LON);

        if (accum.count != 0) {
            rowBuilder
                    .addValue(accum.count)
                    .addValue(accum.latitude / accum.count)
                    .addValue(accum.longitude / accum.count);
        } else {
            rowBuilder.addValue(accum.count)
                    .addValue(null)
                    .addValue(null);
        }

        return rowBuilder.build();
    }
}


