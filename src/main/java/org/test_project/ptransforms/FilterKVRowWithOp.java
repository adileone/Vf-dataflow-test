package org.test_project.ptransforms;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FilterKVRowWithOp extends PTransform<PCollection<KV<String, Row>>, PCollection<KV<String, Row>>> {

    final static Logger logger = LoggerFactory.getLogger(FilterKVRowWithOp.class);
    private String field;
    private String operator;
    private Object value;
    private Schema schema;

    public FilterKVRowWithOp(String field, String operator, Object value, Schema schema) {
        this.field = field;
        this.operator = operator;
        this.value = value;
        this.schema = schema;
    }

    @Override
    public PCollection<KV<String, Row>> expand(PCollection<KV<String, Row>> pCollection) {
        PCollection<KV<String, Row>> out = null;
        if (schema.getField(field).getType().getTypeName().isNumericType()) {
            try {
                switch (operator) {
                    case ">":
                        out = pCollection
                                .apply(Filter.by((SerializableFunction<KV<String, Row>, Boolean>) input -> {
                                    return input.getValue().getDouble(field) > ((Number) value).doubleValue();
                                }));
                        break;

                    case "<":
                        out = pCollection
                                .apply(Filter.by((SerializableFunction<KV<String, Row>, Boolean>) input -> {
                                    return input.getValue().getDouble(field) < ((Number) value).doubleValue();
                                }));
                        break;

                    case ">=":
                        out = pCollection
                                .apply(Filter.by((SerializableFunction<KV<String, Row>, Boolean>) input -> {
                                    return input.getValue().getDouble(field) >= ((Number) value).doubleValue();
                                }));
                        break;

                    case "<=":
                        out = pCollection
                                .apply(Filter.by((SerializableFunction<KV<String, Row>, Boolean>) input -> {
                                    return input.getValue().getDouble(field) <= ((Number) value).doubleValue();
                                }));
                        break;

                    case "!=":
                        out = pCollection
                                .apply(Filter.by((SerializableFunction<KV<String, Row>, Boolean>) input -> {
                                    return input.getValue().getDouble(field) != ((Number) value).doubleValue();
                                }));
                        break;

                    case "==":
                        out = pCollection
                                .apply(Filter.by((SerializableFunction<KV<String, Row>, Boolean>) input -> {
                                    return input.getValue().getDouble(field) == ((Number) value).doubleValue();
                                }));
                        break;
                    default:
                        logger.error("Operator not found, returning null");
                        out = null;
                        break;
                }
            } catch (ClassCastException e) {
                logger.error("Input type and row type are not compatible");
                e.printStackTrace();
            }
        } else if (schema.getField(field).getType().getTypeName().isStringType()) {
            switch (operator) {
                case "!=":
                    out = pCollection
                            .apply(Filter.by((SerializableFunction<KV<String, Row>, Boolean>) input -> {
                                return !input.getValue().getString(field).equals(value);
                            }));
                    break;

                case "==":
                    out = pCollection
                            .apply(Filter.by((SerializableFunction<KV<String, Row>, Boolean>) input -> {
                                return input.getValue().getString(field).equals(value);
                            }));
                    break;

                default:
                    logger.error("String operator not found, returning null");
                    break;
            }
        } else if (schema.getField(field).getType().getTypeName().isDateType()) {
            logger.error("Data time filtering not implemented, returning null");
            return null;
        } else {
            logger.error("Type not recognized, returning null");
            return null;
        }

        return out;
    }
}
