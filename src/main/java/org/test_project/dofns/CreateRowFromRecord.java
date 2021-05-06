package org.test_project.dofns;

import org.test_project.constants.Field;
import org.test_project.constants.Technology;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.csv.CSVRecord;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;

public class CreateRowFromRecord extends DoFn<CSVRecord, Row> {

    final static Logger logger = LoggerFactory.getLogger(CreateRowFromRecord.class);

    // private final static String[] TYPES = {"STRING", "INT32", "DOUBLE", "DATETIME"};

    private final static Locale LOCALE = new Locale("en", "EN");
    private final static String DATE_PATTERN = "MMM-yy";
    private final static String DATE_TIME_PATTERN = "YYYY-MM-dd HH:mm:ss.SSS";

    private Schema schema;

    public CreateRowFromRecord(Schema schema) {
        this.schema = schema;
    }

    @ProcessElement
    public void processElement(@Element CSVRecord element, OutputReceiver<Row> out) {
        Row.Builder rowBuilder = Row.withSchema(schema);

        // logger.info(element.toString());

        for (int i = 0, size = element.size(); i < size; i++) {

            String type = schema.getFields().get(i).getType().getTypeName().toString();
            String value = element.get(i);

            // logger.info(value);

            if (value != null) {

                switch (type) {
                    case "INT32":

                        if (!value.equals("null")) {
                            rowBuilder.addValue(Integer.parseInt(value));
                        } else {
                            rowBuilder.addValue(0);
                        }
                        break;

                    case "DOUBLE":

                        if (!value.equals("null")) {
                            rowBuilder.addValue(Double.parseDouble(value));
                        } else {
                            rowBuilder.addValue(0.0);
                        }
                        break;

                    case "DATETIME":

                        if (!value.equals("null")) {
                            Instant instant = null;
                            if (value.length() <= 6) {
                                instant = Instant.parse(value, DateTimeFormat.forPattern(DATE_PATTERN).withLocale(LOCALE));
                            } else {
                                instant = Instant.parse(value, DateTimeFormat.forPattern(DATE_TIME_PATTERN).withLocale(LOCALE));
                            }
                            rowBuilder.addValue(instant);
                        } else {
                            rowBuilder.addValue(null);
                        }

                        break;

                    default:
                        rowBuilder.addValue(value);
                }

            } else {
                rowBuilder.addValue(null);
            }
        }

        Row row = rowBuilder.build();

        if (row.getString(Field.TECHNOLOGY).equals(Technology.VoLTE_CAP) && (row.getString(Field.KPI_ID).equals("10100") || row.getString(Field.KPI_ID).equals("11000"))) {
            Row.FieldValueBuilder fieldValueBuilder = Row.fromRow(row);
            fieldValueBuilder.withFieldValue(Field.KPI_ID, "10100or11000");
            row = fieldValueBuilder.build();
        }

        out.output(row);
    }
}