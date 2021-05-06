package org.test_project.constants;

import org.apache.beam.sdk.schemas.Schema;

public abstract class SchemaRow {

    public final static Schema SCHEMA_ACCESSORY_PARAMETERS;

    static {
        Schema.Builder schemaBuilder = new Schema.Builder();
        schemaBuilder.addNullableField(Field.SCOPE, Schema.FieldType.STRING);
        schemaBuilder.addNullableField(Field.TECHNOLOGY, Schema.FieldType.STRING);
        schemaBuilder.addNullableField(Field.TARGET, Schema.FieldType.STRING);
        schemaBuilder.addNullableField(Field.SUB_TARGET, Schema.FieldType.STRING);
        schemaBuilder.addNullableField(Field.AREA_OF_INTEREST, Schema.FieldType.STRING);
        schemaBuilder.addNullableField(Field.TEST_MODALITY, Schema.FieldType.STRING);
        schemaBuilder.addNullableField(Field.A_SIDE_DEVICE, Schema.FieldType.STRING); // <-
        schemaBuilder.addNullableField(Field.FIRMWARE_V, Schema.FieldType.STRING);
        schemaBuilder.addNullableField(Field.HANDSET_CAPABILITY, Schema.FieldType.STRING);
        schemaBuilder.addNullableField(Field.ACQUISITION_TIMING, Schema.FieldType.DATETIME);
        schemaBuilder.addNullableField(Field.NED_SUBMISSION_MONTH, Schema.FieldType.DATETIME);
        schemaBuilder.addNullableField(Field.COUNTRY, Schema.FieldType.STRING);
        schemaBuilder.addNullableField(Field.HOME_OPERATOR, Schema.FieldType.STRING);
        schemaBuilder.addNullableField(Field.MCC, Schema.FieldType.STRING);
        schemaBuilder.addNullableField(Field.MNC, Schema.FieldType.STRING);
        schemaBuilder.addNullableField(Field.RAN_VENDOR, Schema.FieldType.STRING);
        schemaBuilder.addNullableField(Field.SCENARIO, Schema.FieldType.STRING);
        schemaBuilder.addNullableField(Field.DB_NAME, Schema.FieldType.STRING); // <-
        schemaBuilder.addNullableField(Field.SERVICE_CATEGORY, Schema.FieldType.STRING);
        schemaBuilder.addNullableField(Field.TEST, Schema.FieldType.STRING);
        schemaBuilder.addNullableField(Field.SUB_TEST, Schema.FieldType.STRING);
        SCHEMA_ACCESSORY_PARAMETERS = schemaBuilder.build();
    }

    public final static Schema SCHEMA_VALUE_LAT_LON;

    static {
        Schema.Builder schemaBuilder = new Schema.Builder();
        schemaBuilder.addNullableField(Field.Custom.VALUE, Schema.FieldType.DOUBLE);
        schemaBuilder.addNullableField(Field.LATITUDE, Schema.FieldType.DOUBLE);
        schemaBuilder.addNullableField(Field.LONGITUDE, Schema.FieldType.DOUBLE);
        SCHEMA_VALUE_LAT_LON = schemaBuilder.build();
    }

    public final static Schema SCHEMA_KPI_NAME_AND_VALUES;

    static {
        Schema.Builder schemaBuilder = new Schema.Builder();
        schemaBuilder.addNullableField(Field.Custom.KPI_NAME, Schema.FieldType.STRING);
        schemaBuilder.addNullableField(Field.Custom.VALUE, Schema.FieldType.DOUBLE);
        schemaBuilder.addNullableField(Field.LATITUDE, Schema.FieldType.DOUBLE);
        schemaBuilder.addNullableField(Field.LONGITUDE, Schema.FieldType.DOUBLE);
        SCHEMA_KPI_NAME_AND_VALUES = schemaBuilder.build();
    }

    public final static Schema SCHEMA_KPI_RESULT;

    static {
        Schema.Builder schemaBuilder = new Schema.Builder();
        schemaBuilder.addNullableField(Field.SCOPE, Schema.FieldType.STRING);
        schemaBuilder.addNullableField(Field.TECHNOLOGY, Schema.FieldType.STRING);
        schemaBuilder.addNullableField(Field.TARGET, Schema.FieldType.STRING);
        schemaBuilder.addNullableField(Field.SUB_TARGET, Schema.FieldType.STRING);
        schemaBuilder.addNullableField(Field.AREA_OF_INTEREST, Schema.FieldType.STRING);
        schemaBuilder.addNullableField(Field.TEST_MODALITY, Schema.FieldType.STRING);
        schemaBuilder.addNullableField(Field.A_SIDE_DEVICE, Schema.FieldType.STRING); // <-
        schemaBuilder.addNullableField(Field.FIRMWARE_V, Schema.FieldType.STRING);
        schemaBuilder.addNullableField(Field.HANDSET_CAPABILITY, Schema.FieldType.STRING);
        schemaBuilder.addNullableField(Field.ACQUISITION_TIMING, Schema.FieldType.DATETIME);
        schemaBuilder.addNullableField(Field.NED_SUBMISSION_MONTH, Schema.FieldType.DATETIME);
        schemaBuilder.addNullableField(Field.COUNTRY, Schema.FieldType.STRING);
        schemaBuilder.addNullableField(Field.HOME_OPERATOR, Schema.FieldType.STRING);
        schemaBuilder.addNullableField(Field.MCC, Schema.FieldType.STRING);
        schemaBuilder.addNullableField(Field.MNC, Schema.FieldType.STRING);
        schemaBuilder.addNullableField(Field.RAN_VENDOR, Schema.FieldType.STRING);
        schemaBuilder.addNullableField(Field.SCENARIO, Schema.FieldType.STRING);
        schemaBuilder.addNullableField(Field.DB_NAME, Schema.FieldType.STRING); // <-
        schemaBuilder.addNullableField(Field.SERVICE_CATEGORY, Schema.FieldType.STRING);
        schemaBuilder.addNullableField(Field.TEST, Schema.FieldType.STRING);
        schemaBuilder.addNullableField(Field.SUB_TEST, Schema.FieldType.STRING);
        schemaBuilder.addNullableField(Field.Custom.KPI_NAME, Schema.FieldType.STRING);
        schemaBuilder.addNullableField(Field.Custom.VALUE, Schema.FieldType.DOUBLE);
        schemaBuilder.addNullableField(Field.LATITUDE, Schema.FieldType.DOUBLE);
        schemaBuilder.addNullableField(Field.LONGITUDE, Schema.FieldType.DOUBLE);
        SCHEMA_KPI_RESULT = schemaBuilder.build();
    }

}
