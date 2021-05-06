package org.test_project.utilis;

import org.test_project.constants.Field;
import org.test_project.constants.Technology;
import org.test_project.constants.Typology;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;

public abstract class Constants {

    private Constants() {
    }

    public final static String JSON_KPI_IDS_FILE_PATTERN = "gs://dataflow-test-vf/json/kpi_ids.json";
    public final static String JSON_KPI_ID_NAMES_FILE_PATTERN = "gs://dataflow-test-vf/json/kpi_id_names.json";

    public final static String CSV_FILE_PATTERN = "gs://dataflow-test-vf/input/IT-VD4_VA_G21C_796-mar_21-Light-NED-2021_04_15_16_01_55.csv";
   
//     public final static String JSON_WITH_TYPES_FILE_PATTERN = "./json/schema_with_types.json";
//     public final static String JSON_WITHOUT_TYPES_PATTERN = "./json/schema_without_types.json";
    public final static String SCHEMA_SERIALIZED_FILE_PATTERN = "/home/adileone/devel/DataflowTestProject/schema/schema_serialized";

    public final static int _1_Mbps = 125000;
    public final static int _2_Mbps = 250000;
    public final static int _3_Mbps = 375000;
    public final static int _10_Mbps = 1250000;

    public final static String[] TECHNOLOGIES = new String[]{
            // Technology.LEGACY,
            Technology.VoLTE_CAP,
            Technology.VoLTE_REAL,
            Technology._4G,
            Technology._5G_CAP,
            Technology._5G_REAL
    };

    public final static String[] TYPOLOGIES = new String[]{
            Typology.COUNT,
            Typology.COUNT_ERROR_CODE_NOT_0,
            Typology.COUNT_ERROR_CODE_EQUALS_0,
            Typology.COUNT_VALUE_1_MINOR_2_5,
            Typology.COUNT_VALUE_1_NOT_NULL,
            Typology.COUNT_VALUE_3_EQUALS_SRVCC,
            Typology.COUNT_VALUE_1_MAJOR_1_Mbps,
            Typology.COUNT_VALUE_1_MAJOR_2_Mbps,
            Typology.COUNT_VALUE_1_MAJOR_3_Mbps,
            Typology.COUNT_VALUE_1_MAJOR_10_Mbps,
            Typology.COUNT_VALUE_3_EQUALS_HD,
            Typology.COUNT_VALUE_3_EQUALS_HD_ERROR_CODE_EQUALS_0,
            Typology.AVG_VALUE_MOS_SCORE,
            Typology.AVG_VALUE_1,
            Typology.AVG_DURATION_SEC,
            Typology.AVG_DURATION_MSEC,
            Typology.AVG_VIDEO_VALUE_PIXELS,
            Typology.AVG_VIDEO_VALUE_VMOS,
            Typology.AVG_VIDEO_DURATION_SEC,
            Typology.ST_DEV_VALUE_MOS_SCORE,
            Typology.ST_DEV,
            Typology.MAX,
            Typology.PERCENTILE_VALUE_MOS_SCORE_SEC,
            Typology.PERCENTILE_VALUE_MOS_SCORE,
            Typology.PERCENTILE,
//            Typology.PERCENTILE_10th,
//            Typology.PERCENTILE_90th,
            Typology.MEDIAN
    };

    public final static String[] TYPOLOGIES_ALL = new String[]{
            Typology.AVG_DURATION_SEC_ALL,
            Typology.AVG_VALUE_1_ALL,
            Typology.AVG_VIDEO_DURATION_SEC_ALL,
            Typology.AVG_VIDEO_VALUE_PIXELS_ALL,
            Typology.AVG_VIDEO_VALUE_VMOS_ALL,
            Typology.COUNT_ALL,
            Typology.COUNT_ERROR_CODE_EQUALS_0_ALL,
            Typology.COUNT_ERROR_CODE_NOT_0_ALL,
            Typology.COUNT_VALUE_1_MAJOR_1_Mbps_ALL,
            Typology.COUNT_VALUE_1_MAJOR_2_Mbps_ALL,
            Typology.COUNT_VALUE_1_MAJOR_3_Mbps_ALL,
            Typology.COUNT_VALUE_1_MAJOR_10_Mbps_ALL,
            Typology.COUNT_VALUE_3_EQUALS_HD_ALL,
            Typology.COUNT_VALUE_3_EQUALS_HD_ERROR_CODE_EQUALS_0_ALL,
            Typology.PERCENTILE_ALL,
            Typology.ST_DEV_ALL
    };

    public final static String[] FULL_KEY_FIELDS = new String[]{
            Field.SCOPE,
            Field.TECHNOLOGY,
            Field.TARGET,
            Field.SUB_TARGET,
            Field.AREA_OF_INTEREST,
            Field.TEST_MODALITY,
            Field.A_SIDE_DEVICE,
            Field.FIRMWARE_V,
            Field.HANDSET_CAPABILITY,
            Field.ACQUISITION_TIMING,
            Field.NED_SUBMISSION_MONTH,
            Field.COUNTRY,
            Field.HOME_OPERATOR,
            Field.MCC,
            Field.MNC,
            Field.RAN_VENDOR,
            Field.SCENARIO,
            Field.DB_NAME,
            Field.SERVICE_CATEGORY,
            Field.TEST,
            Field.SUB_TEST,
            Field.KPI_ID
    };

    public final static String[] KEY_FIELDS = new String[]{
            Field.TECHNOLOGY,
            Field.SUB_TARGET,
            Field.AREA_OF_INTEREST,
            Field.HOME_OPERATOR,
            Field.RAN_VENDOR,
            Field.TEST,
            Field.SUB_TEST,
            Field.KPI_ID
    };

    public final static String[] ACCESSORY_PARAMETERS_FIELDS = new String[]{
            Field.SCOPE,
            Field.TECHNOLOGY,
            Field.TARGET,
            Field.SUB_TARGET,
            Field.AREA_OF_INTEREST,
            Field.TEST_MODALITY,
            Field.A_SIDE_DEVICE,
            Field.FIRMWARE_V,
            Field.HANDSET_CAPABILITY,
            Field.ACQUISITION_TIMING,
            Field.NED_SUBMISSION_MONTH,
            Field.COUNTRY,
            Field.HOME_OPERATOR,
            Field.MCC,
            Field.MNC,
            Field.RAN_VENDOR,
            Field.SCENARIO,
            Field.DB_NAME,
            Field.SERVICE_CATEGORY,
            Field.TEST,
            Field.SUB_TEST
    };


    public static final TupleTag<Row> ACCESSORY_PARAMETERS_TAG = new TupleTag<>();
    public static final TupleTag<Row> VALUES_TAG = new TupleTag<>();

    public final static String OUTPUT_HEADER_OLD = "SCOPE;" + // 0
            "TECHNOLOGY;" + // 1
            "CARRIER AGGREGATION YES/NOT;" + // 2
            "TARGET ON SCOPE;" + // 3
            "CITIES ROUTE LINES PLACE (sub-target);" + // 4
            "AREAS OF INTEREST;" + // 5
            "TEST MODALITY (N NOMADIC/D DINAMIC);" + // 6
            "SMARTPHONE MODEL;" + // 7
            "FIRMWARE VERSION;" + // 8
            "HANDSET CAPABILITY [THROUGHPUT DL/UL];" + // 9
            "LAST ACQUISITION TIMING [YYYY_MM];" + // 10
            "NED MONTH [YYYY_MM];" + // 11
            "Country;" + // 12
            "OPERATOR NAME;" + // 13
            "MCC;" + // 14
            "MNC;" + // 15
            "OPCOS;" + // 16
            "RAN VENDOR;" + // 17
            "SCENARIO;" + // 18
            "DatabaseName;" + // 19
            "Service;" + // 20
            "Test;" + // 21
            "Sub-Test;" + // 22
            "KPI;" + // 23
            "KPI value"; // 24

    public final static String OUTPUT_HEADER = "SCOPE;" + // 0
            "TECHNOLOGY;" + // 1
            "CARRIER AGGREGATION YES/NOT;" + // 2
            "TARGET ON SCOPE;" + // 3
            "CITIES ROUTE LINES PLACE (sub-target);" + // 4
            "AREAS OF INTEREST;" + // 5
            "TEST MODALITY (N NOMADIC/D DINAMIC);" + // 6
            "SMARTPHONE MODEL;" + // 7
            "FIRMWARE VERSION;" + // 8
            "HANDSET CAPABILITY [THROUGHPUT DL/UL];" + // 9
            "LAST ACQUISITION TIMING [YYYY_MM];" + // 10
            "NED MONTH [YYYY_MM];" + // 11
            "Country;" + // 12
            "OPERATOR NAME;" + // 13
            "MCC;" + // 14
            "MNC;" + // 15
            "OPCOS;" + // 16
            "RAN VENDOR;" + // 17
            "SCENARIO;" + // 18
            "DatabaseName;" + // 19
            "Service;" + // 20
            "Test;" + // 21
            "Sub-Test;" + // 22
            "KPI;" + // 23
            "KPI value;" + // 24
            "Latitude;" + // 25
            "Longitude"; //26
}