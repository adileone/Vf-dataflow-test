package org.test_project;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.test_project.config.PipelineAggregationLocalOptions;
import org.test_project.constants.Field;
import org.test_project.constants.Typology;
import org.test_project.kpi_compound_transforms.CalcSuccessRate_V1;
import org.test_project.ptransforms.AddKpiName;
import org.test_project.ptransforms.CSVReader;
import org.test_project.ptransforms.CSVWriter;
import org.test_project.ptransforms.CountRows;
import org.test_project.ptransforms.FilterFromRow;
import org.test_project.ptransforms.JSONReaderKpiNames;
import org.test_project.ptransforms.JSONReaderTypology;
import org.test_project.utilis.Constants;
import org.test_project.utilis.GenerateJsonAndSchema;

public class PipelineRunner {

    private final static Logger logger = LoggerFactory.getLogger(PipelineRunner.class);

    private static Schema schema;

    public static void main(String[] args) throws ClassNotFoundException, IOException {

        PipelineAggregationLocalOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(PipelineAggregationLocalOptions.class);

        // File csvFile = new File(Constants.CSV_FILE_PATTERN);
        // File jsonFile = new File(Constants.JSON_WITH_TYPES_FILE_PATTERN);
        File schemaFile = new File(Constants.SCHEMA_SERIALIZED_FILE_PATTERN);

        options.setProject("exemplary-works-305313");
        options.setStagingLocation("gs://dataflow-test-vf/dataflow-staging");
        options.setRunner(DataflowRunner.class);
        options.setRegion("europe-west6");

        schema = GenerateJsonAndSchema.loadSchema(schemaFile);

        runPipeline(options);
    }

    public static void runPipeline(PipelineAggregationLocalOptions options) {

        logger.info(String.format("Pipeline [%s]: STARTED", PipelineRunner.class.getSimpleName()));

        Pipeline p = Pipeline.create(options);

        PCollectionView<Map<String, List<String>>> MAP_KPI_IDS = p
                .apply(new JSONReaderTypology(Constants.JSON_KPI_IDS_FILE_PATTERN));

        PCollectionView<Map<String, String>> MAP_KPI_ID_NAMES = p
                .apply(new JSONReaderKpiNames(Constants.JSON_KPI_ID_NAMES_FILE_PATTERN));

        // Loaded select CSV file
        PCollection<Row> ROWS_COLLECTION = p.apply("Load and parse CSV file",
                new CSVReader(Constants.CSV_FILE_PATTERN, schema));

        // Calc COUNT
        PCollection<KV<String, Row>> RESULT_ROWS_COUNT = ROWS_COLLECTION.apply(
                new FilterFromRow(Constants.KEY_FIELDS, Constants.TECHNOLOGIES, Typology.COUNT, MAP_KPI_IDS, schema))
                .apply(new CountRows(schema));

        // Calc COUNT_ERROR_CODE_NOT_0
        PCollection<KV<String, Row>> RESULT_ROWS_COUNT_ERROR_CODE_NOT_0 = ROWS_COLLECTION
                .apply(new FilterFromRow(Constants.KEY_FIELDS, Constants.TECHNOLOGIES, Typology.COUNT_ERROR_CODE_NOT_0,
                        MAP_KPI_IDS, schema))
                .apply(new CountRows(Field.ERROR_CODE, "!=", "0", schema));

        // --------------------

        // Calc SUCCESS_RATE
        PCollection<KV<String, Row>> RESULT_ROWS_SUCCESS_RATE = RESULT_ROWS_COUNT.apply(new CalcSuccessRate_V1(RESULT_ROWS_COUNT_ERROR_CODE_NOT_0));

        // Union result
        PCollection<KV<String, Row>> UNION_RESULT = PCollectionList
                .of(RESULT_ROWS_COUNT.apply(new AddKpiName(Typology.COUNT, MAP_KPI_ID_NAMES)))
                .and(RESULT_ROWS_COUNT_ERROR_CODE_NOT_0
                        .apply(new AddKpiName(Typology.COUNT_ERROR_CODE_NOT_0, MAP_KPI_ID_NAMES)))
                .and(RESULT_ROWS_SUCCESS_RATE.apply(new AddKpiName("Calc-SuccessRate", MAP_KPI_ID_NAMES)))
                .apply(Flatten.pCollections());

        final String HEADER = "TECHNOLOGY;CITIES ROUTE LINES PLACE (sub-target);AREAS OF INTEREST;OPERATOR NAME;RAN VENDOR;Test;Sub-Test;KPI ID;KPI Name;KPI value;Latitude;Longitude";

        UNION_RESULT.apply("Write output in CSV File", new CSVWriter("output-KPI-calculation", HEADER));

        p.run().waitUntilFinish();
    }
}