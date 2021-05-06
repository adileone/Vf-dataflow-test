package org.test_project.ptransforms;

import org.test_project.dofns.CreateRowFromRecord;
import org.test_project.dofns.ExtractRecordFromCsv;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CSVReader extends PTransform<PBegin, PCollection<Row>> {
    final static Logger logger = LoggerFactory.getLogger(CSVReader.class);

    private Schema schema;
    private String csvFilePattern;

    public CSVReader(String csvFilePattern, Schema schema) {
        this.schema = schema;
        this.csvFilePattern = csvFilePattern;
    }

    @Override
    public PCollection<Row> expand(PBegin pipeline) {
        return pipeline
                .apply("Load CSV file", FileIO.match().filepattern(csvFilePattern))
                .apply(FileIO.readMatches())
                .apply("Read a record from CSV File", ParDo.of(new ExtractRecordFromCsv()))
                .apply("Convert record to row", ParDo.of(new CreateRowFromRecord(schema))).setRowSchema(schema);
    }
}
