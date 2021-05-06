package org.test_project.dofns;

import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.channels.Channels;


public class ExtractRecordFromCsv extends DoFn<FileIO.ReadableFile, CSVRecord> {

    final static Logger logger = LoggerFactory.getLogger(ExtractRecordFromCsv.class);

    private final static Character SEPARATOR = ';';

    @ProcessElement
    public void processElement(@Element FileIO.ReadableFile element, OutputReceiver<CSVRecord> out) {

        try {
            InputStream inputStream = Channels.newInputStream(element.open());
            Reader reader = new InputStreamReader(inputStream);
            CSVParser csvParser = CSVParser.parse(reader,
                    CSVFormat.DEFAULT
                            .withFirstRecordAsHeader()
                            .withIgnoreHeaderCase(true)
                            .withIgnoreEmptyLines()
                            .withDelimiter(SEPARATOR)
                            .withNullString("")
                            .withTrim()
                            .withQuoteMode(QuoteMode.ALL));

            Iterable<CSVRecord> records = csvParser.getRecords();

            for (CSVRecord record : records) {
                // logger.info(record.get(0).toString());
                out.output(record);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
