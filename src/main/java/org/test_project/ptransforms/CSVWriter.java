package org.test_project.ptransforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.test_project.dofns.AddFieldValue;
import org.test_project.dofns.ConvertDateTimeToStringFormat;
import org.test_project.dofns.CreateStringFromRow;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CSVWriter extends PTransform<PCollection<KV<String, Row>>, PDone>{

    final static Logger logger = LoggerFactory.getLogger(CSVWriter.class);

    private String fileName;
    private String header ;
    private String PATH = "./csv/output-aggregation";

    public CSVWriter(String fileName, String header){
        this.fileName = fileName;
        this.header = header;
    }


    @Override
    public PDone expand(PCollection<KV<String, Row>> pCollection) {

        pCollection
//                .apply(ParDo.of(new CreateStringFromRow()))
                .apply(ParDo.of(new DoFn<KV<String, Row>, String>() {

                    @ProcessElement
                    public void processElement(@Element KV<String, Row> element, OutputReceiver<String> out) {

                        StringBuilder stringBuilder = new StringBuilder();

                        stringBuilder
                                .append(element.getKey())
                                .append(";")
                                .append(element.getValue().getValue(0) != null ? element.getValue().getValue(0).toString() : "null")
                                .append(";")
                                .append(element.getValue().getValue(1) != null ? element.getValue().getValue(1).toString() : "null")
                                .append(";")
                                .append(element.getValue().getValue(2) != null ? element.getValue().getValue(2).toString() : "null")
                                .append(";")
                                .append(element.getValue().getValue(3) != null ? element.getValue().getValue(3).toString() : "null");

                        out.output(stringBuilder.toString());
                    }
                }))
                .apply(FileIO.<String>write().withNumShards(1).via(TextIO.sink().withHeader(header)).to(PATH).withPrefix(fileName).withNoSpilling().withSuffix(".csv"));

        return PDone.in(pCollection.getPipeline());
    }
}
