package org.test_project.dofns;

import org.test_project.constants.SchemaRow;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateRowFromGBK extends DoFn<KV<String, CoGbkResult>, KV<String, Row>> {

    final static Logger logger = LoggerFactory.getLogger(CreateRowFromGBK.class);

    private TupleTag<Row> firstTupleTag;
    private TupleTag<Row> secondTupleTag;

    public CreateRowFromGBK(TupleTag<Row> firstTupleTag, TupleTag<Row> secondTupleTag) {
        this.firstTupleTag = firstTupleTag;
        this.secondTupleTag = secondTupleTag;
    }

    @ProcessElement
    public void processElement(@Element KV<String, CoGbkResult> element, OutputReceiver<KV<String, Row>> out) {

        element.getValue().getAll(firstTupleTag).iterator().forEachRemaining(firstRow -> {
            element.getValue().getAll(secondTupleTag).iterator().forEachRemaining(secondRow -> {
                Row.Builder rowBuilder = Row.withSchema(SchemaRow.SCHEMA_KPI_RESULT);
                rowBuilder
                        .addValues(firstRow.getValues())
                        .addValues(secondRow.getValues());

                out.output(KV.of(element.getKey(), rowBuilder.build()));
            });
        });
    }
}
