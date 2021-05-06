package org.test_project.dofns;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoveRowWithoutJoin extends DoFn<KV<String, CoGbkResult>, KV<String, CoGbkResult>> {

    final static Logger logger = LoggerFactory.getLogger(RemoveRowWithoutJoin.class);

    private TupleTag<Row> tupleTagFirst;
    private TupleTag<Row> tupleTagSecond;

    public RemoveRowWithoutJoin(TupleTag<Row> tupleTagFirst, TupleTag<Row> tupleTagSecond) {
        this.tupleTagFirst = tupleTagFirst;
        this.tupleTagSecond = tupleTagSecond;
    }

    @ProcessElement
    public void processElement(@Element KV<String, CoGbkResult> element, OutputReceiver<KV<String, CoGbkResult>> out) {

        if(element.getValue().getAll(tupleTagFirst).iterator().hasNext() && element.getValue().getAll(tupleTagSecond).iterator().hasNext()){
            out.output(KV.of(element.getKey(), element.getValue()));
        } else {
            // print KPI ID without join
//            logger.info(element.getKey());
        }
    }
}
