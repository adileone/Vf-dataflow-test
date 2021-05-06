package org.test_project.dofns;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterFromKVRow extends DoFn<KV<String, Row>, KV<String, Row>> {

    final static Logger logger = LoggerFactory.getLogger(FilterFromKVRow.class);

    private String valueToTake;

    public FilterFromKVRow(String valueToTake) {
        this.valueToTake = valueToTake;
    }

    @ProcessElement
    public void processElement(@Element KV<String, Row> element, OutputReceiver<KV<String, Row>> out) {

        String[] keys = element.getKey().split(";", -1);
        String kpiId = keys[keys.length - 1]; // position of KPIId value

        if (valueToTake.equals(kpiId)) {
            out.output(KV.of(element.getKey(), element.getValue()));
        }
    }
}
