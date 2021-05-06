package org.test_project.ptransforms;

import org.test_project.dofns.DivideByView;
import org.test_project.dofns.FilterFromKVRow;
import org.test_project.dofns.RemoveValueFromKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class Division extends PTransform<PCollection<KV<String, Row>>, PCollection<KV<String, Row>>> {

    final static Logger logger = LoggerFactory.getLogger(Division.class);

    private PCollection<KV<String, Row>> denominator;
    private String kpiId;

    public Division(PCollection<KV<String, Row>> denominator) {
        this.denominator = denominator;
    }

    public Division(PCollection<KV<String, Row>> denominator, String kpiId) {
        this.denominator = denominator;
        this.kpiId = kpiId;
    }

    @Override
    public PCollection<KV<String, Row>> expand(PCollection<KV<String, Row>> num) {

        PCollectionView<Map<String, Row>> denView;

        if(kpiId == null){

            denView = denominator
                    .apply(View.asMap());
            return num
                    .apply(ParDo.of(new DivideByView(denView)).withSideInputs(denView));

        } else {

            denView = denominator
                    .apply(ParDo.of(new FilterFromKVRow(kpiId)))
                    .apply(ParDo.of(new RemoveValueFromKey(7)))
                    .apply(View.asMap());
            return num
                    .apply(ParDo.of(new RemoveValueFromKey(7)))
                    .apply(ParDo.of(new DivideByView(denView)).withSideInputs(denView));

        }

    }
}