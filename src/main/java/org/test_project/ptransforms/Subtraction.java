package org.test_project.ptransforms;

import org.test_project.dofns.FilterFromKVRow;
import org.test_project.dofns.RemoveValueFromKey;
import org.test_project.dofns.SubtractByView;
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

public class Subtraction extends PTransform<PCollection<KV<String, Row>>, PCollection<KV<String, Row>>> {

    final static Logger logger = LoggerFactory.getLogger(Subtraction.class);

    private PCollection<KV<String, Row>> subtrahend;
    private String kpiId;

    public Subtraction(PCollection<KV<String, Row>> subtrahend, String kpiId) {
        this.subtrahend = subtrahend;
        this.kpiId = kpiId;
    }

    @Override
    public PCollection<KV<String, Row>> expand(PCollection<KV<String, Row>> minuend) {

        PCollectionView<Map<String, Row>> subView = subtrahend
                .apply(ParDo.of(new FilterFromKVRow(kpiId)))
                .apply(ParDo.of(new RemoveValueFromKey(7))) // 7 is position of KPIID value
                .apply(View.asMap());

        return minuend
                .apply(ParDo.of(new RemoveValueFromKey(7)))
                .apply(ParDo.of(new SubtractByView(subView)).withSideInputs(subView));
    }
}
