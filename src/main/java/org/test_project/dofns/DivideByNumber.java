package org.test_project.dofns;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class DivideByNumber extends DoFn<KV<String, Double>, KV<String, Double>> {

    final static Logger logger = LoggerFactory.getLogger(DivideByNumber.class);

    private PCollectionView<Map<String, Double>> pCollection;
    private double number;

    public DivideByNumber(PCollectionView<Map<String, Double>> pCollection, double number) {
        this.pCollection = pCollection;
        this.number = number;
    }

    @ProcessElement
    public void processElement(@Element KV<String, Double> element, OutputReceiver<KV<String, Double>> receiver, ProcessContext c) {
        Map numerator = c.sideInput(pCollection);
        Double num = (Double) numerator.get(element.getKey()) / number;
        receiver.output(KV.of(element.getKey(), (element.getValue() / num)));
    }
}
