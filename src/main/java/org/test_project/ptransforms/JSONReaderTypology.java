package org.test_project.ptransforms;

import org.test_project.dofns.ExtractKpiIdsFromJson;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class JSONReaderTypology extends PTransform<PBegin, PCollectionView<Map<String, List<String>>>> {
    final static Logger logger = LoggerFactory.getLogger(JSONReaderTypology.class);

    private String jsonFilePattern;

    public JSONReaderTypology(String jsonFilePattern) {
        this.jsonFilePattern = jsonFilePattern;
    }

    @Override
    public PCollectionView<Map<String, List<String>>> expand(PBegin pipeline) {
        return pipeline
                .apply("Load JSON file", FileIO.match().filepattern(jsonFilePattern))
                .apply(FileIO.readMatches())
                .apply(ParDo.of(new ExtractKpiIdsFromJson()))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), ListCoder.of(StringUtf8Coder.of())))
                .apply(View.asMap());
    }
}
