package org.test_project.kpi_compound_transforms;

import org.test_project.constants.KpiIds;
import org.test_project.constants.SchemaRow;
import org.test_project.dofns.AddValueToKey;
import org.test_project.dofns.ConvertValueToPercentage;
import org.test_project.dofns.FilterFromKVRow;
import org.test_project.ptransforms.Division;
import org.test_project.ptransforms.Subtraction;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class CalcSuccessRate_V1 extends PTransform<PCollection<KV<String, Row>>, PCollection<KV<String, Row>>> {

    final static Logger logger = LoggerFactory.getLogger(CalcSuccessRate_V1.class);

    private final List<PCollection<KV<String, Row>>> listPCollection = new ArrayList<>();

    private PCollection<KV<String, Row>> pCollectionCountErrorCodeNot0;

    public CalcSuccessRate_V1(PCollection<KV<String, Row>> pCollectionCountErrorCodeNot0) {
        this.pCollectionCountErrorCodeNot0 = pCollectionCountErrorCodeNot0;
    }

    @Override
    public PCollection<KV<String, Row>> expand(PCollection<KV<String, Row>> pCollectionInput) {

        // int mapSize = KpiIds.TP_08.size();
        int mapSize = 3;

        for (int i = 0; i < mapSize; i++) {

            listPCollection.add(pCollectionInput
                    .apply(ParDo.of(new FilterFromKVRow(KpiIds.TP_08.get(i).get(0))))
                    .apply(new Subtraction(pCollectionCountErrorCodeNot0, KpiIds.TP_08.get(i).get(0))) // DK_"position_02"
                    .apply(new Subtraction(pCollectionCountErrorCodeNot0, KpiIds.TP_08.get(i).get(1))) // DK_"position_03"
                    .apply(new Subtraction(pCollectionCountErrorCodeNot0, KpiIds.TP_08.get(i).get(2))) // DK_"position_04"
                    .apply(new Subtraction(pCollectionCountErrorCodeNot0, KpiIds.TP_08.get(i).get(3))) // DK_"position_05"
                    .apply(new Division(pCollectionInput, KpiIds.TP_08.get(i).get(0))) // DK_"denominator"
                    .apply(ParDo.of(new ConvertValueToPercentage()))
                    .apply(ParDo.of(new AddValueToKey(KpiIds.TP_08.get(i).get(4), 7))));
        }

        return PCollectionList.of(listPCollection).apply(Flatten.pCollections()).setCoder(KvCoder.of(StringUtf8Coder.of(), RowCoder.of(SchemaRow.SCHEMA_VALUE_LAT_LON)));
    }

    private String createKey(List<String> list) {

        StringBuilder newKey = new StringBuilder();
        for (String s : list) {
            newKey
                    .append(s)
                    .append("_");
        }

        newKey.deleteCharAt(newKey.length() - 1);

        return newKey.toString();
    }
}
