package org.test_project.kpi_compound_transforms;

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
import org.test_project.constants.KpiIds;
import org.test_project.constants.SchemaRow;
import org.test_project.dofns.AddValueToKey;
import org.test_project.dofns.ConvertValueToPercentage;
import org.test_project.dofns.FilterFromKVRow;
import org.test_project.ptransforms.Division;
import org.test_project.ptransforms.Subtraction;

import java.util.ArrayList;
import java.util.List;

public class CalcSuccessRate_V2 extends PTransform<PCollection<KV<String, Row>>, PCollection<KV<String, Row>>> {

    final static Logger logger = LoggerFactory.getLogger(CalcSuccessRate_V2.class);

    private final List<PCollection<KV<String, Row>>> listPCollection = new ArrayList<>();

    private PCollection<KV<String, Row>> pCollectionCountErrorCodeNot0;

    public CalcSuccessRate_V2(PCollection<KV<String, Row>> pCollectionCountErrorCodeNot0) {
        this.pCollectionCountErrorCodeNot0 = pCollectionCountErrorCodeNot0;
    }

    @Override
    public PCollection<KV<String, Row>> expand(PCollection<KV<String, Row>> pCollectionInput) {

        int mapSize = KpiIds.TP_08.size();

        for (int i = 0; i < mapSize; i++) {

            PCollection<KV<String, Row>> temp = pCollectionInput
                    .apply(ParDo.of(new FilterFromKVRow(KpiIds.TP_08.get(i).get(0))));

            int listSize = KpiIds.TP_08.get(i).size();

            PCollection<KV<String, Row>> result = null;

            for (int j = 0; j < listSize - 1; j++) {
                result = temp
                        .apply(new Subtraction(pCollectionCountErrorCodeNot0, KpiIds.TP_08.get(i).get(j)));
            }
//                            .apply(new Subtraction(pCollectionCountErrorCodeNot0, KpiIds.TP_08.get(i).get(x))) // DK_"position_02"
//                            .apply(new Subtraction(pCollectionCountErrorCodeNot0, KpiIds.TP_08.get(i).get(1))) // DK_"position_03"
//                            .apply(new Subtraction(pCollectionCountErrorCodeNot0, KpiIds.TP_08.get(i).get(2))) // DK_"position_04"
//                            .apply(new Subtraction(pCollectionCountErrorCodeNot0, KpiIds.TP_08.get(i).get(3))) // DK_"position_05"

            listPCollection.add(result
                    .apply(new Division(pCollectionInput, KpiIds.TP_08.get(i).get(0))) // DK_"denominator"
                    .apply(ParDo.of(new ConvertValueToPercentage()))
                    .apply(ParDo.of(new AddValueToKey(KpiIds.TP_08.get(i).get(listSize - 1), 7))));
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
