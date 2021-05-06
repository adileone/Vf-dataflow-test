package org.test_project.ptransforms;

import org.test_project.constants.Field;
import org.test_project.dofns.CreateKVRowFromRow;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class FilterFromRow extends PTransform<PCollection<Row>, PCollection<KV<String, Row>>> {

    final static Logger logger = LoggerFactory.getLogger(FilterFromRow.class);

    private String[] keys;
    private String[] technologies;
    private String category;
    private PCollectionView<Map<String, List<String>>> pCollectionView;
    private Schema schema;

    public FilterFromRow(String[] keys, String[] technologies, String category, PCollectionView<Map<String, List<String>>> pCollectionView, Schema schema) {
        this.keys = keys;
        this.technologies = technologies;
        this.category = category;
        this.pCollectionView = pCollectionView;
        this.schema = schema;
    }

    @Override
    public PCollection<KV<String, Row>> expand(PCollection<Row> pCollection) {

        return pCollection
                .apply(ParDo.of(new DoFn<Row, Row>() {

                    @ProcessElement
                    public void processElement(@Element Row row, OutputReceiver<Row> out, ProcessContext c) {

                        Map<String, List<String>> map = c.sideInput(pCollectionView);

                        for (String tech : technologies) {
                            String key = tech + "." + category;
                            if (map != null && map.containsKey(key) && map.get(key).contains(row.getString(Field.KPI_ID))) {
                                out.output(row);
                            }
                        }

                    }
                }).withSideInputs(pCollectionView)).setRowSchema(schema)
                .apply(ParDo.of(new CreateKVRowFromRow(keys))).setCoder(KvCoder.of(StringUtf8Coder.of(), RowCoder.of(schema)));
    }
}
