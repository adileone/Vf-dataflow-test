package org.test_project.ptransforms;

import org.test_project.constants.SchemaRow;
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
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateKVRowAccessoryParameters extends PTransform<PCollection<Row>, PCollection<KV<String, Row>>> {

    final static Logger logger = LoggerFactory.getLogger(CreateKVRowAccessoryParameters.class);

    private String[] keys;
    private String[] accessory_parameters;
    private Schema schema;

    public CreateKVRowAccessoryParameters(String[] keys, String[] accessory_parameters, Schema schema) {
        this.keys = keys;
        this.accessory_parameters = accessory_parameters;
        this.schema = schema;
    }

    @Override
    public PCollection<KV<String, Row>> expand(PCollection<Row> pCollection) {

        return pCollection
                .apply(ParDo.of(new CreateKVRowFromRow(keys))).setCoder(KvCoder.of(StringUtf8Coder.of(), RowCoder.of(schema)))
                .apply(ParDo.of(new DoFn<KV<String, Row>, KV<String, Row>>() {

                    @ProcessElement
                    public void processElement(@Element KV<String, Row> element, OutputReceiver<KV<String, Row>> out) {

                        Row.Builder rowBuilder = Row.withSchema(SchemaRow.SCHEMA_ACCESSORY_PARAMETERS);

                        for (String p : accessory_parameters) {
                            rowBuilder.addValue(element.getValue().getValue(p));
                        }

                        out.output(KV.of(element.getKey(), rowBuilder.build()));
                    }
                })).setCoder(KvCoder.of(StringUtf8Coder.of(), RowCoder.of(SchemaRow.SCHEMA_ACCESSORY_PARAMETERS)));
    }
}
