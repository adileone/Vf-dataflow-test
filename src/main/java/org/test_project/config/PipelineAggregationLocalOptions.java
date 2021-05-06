package org.test_project.config;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

public interface PipelineAggregationLocalOptions extends DataflowPipelineOptions {

    @Description("Path of the file to read from")
    // @Default.String("./csv/*.csv")
    String getInputFile();
    void setInputFile(String input);

    @Description("Path of the file to write to")
    @Default.String("./csv/output")
    String getOutput();
    void setOutput(String output);

}
