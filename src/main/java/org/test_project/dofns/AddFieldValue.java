package org.test_project.dofns;

import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AddFieldValue extends DoFn<String, String>{

    final static Logger logger = LoggerFactory.getLogger(AddFieldValue.class);

    private String value;
    private int position;

    public AddFieldValue(String value, int position) {
        this.value = value;
        this.position = position;
    }

    @ProcessElement
    public void processElement(@Element String element, OutputReceiver<String> out) {

        String[] words = element.split(";", -1);
        StringBuilder stringBuilder = new StringBuilder();

        if(position == words.length){

            stringBuilder
                    .append(element)
                    .append(";")
                    .append(value);

        } else{

            for(int i = 0, size = words.length; i < size; i++){
                if(i == position){
                    stringBuilder.append(value).append(";");
                }
                stringBuilder.append(words[i]).append(";");
            }
            stringBuilder.deleteCharAt(stringBuilder.length() - 1); // remove last ";"

        }
        out.output(stringBuilder.toString());
    }
}
