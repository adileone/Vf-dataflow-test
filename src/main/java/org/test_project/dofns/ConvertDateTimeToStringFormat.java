package org.test_project.dofns;

import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;

public class ConvertDateTimeToStringFormat extends DoFn<String, String> {

    final static Logger logger = LoggerFactory.getLogger(ConvertDateTimeToStringFormat.class);

    private final static Locale LOCALE = new Locale("en", "EN");
    private final static String DATE_PATTERN = "yyyy_MM";

    public ConvertDateTimeToStringFormat(){

    }

    @ProcessElement
    public void processElement(@Element String element, OutputReceiver<String> out) {

        String[] words = element.split(";", -1);

        StringBuilder stringBuilder = new StringBuilder();

        for(int i = 0, size = words.length; i < size; i++){
            if(i == 9 || i == 10){
                Instant instant = Instant.parse(words[i]);
                DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern(DATE_PATTERN).withLocale(LOCALE);
                words[i] = dateTimeFormatter.print(instant);
            }
            stringBuilder.append(words[i]).append(";");
        }
        stringBuilder.deleteCharAt(stringBuilder.length() - 1); // remove last ";"

        out.output(stringBuilder.toString());
    }
}
