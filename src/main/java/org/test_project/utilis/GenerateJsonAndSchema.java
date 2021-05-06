package org.test_project.utilis;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;

public class GenerateJsonAndSchema {

    private final static Logger logger = LoggerFactory.getLogger(GenerateJsonAndSchema.class);

    private final static Character SEPARATOR = ';';

    private final static String FIELD = "field";
    private final static String TYPE = "type";

    // private final static String[] TYPES = {"STRING", "INT32", "DOUBLE", "DATETIME"};

    // methods to read Csv File

    public static CSVParser readCsvFile(String fileInputPattern) throws IOException {
        return readCsvFile(new File(fileInputPattern));
    }

    public static CSVParser readCsvFile(File inputFile) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new FileReader(inputFile));
        CSVParser csvParser = CSVParser.parse(bufferedReader, CSVFormat
                .DEFAULT
                .withFirstRecordAsHeader()
                .withNullString("")
                .withDelimiter(SEPARATOR));
        return csvParser;
    }

    // methods to get header names

    public static List<String> getHeaderNames(String inputFilePattern) throws IOException {
        return readCsvFile(new File(inputFilePattern)).getHeaderNames();
    }

    public static List<String> getHeaderNames(File inputFile) throws IOException {
        return readCsvFile(inputFile).getHeaderNames();
    }

    // methods to write Json File

    public static void writeJsonFile(String inputFilePattern, String outputFilePattern) throws IOException {
        writeJsonFile(readCsvFile(new File(inputFilePattern)), outputFilePattern);
    }

    public static void writeJsonFile(File inputFile, String outputFilePattern) throws IOException {
        writeJsonFile(readCsvFile(inputFile), outputFilePattern);
    }

    public static void writeJsonFile(CSVParser csvParser, String outputFilePattern) throws IOException {
        List<String> headerNames = csvParser.getHeaderNames();
        JSONArray jsonArray = new JSONArray();

        for (String headerName : headerNames) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put(FIELD, headerName);
            jsonObject.put(TYPE, "");
            jsonArray.add(jsonObject);
        }

        FileWriter file = new FileWriter(outputFilePattern);
        file.write(jsonArray.toString());
        file.flush();
        file.close();
    }


    // methods to read Json File

    public static Schema readJsonFile(String inputFilePattern) throws IOException, ParseException {
        return readJsonFile(new File(inputFilePattern));
    }

    public static Schema readJsonFile(File inputFile) throws IOException, ParseException {
        FileReader fileReader = new FileReader(inputFile);
        JSONParser jsonParser = new JSONParser();

        JSONArray jsonArray = (JSONArray) jsonParser.parse(fileReader);

        Schema.Builder schemaBuilder = new Schema.Builder();

        for (int i = 0, size = jsonArray.size(); i < size; i++) {
            JSONObject jsonObject = (JSONObject) jsonArray.get(i);

            switch (jsonObject.get(TYPE).toString()) {
                case "INT32":
                    schemaBuilder.addNullableField(jsonObject.get(FIELD).toString(), Schema.FieldType.INT32);
                    break;
                case "DOUBLE":
                    schemaBuilder.addNullableField(jsonObject.get(FIELD).toString(), Schema.FieldType.DOUBLE);
                    break;
                case "DATETIME":
                    schemaBuilder.addNullableField(jsonObject.get(FIELD).toString(), Schema.FieldType.DATETIME);
                    break;
                default:
                    schemaBuilder.addNullableField(jsonObject.get(FIELD).toString(), Schema.FieldType.STRING);
            }
        }

        return schemaBuilder.build();
    }

    // methods to save schema

    public static void saveSchema(String filePattern, Schema schema) throws IOException {
        saveSchema(new File(filePattern), schema);
    }

    public static void saveSchema(File file, Schema schema) throws IOException {
        FileOutputStream fileOutputStream = new FileOutputStream(file);
        ObjectOutput objectOutput = new ObjectOutputStream(fileOutputStream);
        objectOutput.writeObject(schema);
        objectOutput.close();
        fileOutputStream.close();
    }

    // methods to load schema

    public static Schema loadSchema(String filePattern) throws IOException, ClassNotFoundException {
        return loadSchema(new File(filePattern));
    }

    public static Schema loadSchema(File file) throws IOException, ClassNotFoundException {
        FileInputStream fileInputStream = new FileInputStream(file);
        ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
        Schema schema = (Schema) objectInputStream.readObject();
        objectInputStream.close();
        fileInputStream.close();
        return schema;
    }

}
