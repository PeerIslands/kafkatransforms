package com.peer.kafka.connect.smt;


import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;

import org.apache.kafka.connect.transforms.util.SimpleConfig;


import java.util.Map;

public class CustomArrayToStrTransform<R extends ConnectRecord<R>> implements Transformation<R> {
    public static final String FIELD1_CONFIG = "element1";
    public static final String FIELD2_CONFIG = "element2";
    public static final String ARRAY1_CONFIG = "array1";
    public static final String ARRAY2_CONFIG = "array2";

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELD1_CONFIG, Type.STRING, Importance.HIGH, "Name of the first element containing the array.")
            .define(FIELD2_CONFIG, Type.STRING, Importance.HIGH, "Name of the second element containing the array.")
            .define(ARRAY1_CONFIG, Type.STRING, Importance.HIGH, "Name of the first array within the first element.")
            .define(ARRAY2_CONFIG, Type.STRING, Importance.HIGH, "Name of the second array within the second element.");

    private String element1;
    private String element2;
    private String array1;
    private String array2;

    @Override
    public R apply(R record) {
        try {
            // Get the JSON payload from the Kafka record
            String jsonString = (String) record.value();

            // Use Jayway JsonPath library to parse the JSON string
            Configuration jsonConfig = Configuration.builder().options(Option.DEFAULT_PATH_LEAF_TO_NULL).build();
            DocumentContext jsonContext = JsonPath.using(jsonConfig).parse(jsonString);

            // Get the values of the two arrays as strings
            String array1String = jsonContext.read("$." + element1 + "." + array1 + "[0]").toString();
            String array2String = jsonContext.read("$." + element2 + "." + array2+ "[0]").toString();

            // Replace the original array values with the strings in the JSON payload
            jsonContext.set("$." + element1 + "." + array1, array1String);
            jsonContext.set("$." + element2 + "." + array2, array2String);

            // Update the Kafka record with the modified JSON payload
            R updatedRecord = record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    record.keySchema(),
                    record.key(),
                    record.valueSchema(),
                    jsonContext.jsonString(),
                    record.timestamp()
            );

            return updatedRecord;
        } catch (Exception e) {
            throw new DataException("Failed to convert arrays to strings in JSON payload: " + e.getMessage());
        }
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        element1 = config.getString(FIELD1_CONFIG);
        element2 = config.getString(FIELD2_CONFIG);
        array1 = config.getString(ARRAY1_CONFIG);
        array2 = config.getString(ARRAY2_CONFIG);
    }

    @Override
    public void close() {
        // Do nothing
    }
}
