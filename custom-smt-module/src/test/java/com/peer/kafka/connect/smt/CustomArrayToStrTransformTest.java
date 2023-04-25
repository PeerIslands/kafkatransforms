package com.peer.kafka.connect.smt;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;


public class CustomArrayToStrTransformTest {

    private CustomArrayToStrTransform<SourceRecord> transform;

    /*@Before
    public void setup() {
        transform = new CustomArrayToStrTransform<>();
    }*/

    @Test
    public void testApplyWithValidInput() {
        transform = new CustomArrayToStrTransform<>();
        // Create a test JSON string
        String jsonString = "{\"id\":1, \"element1\":{ \"array1\": [\"value1\"]}, \"element2\": {\"array2\": [\"value4\"]}}";

        // Create a test Kafka record with the JSON payload
        SourceRecord record = new SourceRecord(
                null,
                null,
                "test-topic",
                null,
                null,
                null,
                Schema.STRING_SCHEMA,
                jsonString,
                null
        );

        // Create a configuration for the transform
        Map<String, String> configMap = new HashMap<>();
        configMap.put(transform.FIELD1_CONFIG, "element1");
        configMap.put(transform.FIELD2_CONFIG, "element2");
        configMap.put(transform.ARRAY1_CONFIG, "array1");
        configMap.put(transform.ARRAY2_CONFIG, "array2");
        CustomArrayToStrTransform<SourceRecord> transform = new CustomArrayToStrTransform<>();
        transform.configure(configMap);

        // Apply the transform to the Kafka record
        SourceRecord transformedRecord = transform.apply(record);

        // Verify that the JSON payload has been modified correctly
        String transformedJsonString = (String) transformedRecord.value();
        assertEquals("{\"id\":1,\"element1\":{\"array1\":\"value1\"},\"element2\":{\"array2\":\"value4\"}}",transformedJsonString);
    }

    }

