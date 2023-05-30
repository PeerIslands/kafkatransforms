package com.peer.kafka.connect.smt;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
public class CustomTimestampConverterTest {

    private Object transformedValue;
    private Object value;

    @Test
    public void testApplyTransformation() {
        // Create a sample record with the target field
        Map<String, Object> value = new HashMap<>();
        value.put("firstEventTime","2021-02-08T08:30:16.775Z");
        Schema valueSchema = Schema.STRING_SCHEMA;
        String topic = "test-topic";
        int partition = 0;

        SourceRecord record = new SourceRecord(
                null,
                null,
                topic,
                partition,
                valueSchema,
                value
        );

        // Set up the configuration for the transform
        Map<String, String> config = new HashMap<>();
        config.put("field", "firstEventTime");
        config.put("target.type", "Timestamp");
        config.put("date.formats", "yyyy-MM-dd'T'HH:mm:ss'Z',yyyy-MM-dd'T'HH:mm:ss.S'Z',yyyy-MM-dd'T'HH:mm:ss.SS'Z',yyyy-MM-dd'T'HH:mm:ss.SSS'Z',yyyy-MM-dd'T'HH:mm:ss.SSSS'Z',yyyy-MM-dd'T'HH:mm:ss.SSSSS'Z',yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z',yyyy-MM-dd'T'HH:mm:ss.SSSSSSS'Z'");
//yyyy-MM-dd'T'HH:mm:ss.SSS'Z'
        //yyyy-MM-dd'T'HH:mm:ss.SSSSSSS'Z'
        // Create the transform instance and configure it
        Transformation<SourceRecord> transform = new CustomTimestampConverter<>();
        transform.configure(config);

        // Apply the transformation
        SourceRecord transformedRecord = transform.apply(record);

        // Verify the transformed record
        assertEquals(topic, transformedRecord.topic());
        assertEquals(valueSchema, transformedRecord.valueSchema());

        // Verify the transformed value
        transformedValue = transformedRecord.value();

        // Verify the transformed timestamp field
        Object transformedField = ((Map<?, ?>) transformedValue).get("firstEventTime");
        //assertNull(transformedField);

        assertEquals("2021-02-08 03:30:16.775", transformedField.toString());
//2023-05-15T08:12:51.0
        // Clean up resources
        transform.close();
    }
}
