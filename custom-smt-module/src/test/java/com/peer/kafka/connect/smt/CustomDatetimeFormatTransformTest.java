package com.peer.kafka.connect.smt;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class CustomDatetimeFormatTransformTest {

    private CustomDatetimeFormatTransform<SourceRecord> transform;

    @Before
    public void setup() {
        transform = new CustomDatetimeFormatTransform<>();
        Map<String, String> props = new HashMap<>();
        props.put(CustomDatetimeFormatTransform.FIELD_CONFIG, "timestamp");
        props.put(CustomDatetimeFormatTransform.TARGET_TYPE_CONFIG, "timestamp");
        transform.configure(props);
    }

    @Test
    public void testApply_ConvertsDatetimeStringToTimestamp() {
        Schema valueSchema = SchemaBuilder.struct()
                .field("timestamp", Schema.STRING_SCHEMA)
                .build();

        Map<String, Object> valueMap = new HashMap<>();
        //valueMap.put("timestamp", "2023-05-15T08:12:51Z");
        valueMap.put("timestamp", "2023-05-15T02:30:31.908Z");

        SourceRecord record = new SourceRecord(null, null, "topic", null, valueSchema, valueMap);

        SourceRecord transformed = transform.apply(record);

        assertNotNull(transformed.value());

        Map<String, Object> transformedValue = (Map<String, Object>) transformed.value();
        assertTrue(transformedValue.containsKey("timestamp"));
        assertTrue(transformedValue.get("timestamp") instanceof Long);
        //assertEquals(1684138371000L, transformedValue.get("timestamp"));
        assertEquals(1684117831908L, transformedValue.get("timestamp"));
    }

    @Test
    public void testApply_PreservesRecordHeaders() {
        Schema valueSchema = SchemaBuilder.struct()
                .field("timestamp", Schema.STRING_SCHEMA)
                .build();

        Map<String, Object> valueMap = new HashMap<>();
        valueMap.put("timestamp", "2023-05-15T08:12:51Z");

        SourceRecord record = new SourceRecord(null, null, "topic", null, valueSchema, valueMap);
        record.headers().addString("headerKey", "headerValue");

        SourceRecord transformed = transform.apply(record);

        assertNotNull(transformed.headers());

        assertNotNull(transformed.headers().lastWithName("headerKey"));
        assertEquals("headerValue", transformed.headers().lastWithName("headerKey").value());
    }

    @Test
    public void testApply_ReturnsUnmodifiedRecordWhenFieldIsNull() {
        Schema valueSchema = SchemaBuilder.struct()
                .field("timestamp", Schema.STRING_SCHEMA)
                .build();

        Map<String, Object> valueMap = new HashMap<>();
        valueMap.put("timestamp", null);

        SourceRecord record = new SourceRecord(null, null, "topic", null, valueSchema, valueMap);

        SourceRecord transformed = transform.apply(record);

        assertSame(record, transformed);
    }

    @Test(expected = org.apache.kafka.connect.errors.DataException.class)
    public void testApply_ThrowsDataExceptionWhenFieldIsNotString() {
        Schema valueSchema = SchemaBuilder.struct()
                .field("timestamp", Schema.INT32_SCHEMA)
                .build();

        Map<String, Object> valueMap = new HashMap<>();
        valueMap.put("timestamp", 12345);

        SourceRecord record = new SourceRecord(null, null, "topic", null, valueSchema, valueMap);

        transform.apply(record);
    }
}
