package com.peer.kafka.connect.smt;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class CustomDatetimeFormatTransformTest {

    private CustomDatetimeFormatTransform<SourceRecord> transform;

    @Before
    public void setup() {
        transform = new CustomDatetimeFormatTransform<>();
        Map<String, String> configs = new HashMap<>();
        configs.put("field.name", "firstEventTime");
        configs.put("target.type", "Timestamp");
        transform.configure(configs);
    }

    @Test
    public void testApply_withValidDateString_shouldConvertToTimestamp() {
        SourceRecord record = createSourceRecord("2023-05-15T08:12:51Z");
        SourceRecord transformedRecord = transform.apply(record);

        Object value = transformedRecord.value();
        Assert.assertTrue(value instanceof Map);

        Map<String, Object> valueMap = (Map<String, Object>) value;
        Object fieldValue = valueMap.get("firstEventTime");
        Assert.assertTrue(fieldValue instanceof Long);
        Assert.assertEquals(1684156371000L, fieldValue);
    }

    @Test
    public void testApply_withValidDateString_shouldConvertToTimeStamp2Format() {
        SourceRecord record = createSourceRecord("2023-05-15T02:30:31.908Z");
        SourceRecord transformedRecord = transform.apply(record);

        Object value = transformedRecord.value();
        Assert.assertTrue(value instanceof Map);

        Map<String, Object> valueMap = (Map<String, Object>) value;
        Object fieldValue = valueMap.get("firstEventTime");
        Assert.assertTrue(fieldValue instanceof Long);
        Assert.assertEquals(1684135831908L, fieldValue);
    }

    @Test
    public void testApply_withValidDateString_shouldConvertToTimeStamp3Format() {
        SourceRecord record = createSourceRecord("2021-01-28T09:21:24.0391729Z");
        SourceRecord transformedRecord = transform.apply(record);

        Object value = transformedRecord.value();
        Assert.assertTrue(value instanceof Map);

        Map<String, Object> valueMap = (Map<String, Object>) value;
        Object fieldValue = valueMap.get("firstEventTime");
        Assert.assertTrue(fieldValue instanceof Long);
        Assert.assertEquals(1611844075729L, fieldValue);
    }

    @Test(expected = DataException.class)
    public void testApply_withInvalidDateString_shouldThrowDataException() {
        SourceRecord record = createSourceRecord("2023-05-17T10:30:00");
        transform.apply(record);
    }

    private SourceRecord createSourceRecord(String timestampValue) {
        Map<String, Object> valueMap = new HashMap<>();
        valueMap.put("firstEventTime", timestampValue);

        Schema valueSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build();

        return new SourceRecord(null, null, "topic", 0, valueSchema, valueMap);
    }
}
