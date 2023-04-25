/*package com.peer.kafka.connect.smt;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class CustomArrayToStrTransformTest {
    private CustomArrayToStrTransform<SourceRecord> transform;

    @Before
    public void setup() {

    }

    @Test
    public void testApply() {

        transform = new CustomArrayToStrTransform<>();
        Map<String, String> configs = new HashMap<>();
        configs.put(CustomArrayToStrTransform.FIELD1_CONFIG, "element1");
        configs.put(CustomArrayToStrTransform.FIELD2_CONFIG, "element2");
        configs.put(CustomArrayToStrTransform.ARRAY1_CONFIG, "array1");
        configs.put(CustomArrayToStrTransform.ARRAY2_CONFIG, "array2");
        transform.configure(configs);

        Schema schema = SchemaBuilder.struct()
                .field("element1", SchemaBuilder.struct()
                        .field("array1", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                        .build())
                .field("element2", SchemaBuilder.struct()
                        .field("array2", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                        .build())
                .build();

        Struct value = new Struct(schema)
                .put("element1", new Struct(schema.field("element1").schema())
                        .put("array1", Collections.singletonList("value1")))
                .put("element2", new Struct(schema.field("element2").schema())
                        .put("array2", Collections.singletonList("value2")));

        SourceRecord record = new SourceRecord(null, null, "test", null,
                schema, value);

        SourceRecord transformedRecord = transform.apply(record);

        assertTrue(transformedRecord != null);

    }
}

 */
