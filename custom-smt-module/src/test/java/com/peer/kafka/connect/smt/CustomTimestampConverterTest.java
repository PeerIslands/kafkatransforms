package com.peer.kafka.connect.smt;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import static org.junit.Assert.assertEquals;

public class CustomTimestampConverterTest {

        private Object transformedValue;
        private Object value;

        @Test
        public void testApplyTransformation() {
                // Create a sample record with the target field
                Map<String, Object> value = new HashMap<>();
                value.put("firstEventTime", "2021-02-08T08:30:16.775Z");
                Schema valueSchema = Schema.STRING_SCHEMA;
                String topic = "test-topic";
                int partition = 0;

                SourceRecord record = new SourceRecord(
                                null,
                                null,
                                topic,
                                partition,
                                valueSchema,
                                value);

                // Set up the configuration for the transform
                Map<String, String> config = new HashMap<>();
                config.put("field", "firstEventTime");
                config.put("target.type", "TimestampUTC");
                config.put("date.formats", "yyyy-MM-dd'T'HH:mm:ss'Z'");
                // yyyy-MM-dd'T'HH:mm:ss.SSS'Z'
                // yyyy-MM-dd'T'HH:mm:ss.SSSSSSS'Z'
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
                // assertNull(transformedField);

                // assertEquals("2021-02-08 03:30:16.775", transformedField.toString());

                // MongoDB setup
                MongoDatabase database = MongoClients.create("mongodb://localhost:27017").getDatabase("matrix");
                MongoCollection<Document> collection = database.getCollection("backup");
                Document doc = Document.parse("{ \"processedDate\": \"2021-02-08T08:30:16.775Z\" }");
                doc.put("processedTime", transformedField.toString()); // Convert timestamp
                collection.insertOne(doc);

                // 2023-05-15T08:12:51.0
                // Clean up resources
                transform.close();
        }
}
