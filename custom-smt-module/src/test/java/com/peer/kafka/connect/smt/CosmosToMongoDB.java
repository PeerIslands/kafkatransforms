
package com.peer.kafka.connect.smt;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

public class CosmosToMongoDB {
    public static void main(String[] args) {
        // Configure Kafka Consumer

        // MongoDB setup
        MongoDatabase database = MongoClients.create("mongodb://localhost:27017").getDatabase("matrix");
        MongoCollection<Document> collection = database.getCollection("backup");

        // Subscribe to the Kafka topic

        String cosmosTimestamp = extractTimestamp(); // Implement this method based on your
                                                     // message format
        Document doc = Document.parse("{ \"processedDate\": \"2021-06-08T09:16:40.8133732Z\" }");
        doc.put("processedTime", convertTimestamp(cosmosTimestamp)); // Convert timestamp
        collection.insertOne(doc);

    }

    private static String extractTimestamp() {
        // Extract and return the timestamp from the message. Placeholder
        // implementation.
        return "2021-06-08T09:16:40.8133732Z"; // This should be replaced with actual extraction logic
    }

    private static java.util.Date convertTimestamp(String cosmosTimestamp) {
        try {
            // Parse the string to an Instant
            java.time.Instant instant = java.time.Instant.parse(cosmosTimestamp);
            // Convert the Instant to a Date
            return java.util.Date.from(instant);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
