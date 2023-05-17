package com.peer.kafka.connect.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class CustomDatetimeFormatTransform<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String FIELD_CONFIG = "field";
    public static final String TARGET_TYPE_CONFIG = "targetType";

    private String fieldName;
    private String targetType;
    private DateTimeFormatter dateTimeFormatter;

    @Override
    public R apply(R record) {
        Map<String, Object> value = (Map<String, Object>) record.value();
        Object fieldValue = value.get(fieldName);
        if (fieldValue == null) {
            return record;
        }

        if (!(fieldValue instanceof String)) {
            throw new DataException("Field value is not a string: " + fieldValue);
        }

        String dateTimeString = (String) fieldValue;

        try {
            Object convertedValue;

            switch (targetType.toLowerCase()) {
                case "timestamp":
                    Instant instant = Instant.parse(dateTimeString);
                    //convertedValue = Date.from(instant);
                    convertedValue = instant.toEpochMilli();
                    break;
                // Add more cases for other target types as needed

                default:
                    throw new ConfigException("Unsupported target type: " + targetType);
            }

            value.put(fieldName, convertedValue);
            return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    record.keySchema(),
                    record.key(),
                    record.valueSchema(),
                    value,
                    record.timestamp()
            );
        } catch (DateTimeParseException e) {
            throw new DataException("Error parsing datetime string: " + dateTimeString, e);
        }
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef()
                .define(FIELD_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                        ConfigDef.Importance.HIGH, "Field name to convert")
                .define(TARGET_TYPE_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                        ConfigDef.Importance.HIGH, "Target type to convert to");
    }

    @Override
    public void configure(Map<String, ?> props) {
        SimpleConfig config = new SimpleConfig(config(), props);
        fieldName = config.getString(FIELD_CONFIG);
        targetType = config.getString(TARGET_TYPE_CONFIG);

        dateTimeFormatter = DateTimeFormatter.ISO_INSTANT;
    }

    @Override
    public void close() {
        // No resources to release
    }

    public R newRecord(R record, Object value) {
        return apply(record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                record.valueSchema(),
                value,
                record.timestamp()
        ));
    }

    public boolean preservesHeaders() {
        return true;
    }
}
