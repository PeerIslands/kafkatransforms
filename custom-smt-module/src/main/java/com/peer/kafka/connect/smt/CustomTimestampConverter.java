package com.peer.kafka.connect.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.connector.ConnectRecord;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public class CustomTimestampConverter<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final String TARGET_TYPE = "Timestamp";
    private static final String FIELD_NAME = "timestamp";

    @Override
    public void configure(Map<String, ?> configs) {
        // Configuration, if any
    }

    @Override
    public R apply(R record) {
        Object value = record.value();
        if (value instanceof Struct) {
            Struct struct = (Struct) value;
            if (struct.schema().field(FIELD_NAME) != null) {
                Object fieldValue = struct.get(FIELD_NAME);
                if (fieldValue instanceof String) {
                    String datetimeString = (String) fieldValue;
                    Date convertedDate = convertDatetime(datetimeString);
                    if (convertedDate != null) {
                        struct.put(FIELD_NAME, convertedDate);
                    }
                }
            }
        }
        return record;
    }

    private Date convertDatetime(String datetimeString) {
        SimpleDateFormat[] formats = {
                new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ"),
                new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.'Z'"),
                new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSZ")
        };
        for (SimpleDateFormat format : formats) {
            try {
                return format.parse(datetimeString);
            } catch (ParseException e) {
                // Ignore parsing errors and try the next format
            }
        }
        // If none of the formats match, handle the error or return null
        return null;
    }

    @Override
    public void close() {
        // Clean up resources, if any
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    //@Override
    public R getNewRecord(R record) {
        return record;
    }

    //@Override
    public boolean preserveSchema() {
        return true;
    }
}
