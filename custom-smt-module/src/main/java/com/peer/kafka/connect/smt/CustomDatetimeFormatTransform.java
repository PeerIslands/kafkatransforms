package com.peer.kafka.connect.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class CustomDatetimeFormatTransform<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final String FIELD_NAME_CONFIG = "field.name";
    private static final String TARGET_TYPE_CONFIG = "target.type";
    private static final String DEFAULT_TARGET_TYPE = "timestamp";
    private static final List<String> DEFAULT_DATE_FORMATS = Arrays.asList(
            "yyyy-MM-dd'T'HH:mm:ss'Z'",
            "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
            "yyyy-MM-dd'T'HH:mm:ss.SSSSSSS'Z'"
    );

    private String fieldName;
    private String targetType;
    private List<DateFormat> dateFormats;

    @Override
    public R apply(R record) {
        Object value = record.value();
        if (value instanceof Map) {
            Map<String, Object> valueMap = (Map<String, Object>) value;
            Object fieldValue = valueMap.get(fieldName);
            if (fieldValue instanceof String) {
                String strFieldValue = (String) fieldValue;
                for (DateFormat dateFormat : dateFormats) {
                    try {
                        Date parsedDate = dateFormat.parse(strFieldValue);
                        if (targetType.toLowerCase().equals("timestamp")) {
                            valueMap.put(fieldName, parsedDate.getTime());
                        } else if (targetType.toLowerCase().equals("string")) {
                            valueMap.put(fieldName, parsedDate.toString());
                        }
                        return record;
                    } catch (ParseException e) {
                        // Ignore and try the next format
                    }
                }
                // Failed to parse datetime using all formats
                throw new DataException("Failed to parse datetime field: " + fieldName);
            }
        }
        return record;
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef()
                .define(FIELD_NAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                        "The name of the field containing the datetime value")
                .define(TARGET_TYPE_CONFIG, ConfigDef.Type.STRING, DEFAULT_TARGET_TYPE, ConfigDef.Importance.MEDIUM,
                        "The target type to convert the datetime value to ('timestamp' or 'string')");
    }

    @Override
    public void configure(Map<String, ?> configs) {
        SimpleConfig config = new SimpleConfig(config(), configs);
        fieldName = config.getString(FIELD_NAME_CONFIG);
        targetType = config.getString(TARGET_TYPE_CONFIG);
        dateFormats = new ArrayList<>();
        for (String format : DEFAULT_DATE_FORMATS) {
            dateFormats.add(new SimpleDateFormat(format));
        }
    }

    @Override
    public void close() {
        // No-op
    }

    //@Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        configure(configs);
    }

    //@Override
    public Schema outputSchema(Schema inputSchema) {
        SchemaBuilder builder = SchemaBuilder.type(inputSchema.type());
        builder.optional();
        return builder.build();
    }
}
