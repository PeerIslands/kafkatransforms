package com.peer.kafka.connect.smt;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public class CustomTimestampConverter<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final String FIELD_CONFIG = "field";
    private static final String TARGET_TYPE_CONFIG = "target.type";
    private static final String DATE_FORMATS_CONFIG = "date.formats";

    private String fieldName;
    private String targetType;
    private String[] dateFormats;

//    @Override
//    public void configure(Map<String, ?> configs) {
//        final SimpleConfig config = new SimpleConfig(ConfigDef(), configs);
//        fieldName = config.getString(FIELD_CONFIG);
//        targetType = config.getString(TARGET_TYPE_CONFIG);
//        dateFormats = config.getString(DATE_FORMATS_CONFIG).split(",");
//    }

    @Override
    public void configure(Map<String, ?> configs) {
        fieldName = (String) configs.get("field");
        targetType = (String) configs.get("target.type");
        dateFormats = ((String) configs.get("date.formats")).split(",");
    }

    @Override
    public R apply(R record) {
        Object value = record.value();

        if (value != null && value instanceof Map) {
            Map<String, Object> valueMap = (Map<String, Object>) value;
            Object fieldValue = valueMap.get(fieldName);

            if (fieldValue != null && fieldValue instanceof String) {
                String dateString = (String) fieldValue;
                for (String dateFormat : dateFormats) {
                    try {

                        Object transformedValue = convertToTargetType(dateString, targetType, dateFormat);

                        valueMap.put(fieldName, transformedValue);
                        break;
                    } catch (Exception e) {

                    }
                }
            }
        }

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                record.valueSchema(),
                record.value(),
                record.timestamp(),
                record.headers()
        );
    }

    private Object convertToTargetType(String dateString, String targetType, String dateFormat) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
        Date date = sdf.parse(dateString);

        switch (targetType) {
            case "Timestamp":
                return new java.sql.Timestamp(date.getTime());
            case "Date":
                return date;
            case "Unix":
                return date.getTime();
            default:
                throw new IllegalArgumentException("Unsupported target type: " + targetType);
        }
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef()
                .define(FIELD_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "Field name to transform")
                .define(TARGET_TYPE_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "Target type for conversion")
                .define(DATE_FORMATS_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "Comma-separated date formats");
    }

    @Override
    public void close() {
        // Close any resources if needed
    }
}
