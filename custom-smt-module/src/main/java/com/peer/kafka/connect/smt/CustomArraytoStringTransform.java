package com.peer.kafka.connect.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.json.JSONObject;
import org.json.JSONArray;

import java.util.Map;

public class CustomArraytoStringTransform <R extends ConnectRecord<R>> implements Transformation<R> {
    private String subdoc1Name;
    private String subdoc2Name;
    private String array1FieldName;
    private String array2FieldName;

    @Override
    public R apply(R record) {
        String jsonString = record.value().toString();
        JSONObject json = new JSONObject(jsonString);
        //Converting Subdocument Array to string.
        if (json.has(subdoc1Name) && !json.isNull(subdoc1Name)) {
            JSONObject subDoc1= json.getJSONObject(subdoc1Name);
            if(subDoc1.has(array1FieldName))
            {
                JSONArray assignedToRefArray = subDoc1.getJSONArray(array1FieldName);
                String array1String = assignedToRefArray.getString(0);
                json.getJSONObject(subdoc1Name).put(array1FieldName, array1String);
            }
        }

        if (json.has(subdoc2Name) && !json.isNull(subdoc2Name))
        {
            JSONObject subDoc2 = json.getJSONObject(subdoc2Name);

            if (subDoc2.has(array2FieldName)) {
                JSONArray originatorRefArray = subDoc2.getJSONArray(array2FieldName);
                String array2String = originatorRefArray.getString(0);
                json.getJSONObject(subdoc2Name).put(array2FieldName, array2String);
            }
        }
        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                record.valueSchema(),
                json.toString(),
                record.timestamp()
        );
    }

    @Override
    public void configure(Map<String, ?> configs) {
        subdoc1Name = (String) configs.get("subdoc1Name");
        subdoc2Name = (String) configs.get("subdoc2Name");
        array1FieldName = (String) configs.get("array1FieldName");
        array2FieldName = (String) configs.get("array2FieldName");
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef()
                .define("array1FieldName", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "The name of the first array field")
                .define("array2FieldName", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "The name of the second array field")
                .define("subdoc1Name", ConfigDef.Type.STRING,ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.LOW, "The name of the first subdocument")
                .define("subdoc2Name", ConfigDef.Type.STRING,ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.LOW, "The name of the second subdocument");
    }

    @Override
    public void close() {}
}
