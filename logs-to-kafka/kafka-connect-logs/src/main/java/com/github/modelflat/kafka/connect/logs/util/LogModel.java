package com.github.modelflat.kafka.connect.logs.util;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

public class LogModel {
    @JsonIgnore
    public String raw;

    public String ip = "";
    public String time = "";
    public String action = "";
    public String categoryId = "";

    public static final Schema schema =
            SchemaBuilder.struct().name("LogEntry")
                    .field("raw", Schema.STRING_SCHEMA)
                    .field("ip", Schema.STRING_SCHEMA)
                    .field("time", Schema.STRING_SCHEMA)
                    .field("action", Schema.STRING_SCHEMA)
                    .field("categoryId", Schema.STRING_SCHEMA)
                    .build();

    public Struct toStruct() {
        return new Struct(schema)
                .put("raw", raw)
                .put("ip", ip)
                .put("time", time)
                .put("action", action)
                .put("categoryId", categoryId);
    }

}
