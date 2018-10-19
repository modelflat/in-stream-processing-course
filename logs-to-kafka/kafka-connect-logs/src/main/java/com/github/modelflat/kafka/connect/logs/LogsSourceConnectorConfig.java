package com.github.modelflat.kafka.connect.logs;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;


class LogsSourceConnectorConfig extends AbstractConfig {

    private static final String FS_URI = "uri";
    private static final String FS_URI_DOC = "FS uri to track files under";

    private static final String TOPIC = "topic";
    private static final String TOPIC_DOC = "Topic to copy data to.";

    private static final String SLEEP_MS = "sleep";
    private static final String SLEEP_MS_DOC = "Time in ms to sleep between polls";

    private static final String REGEX = "regex";
    private static final String REGEX_DOC = "Regular expression to filter files from the FS.";

    private LogsSourceConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    LogsSourceConnectorConfig(Map<String, String> parsedConfig) {
        this(conf(), parsedConfig);
    }

    static ConfigDef conf() {
        return new ConfigDef()
                .define(FS_URI, Type.STRING, Importance.HIGH, FS_URI_DOC)
                .define(TOPIC, Type.STRING, Importance.HIGH, TOPIC_DOC)
                .define(SLEEP_MS, Type.LONG, Importance.HIGH, SLEEP_MS_DOC)
                .define(REGEX, Type.STRING, Importance.HIGH, REGEX_DOC);
    }

    String getFsUri() {
        return this.getString(FS_URI);
    }

    String getTopic() {
        return this.getString(TOPIC);
    }

    String getRegex() {
        return this.getString(REGEX);
    }

    long getSleep() {
        return this.getLong(SLEEP_MS);
    }

}