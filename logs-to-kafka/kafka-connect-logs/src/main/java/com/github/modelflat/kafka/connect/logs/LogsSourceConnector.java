package com.github.modelflat.kafka.connect.logs;

import com.github.modelflat.kafka.connect.logs.util.Util;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LogsSourceConnector extends SourceConnector {

    private static Logger log = LoggerFactory.getLogger(LogsSourceConnector.class);

    private LogsSourceConnectorConfig config;

    @Override
    public String version() {
        return Util.getVersion();
    }

    @Override
    public void start(Map<String, String> properties) {
        log.info("Starting LogsSourceConnector...");

        try {
            config = new LogsSourceConnectorConfig(properties);
        } catch (ConfigException ce) {
            log.error("Couldn't start LogsSourceConnector:", ce);
            throw new ConnectException("Couldn't start LogsSourceConnector due to configuration error.", ce);
        } catch (Exception ce) {
            log.error("Couldn't start LogsSourceConnector:", ce);
            throw new ConnectException("An error has occurred when starting LogsSourceConnector" + ce);
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return LogsSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (config == null) {
            throw new ConnectException("Connector config has not been initialized");
        }
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        for (int i = 0; i < maxTasks; ++i) { // do not support multitasking
            taskConfigs.add(config.originalsStrings());
        }
        return taskConfigs;
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return LogsSourceConnectorConfig.conf();
    }
}
