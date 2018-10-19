package com.github.modelflat.kafka.connect.logs;

import com.github.modelflat.kafka.connect.logs.util.FileMetadata;
import com.github.modelflat.kafka.connect.logs.util.FileReader;
import com.github.modelflat.kafka.connect.logs.util.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class LogsSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(LogsSourceTask.class);

    private LogsSourceConnectorConfig config;

    private Pattern fileRegex;
    private FileSystem fileSystem;

    private long sleep;

    private AtomicBoolean stop;
    private AtomicInteger executions = new AtomicInteger(0);

    @Override
    public String version() {
        return Util.getVersion();
    }

    @Override
    public void start(Map<String, String> properties) {
        try {
            this.config = new LogsSourceConnectorConfig(properties);
            Util.logConfig(log, getClass().getSimpleName(), config.originals());

            this.fileRegex = Pattern.compile(config.getRegex());
            this.sleep = config.getSleep();

            Path workingDir = new Path(config.getFsUri());
            fileSystem = FileSystem.newInstance(workingDir.toUri(), new Configuration());
            fileSystem.setWorkingDirectory(workingDir);
        } catch (ConfigException ce) {
            log.error("Couldn't start LogsSourceTask:", ce);
            throw new ConnectException("Couldn't start LogsSourceTask due to configuration error", ce);
        } catch (Throwable t) {
            log.error("Couldn't start LogsSourceConnector:", t);
            throw new ConnectException("A problem has occurred reading configuration:" + t.getMessage());
        }


        stop = new AtomicBoolean(false);

        log.info("Initialization complete!");
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {

        while (stop != null && !stop.get()) {
            log.info("Polling for new data");

            sleepIfNeeded();

            try {
                List<FileMetadata> files = Util
                        .streamFiles(fileSystem, fileRegex)
                        .filter(metadata -> metadata.getLen() > 0)
                        .collect(Collectors.toList());

                final List<SourceRecord> results = new ArrayList<>();

                files.forEach(metadata -> {
                    try (FileReader reader = readerForFile(metadata)) {
                        log.info("Processing records for file {}", metadata);

                        while (reader.hasNext()) {
                            results.add(Util.toSourceRecord(
                                    config.getTopic(), reader.next(), metadata, reader.currentOffset()
                            ));
                        }
                    } catch (ConnectException | IOException e) {
                        log.error("Error reading file from FS: {}. Keep going...", metadata.getPath(), e);
                    }
                });

                if (results.isEmpty()) {
                    continue;
                }

                return results;
            } catch (IOException e) {
                log.error("IOException while streaming files from " + fileSystem.getCanonicalServiceName(), e);
            }
        }

        return Collections.emptyList();
    }

    @Override
    public void stop() {
        log.info("Stopping connector...");

        if (stop != null) {
            stop.set(true);
        }

        try {
            fileSystem.close();
        } catch (IOException e) {
            log.warn("Interruption exception: ", e);
        }
    }

    private FileReader readerForFile(FileMetadata metadata) {
        Map<String, Object> partition = new HashMap<String, Object>() {{
            put("path", metadata.getPath());
        }};

        FileSystem current = metadata.getPath().startsWith(
                fileSystem.getWorkingDirectory().toString()
        ) ? fileSystem : null;

        FileReader reader;
        try {
            reader = new FileReader(current, new Path(metadata.getPath()));
        } catch (Throwable t) {
            throw new ConnectException("An error has occurred when creating reader for file: " + metadata.getPath(), t);
        }

        Map<String, Object> offset = context.offsetStorageReader().offset(partition);

        if (offset != null && offset.get("offset") != null) {
            reader.seek((long) offset.get("offset"));
        }

        return reader;
    }

    private void sleepIfNeeded() {
        if (executions.get() > 0) {
            try {
                Thread.sleep(sleep);
            } catch (InterruptedException ie) {
                log.warn("An interrupted exception has occurred.", ie);
            }
        }

        executions.incrementAndGet();
    }

}