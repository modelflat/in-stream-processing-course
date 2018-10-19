package com.github.modelflat.kafka.connect.logs.util;

import org.apache.hadoop.fs.FileSystem;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Util {

    public static String getVersion() {
        return "0.1";
    }

    public static SourceRecord toSourceRecord(String topic, Struct struct, FileMetadata metadata, long offset) {
        return new SourceRecord(
                new HashMap<String, Object>() {{
                    put("path", metadata.getPath());
                }},
                Collections.singletonMap("offset", offset),
                topic, struct.schema(), struct
        );
    }

    public static Stream<FileMetadata> streamFiles(FileSystem fileSystem, Pattern fileRegex) throws IOException {
        return asStream(new FileIterator(fileSystem, fileRegex));
    }

    private static <T> Stream<T> asStream(Iterator<T> src) {
        Iterable<T> iterable = () -> src;
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    public static void logConfig(Logger log, String name, Map<String, Object> conf) {
        StringBuilder b = new StringBuilder();
        b.append(name);
        b.append(" values: ");
        b.append(Utils.NL);
        for (Map.Entry<String, Object> entry : conf.entrySet()) {
            b.append('\t');
            b.append(entry.getKey());
            b.append(" = ");
            b.append(entry.getValue());
            b.append(Utils.NL);
        }

        log.info(b.toString());
    }

}