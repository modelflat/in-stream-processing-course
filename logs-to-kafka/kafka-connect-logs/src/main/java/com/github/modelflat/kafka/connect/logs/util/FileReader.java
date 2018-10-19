package com.github.modelflat.kafka.connect.logs.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class FileReader implements Iterator<Struct>, Closeable {
    private final FileSystem fs;
    private final Path filePath;
    private long offset;
    private String currentLine;
    private boolean finished = false;
    private LineNumberReader reader;
    private ObjectMapper mapper = new ObjectMapper();

    public FileReader(FileSystem fs, Path filePath) throws IOException {
        if (fs == null || filePath == null) {
            throw new IllegalArgumentException("fileSystem and filePath are required");
        }

        this.fs = fs;
        this.filePath = filePath;

        this.reader = new LineNumberReader(new InputStreamReader(fs.open(filePath)));

        this.offset = 0;
    }

    @Override
    public boolean hasNext() {
        if (currentLine != null) {
            return true;
        } else if (finished) {
            return false;
        } else {
            try {
                String line = reader.readLine();
                offset = reader.getLineNumber();
                if (line == null) {
                    finished = true;
                    return false;
                }
                currentLine = line;
                return true;
            } catch (IOException ioe) {
                throw new IllegalStateException(ioe);
            }
        }
    }

    @Override
    public final Struct next() {
        if (!hasNext()) {
            throw new NoSuchElementException("There are no more records in file: " + filePath);
        }

        String aux = currentLine;
        currentLine = null;

        LogModel logEntry;
        try {
            logEntry = mapper.readValue(aux, LogModel.class);
        } catch (IOException e) {
            logEntry = new LogModel();
        }

        logEntry.raw = aux;

        return logEntry.toStruct();
    }

    public void seek(long offset) {
        if (offset < 0) {
            throw new IllegalArgumentException("Record offset must be greater than 0");
        }

        try {
            if (offset < reader.getLineNumber()) {
                this.reader = new LineNumberReader(new InputStreamReader(fs.open(filePath)));
                currentLine = null;
            }
            while ((currentLine = reader.readLine()) != null) {
                if (reader.getLineNumber() - 1 == offset) {
                    this.offset = reader.getLineNumber();
                    return;
                }
            }
            this.offset = reader.getLineNumber();
        } catch (IOException ioe) {
            throw new ConnectException("Error seeking file " + filePath, ioe);
        }
    }

    public long currentOffset() {
        return offset;
    }

    public void close() throws IOException {
        reader.close();
    }

}
