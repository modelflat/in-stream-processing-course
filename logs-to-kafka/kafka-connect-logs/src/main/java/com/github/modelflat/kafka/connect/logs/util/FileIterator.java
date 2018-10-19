package com.github.modelflat.kafka.connect.logs.util;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.kafka.connect.errors.ConnectException;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.regex.Pattern;

public class FileIterator implements Iterator<FileMetadata> {
    private RemoteIterator<LocatedFileStatus> it;
    private Pattern fileRegex;
    private LocatedFileStatus current = null;

    FileIterator(FileSystem fs, Pattern fileRegex) throws IOException {
        this.it = fs.listFiles(fs.getWorkingDirectory(), false);
        this.fileRegex = fileRegex;
    }

    @Override
    public boolean hasNext() {
        try {
            if (current == null) {
                current = advanceTillNextGood();
            }
            return current != null;
        } catch (IOException ioe) {
            throw new ConnectException(ioe);
        }
    }

    @Override
    public FileMetadata next() {
        if (current == null && !hasNext()) {
            throw new NoSuchElementException("There are no more items");
        }

        FileMetadata metadata = new FileMetadata(current.getPath().toString(), current.getLen());
        current = null;
        return metadata;
    }

    private boolean isGood(LocatedFileStatus file) {
        return file != null && file.isFile() && fileRegex.matcher(file.getPath().getName()).find();
    }

    private LocatedFileStatus advanceTillNextGood() throws IOException {
        LocatedFileStatus file;
        while (it.hasNext()) {
            file = it.next();
            if (isGood(file)) {
                return file;
            }
        }
        return null;
    }

};