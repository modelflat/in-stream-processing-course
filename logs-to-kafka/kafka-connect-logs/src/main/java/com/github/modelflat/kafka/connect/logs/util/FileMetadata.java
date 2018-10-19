package com.github.modelflat.kafka.connect.logs.util;

public class FileMetadata {
    private String path;
    private long length;

    FileMetadata(String path, long length) {
        this.path = path;
        this.length = length;
    }

    public String getPath() {
        return path;
    }

    public long getLen() {
        return length;
    }

    @Override
    public String toString() {
        return String.format("[path = %s, length = %s]", path, length);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (!(object instanceof FileMetadata)) return false;

        FileMetadata metadata = (FileMetadata) object;
        return this.path.equals(metadata.getPath()) && this.length == metadata.length;
    }

    @Override
    public int hashCode() {
        return path == null ? 0 : path.hashCode();
    }

}