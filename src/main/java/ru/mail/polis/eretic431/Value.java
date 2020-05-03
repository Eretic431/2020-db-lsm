package ru.mail.polis.eretic431;

import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

final class Value implements Comparable<Value> {
    final private long timestamp;
    @Nullable
    private final ByteBuffer data;

    Value(@Nullable ByteBuffer data) {
        this.timestamp = System.currentTimeMillis();
        this.data = data;
    }

    Value(long timestamp,
          @Nullable ByteBuffer data) {
        this.timestamp = timestamp;
        this.data = data;
    }

    public boolean isTombstone() {
        return data == null;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Nullable
    public ByteBuffer getData() {
        return data;
    }

    @Override
    public int compareTo(@NotNull Value value) {
        return -Long.compare(timestamp, value.getTimestamp());
    }
}
