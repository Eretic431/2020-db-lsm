package ru.mail.polis.eretic431;

import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

final class Value implements Comparable<Value> {
    final private long timestamp;
    @Nullable
    private final ByteBuffer data;

    private Value(@Nullable ByteBuffer data) {
        this(System.currentTimeMillis(), data);
    }

    private Value(long timestamp, @Nullable ByteBuffer data) {
        this.timestamp = timestamp;
        this.data = data;
    }

    @NotNull
    public static Value of(@NotNull ByteBuffer data) {
        return new Value(data);
    }

    @NotNull
    public static Value of(final long timestamp, @NotNull ByteBuffer data) {
        return new Value(timestamp, data);
    }

    @NotNull
    public static Value tombstone() {
        return new Value(null);
    }

    @NotNull
    public static Value tombstone(final long timestamp) {
        return new Value(timestamp, null);
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
