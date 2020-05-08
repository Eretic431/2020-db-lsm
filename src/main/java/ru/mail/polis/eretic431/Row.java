package ru.mail.polis.eretic431;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Comparator;

public class Row {
    public static final Comparator<Row> COMPARATOR = Comparator.comparing(Row::getKey).thenComparing(Row::getValue);

    private final ByteBuffer key;
    private final Value value;

    public Row(final @NotNull ByteBuffer key, final @NotNull Value value) {
        this.key = key;
        this.value = value;
    }

    public static Row of(
            @NotNull final ByteBuffer key,
            @NotNull final Value value) {
        return new Row(key, value);
    }

    public ByteBuffer getKey() {
        return key.asReadOnlyBuffer();
    }

    public Value getValue() {
        return value;
    }
}
