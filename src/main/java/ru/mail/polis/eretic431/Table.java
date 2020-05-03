package ru.mail.polis.eretic431;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Iterator;

public interface Table {
    Iterator<Row> iterator(@NotNull ByteBuffer from);

    default void upsert(
            @NotNull final ByteBuffer key,
            @NotNull final ByteBuffer value
    ) {
        throw new UnsupportedOperationException("Immutable object. Method is not supported!");
    }

    default void remove(@NotNull final ByteBuffer key) {
        throw new UnsupportedOperationException("Immutable object. Method is not supported!");
    }
}
