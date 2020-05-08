package ru.mail.polis.eretic431;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class MemoryTable implements Table {
    private final SortedMap<ByteBuffer, Value> map = new TreeMap<>();
    private long size;

    @Override
    public Iterator<Row> iterator(@NotNull final ByteBuffer from) {
        final Iterator<Map.Entry<ByteBuffer, Value>> iterator = map.tailMap(from).entrySet().iterator();
        return Iterators.transform(iterator, element -> {
            assert element != null;
            return Row.of(element.getKey(), element.getValue());
        });
    }

    @Override
    public void upsert(
            @NotNull final ByteBuffer key,
            @NotNull final ByteBuffer value) {
        final Value oldValue = map.put(key, Value.of(value));
        resize(key, oldValue);
        size += value.remaining();
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        final Value value = map.put(key, Value.tombstone());
        resize(key, value);
    }

    public long getSize() {
        return size;
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }

    public int getQuantity() {
        return map.size();
    }

    private void resize(@NotNull final ByteBuffer key, @Nullable final Value value) {
        if (value == null || value.getData() == null) {
            size += key.remaining();
        } else {
            size -= value.getData().remaining();
        }
    }
}
