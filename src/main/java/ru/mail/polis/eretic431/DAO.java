package ru.mail.polis.eretic431;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Record;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Objects;

public class DAO implements ru.mail.polis.DAO {
    private final SortedMap<ByteBuffer, ByteBuffer> map = new TreeMap<>();

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) {
        final Iterator<Map.Entry<ByteBuffer, ByteBuffer>> iterator = map.tailMap(from).entrySet().iterator();
        return Iterators.transform(iterator,
                element -> Record.of(Objects.requireNonNull(element).getKey(), element.getValue()));
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        map.put(key, value);
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        map.remove(key);
    }

    @Override
    public void close() {
        map.clear();
    }
}
