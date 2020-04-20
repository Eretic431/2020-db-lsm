package ru.mail.polis.Eretic431;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class DAO implements ru.mail.polis.DAO {
    private final SortedMap<ByteBuffer, ByteBuffer> map = new TreeMap<>();

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull ByteBuffer from) throws IOException {
        Iterator<Map.Entry<ByteBuffer, ByteBuffer>> iterator = map.tailMap(from).entrySet().iterator();
        return Iterators.transform(iterator,
                element -> Record.of(Objects.requireNonNull(element).getKey(), element.getValue()));
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException {
        map.put(key, value);
    }

    @Override
    public void remove(@NotNull ByteBuffer key) throws IOException {
        map.remove(key);
    }


    @Override
    public void close() throws IOException {
        map.clear();
    }
}
