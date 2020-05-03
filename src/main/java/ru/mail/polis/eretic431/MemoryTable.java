package ru.mail.polis.eretic431;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class MemoryTable implements Table {
    private final SortedMap<ByteBuffer, Value> map = new TreeMap<>();
    private long size = 0;

    @Override
    public Iterator<Row> iterator(@NotNull ByteBuffer from) {
        final Iterator<Map.Entry<ByteBuffer, Value>> iterator = map.tailMap(from).entrySet().iterator();
        return Iterators.transform(iterator, element -> {
            assert element != null;
            return Row.of(element.getKey(), element.getValue());
        });
    }

    @Override
    public void upsert(
            @NotNull ByteBuffer key,
            @NotNull ByteBuffer value) {
        if (map.containsKey(key)) {
            final Value oldValue = map.get(key);
            if (oldValue.getData() != null) {
                size -= oldValue.getData().limit();
            }
        } else {
            size += key.limit();
        }
        size += value.limit();

        map.put(key, Value.of(value));
        key.clear();
        value.clear();
    }

    @Override
    public void remove(@NotNull ByteBuffer key) {
        final Value value = map.get(key);
        if (value != null && value.getData() != null) {
            size -= value.getData().remaining();
        } else {
            size += key.remaining();
        }
        map.put(key, Value.tombstone());
    }

    public File flush(@NotNull final File storage, final int generation) throws IOException {
        if (map.isEmpty()) {
            return null;
        }

        final File sstFile = new File(storage, generation + SSTable.DAT);
        final File tmp = new File(storage, generation + SSTable.TMP);
        tmp.createNewFile();

        final long fileSize = getSize() + map.size() * Long.BYTES * 4 + Long.BYTES;
        final List<Integer> positions = new ArrayList<>(map.size());
        final MappedByteBuffer buf;

        try (final FileChannel fc = FileChannel.open(tmp.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            buf = fc.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
        }

        buf.clear();
        for (final Map.Entry<ByteBuffer, Value> entrySet : map.entrySet()) {
            final ByteBuffer key = entrySet.getKey();
            final Value value = entrySet.getValue();
            positions.add(buf.position());

            buf.putLong(key.remaining());
            buf.put(key);

            buf.putLong(value.getTimestamp());
            if (value.isTombstone()) {
                buf.putLong(-1);
            } else {
                final ByteBuffer data = value.getData();
                assert data != null;
                buf.putLong(data.remaining());
                buf.put(data);
            }
            key.clear();
            if (value.getData() != null) {
                value.getData().clear();
            }
        }

        for (final int position : positions) {
            buf.putLong(position);
        }

        buf.putLong(map.size());
        Files.move(tmp.toPath(), sstFile.toPath(), StandardCopyOption.ATOMIC_MOVE);

        sstFile.setReadOnly();

        return sstFile;
    }

    public long getSize() {
        return size;
    }
}
