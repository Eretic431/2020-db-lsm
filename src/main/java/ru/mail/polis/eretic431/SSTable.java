package ru.mail.polis.eretic431;

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

final class SSTable implements Table {
    final File file;
    private final int generation;
    private final List<Long> keys;
    private final MappedByteBuffer memMap;

    public SSTable(File file) throws IOException {
        this.file = file;
        final String name = file.getName();
        generation = Integer.parseInt(name.substring(0, name.length() - 4));

        try (final FileChannel fc = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
            memMap = fc.map(FileChannel.MapMode.READ_ONLY, 0, file.length());
        }

        final int rowsAmount = (int) memMap.getLong(memMap.limit() - Long.BYTES);
        keys = new ArrayList<>(rowsAmount);

        memMap.position(memMap.limit() - Long.BYTES * (rowsAmount + 1));
        for (int i = 0; i < rowsAmount; i++) {
            keys.add(memMap.getLong());
        }
    }

    @Override
    public Iterator<Row> iterator(@NotNull ByteBuffer from) {
        return new Iterator<>() {
            private int position = binarySearch(from);

            @Override
            public boolean hasNext() {
                return position < keys.size();
            }

            @Override
            public Row next() {
                if (!hasNext()) {
                    throw new IllegalStateException("Iterator is empty!");
                }
                return getRow(keys.get(position++));
            }
        };
    }

    public int getGeneration() {
        return generation;
    }

    private int binarySearch(@NotNull final ByteBuffer from) {
        int low = 0;
        int high = keys.size() - 1;

        while (low <= high) {
            final int pivot = (low + high) >>> 1;
            ByteBuffer pivotVal = getKey(keys.get(pivot));
            if (pivotVal.compareTo(from) < 0) {
                low = pivot + 1;
            } else {
                if (pivotVal.compareTo(from) <= 0) {
                    return pivot;
                }

                high = pivot - 1;
            }
        }

        return low;
    }

    private ByteBuffer getKey(long position) {
        memMap.clear();
        memMap.position((int) position);
        final long keyLength = memMap.getLong();
        memMap.limit((int) (memMap.position() + keyLength));

        return memMap.slice();
    }

    private Row getRow(long position) {
        final ByteBuffer key = getKey(position);
        memMap.clear();
        memMap.position((int) position + key.limit() + Long.BYTES);
        final long timestamp = memMap.getLong();
        final long valueLength = memMap.getLong();
        if (valueLength < 0) {
            return Row.of(key, new Value(timestamp, null));
        }
        memMap.limit((int) (memMap.position() + valueLength));

        return Row.of(key, new Value(timestamp, memMap.slice()));
    }
}
