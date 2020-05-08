package ru.mail.polis.eretic431;

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

final class SSTable implements Table {
    public static final String DAT = ".dat";
    public static final String TMP = ".tmp";

    final File file;
    private final MappedByteBuffer memMap;
    private final int generation;
    private final long indexBytes;
    private final int quantity;

    /**
     * Flushes memory table.
     *
     * @param memTable   is a flushing {@link MemoryTable}
     * @param storage    where file flushed to
     * @param generation of memory table
     * @return Flushed file
     * @throws IOException when {@link FileChannel} opening goes wrong
     */
    public static SSTable flush(
            @NotNull final MemoryTable memTable,
            @NotNull final File storage,
            final int generation) throws IOException {
        if (memTable.isEmpty()) {
            return null;
        }

        final File sstFile = new File(storage, generation + SSTable.DAT);
        final File tmp = new File(storage, generation + SSTable.TMP);
        tmp.createNewFile();

        final long fileSize = memTable.getSize() + memTable.getQuantity() * Long.BYTES * 4 + Long.BYTES;
        final List<Integer> positions = new ArrayList<>(memTable.getQuantity());
        final MappedByteBuffer buf;

        try (FileChannel fc = FileChannel.open(tmp.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            buf = fc.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
        }

        memTable.iterator(ByteBuffer.allocate(0)).forEachRemaining(row -> {
            final ByteBuffer key = row.getKey();
            final Value value = row.getValue();
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
        });

        for (final int position : positions) {
            buf.putLong(position);
        }

        buf.putLong(memTable.getQuantity());
        Files.move(tmp.toPath(), sstFile.toPath(), StandardCopyOption.ATOMIC_MOVE);

        sstFile.setReadOnly();

        return new SSTable(sstFile);
    }

    public SSTable(@NotNull final File file) throws IOException {
        this.file = file;
        final String name = file.getName();
        generation = Integer.parseInt(name.substring(0, name.length() - DAT.length()));

        try (FileChannel fc = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
            memMap = fc.map(FileChannel.MapMode.READ_ONLY, 0, file.length());
        }

        quantity = (int) memMap.getLong(memMap.limit() - Long.BYTES);
        indexBytes = memMap.remaining() - (long) Long.BYTES * (quantity + 1);
    }

    @Override
    public Iterator<Row> iterator(@NotNull final ByteBuffer from) {
        return new Iterator<>() {
            private int position = binarySearch(from);

            @Override
            public boolean hasNext() {
                return position < quantity;
            }

            @Override
            public Row next() {
                if (!hasNext()) {
                    throw new IllegalStateException("Iterator is empty!");
                }
                return getRow(position++);
            }
        };
    }

    @Override
    public void upsert(
            @NotNull final ByteBuffer key,
            @NotNull final ByteBuffer value) {
        throw new UnsupportedOperationException("Method is not supported!");
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        throw new UnsupportedOperationException("Method is not supported!");
    }

    public int getGeneration() {
        return generation;
    }

    private int binarySearch(@NotNull final ByteBuffer from) {
        int low = 0;
        int high = quantity - 1;

        while (low <= high) {
            final int pivot = (low + high) >>> 1;
            final ByteBuffer pivotVal = getKey(pivot);
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

    private ByteBuffer getKey(final long index) {
        memMap.clear();
        memMap.position((int) (indexBytes + index * Long.BYTES));
        memMap.position((int) memMap.getLong());
        final long keyLength = memMap.getLong();
        memMap.limit((int) (memMap.position() + keyLength));

        return memMap.slice().asReadOnlyBuffer();
    }

    private Row getRow(final long index) {
        final ByteBuffer key = getKey(index);
        memMap.clear();
        memMap.position((int) (indexBytes + index * Long.BYTES));
        memMap.position((int) memMap.getLong() + Long.BYTES + key.remaining());
        final long timestamp = memMap.getLong();
        final long valueLength = memMap.getLong();
        if (valueLength < 0) {
            return Row.of(key, Value.tombstone(timestamp));
        }
        memMap.limit((int) (memMap.position() + valueLength));

        return Row.of(key, Value.of(timestamp, memMap.slice().asReadOnlyBuffer()));
    }
}
