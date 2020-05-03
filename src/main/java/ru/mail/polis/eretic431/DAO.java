package ru.mail.polis.eretic431;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Iters;
import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DAO implements ru.mail.polis.DAO {
    private final SortedMap<Integer, Table> ssTables;
    private MemoryTable memTable;
    private final File storage;
    private int generation;
    private final long flushThreshold;

    public DAO(final File storage, final long flushThreshold) throws IOException {
        if (storage == null) {
            throw new IllegalArgumentException("Storage must not be null");
        }
        this.storage = storage;
        this.flushThreshold = flushThreshold;
        this.generation = 0;
        this.memTable = new MemoryTable();
        this.ssTables = new TreeMap<>(Comparator.reverseOrder());

        try (final Stream<Path> walker = Files.walk(storage.toPath(), 1)) {
            final List<File> files = walker.filter(path -> {
                final String fileName = path.getFileName().toString();
                return fileName.endsWith(SSTable.DAT) && !fileName.matches("[A-z]+" + SSTable.DAT);
            }).map(Path::toFile).collect(Collectors.toList());

            for (final File file : files) {
                final SSTable sst = new SSTable(file);
                ssTables.put(sst.getGeneration(), sst);

                if (sst.getGeneration() > generation) {
                    generation = sst.getGeneration();
                }
            }
        }

        if (!ssTables.isEmpty()) {
            generation++;
        }
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) {
        final List<Iterator<Row>> iterators = new ArrayList<>(ssTables.size() + 1);
        iterators.add(memTable.iterator(from));
        for (final Table sst : ssTables.values()) {
            iterators.add(sst.iterator(from));
        }
        final Iterator<Row> merged = Iterators.mergeSorted(
                iterators, Comparator.comparing(Row::getKey).thenComparing(Row::getValue));
        final Iterator<Row> collapsed = Iters.collapseEquals(merged, Row::getKey);
        final Iterator<Row> filtered = Iterators.filter(collapsed, e -> {
            assert e != null;
            return !e.getValue().isTombstone();
        });

        return Iterators.transform(filtered, element -> {
            assert element != null;
            assert element.getValue().getData() != null;
            return Record.of(element.getKey(), element.getValue().getData());
        });
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        memTable.upsert(key, value);
        if (memTable.getSize() > flushThreshold) {
            flush();
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        memTable.remove(key);
        if (memTable.getSize() > flushThreshold) {
            flush();
        }
    }

    @Override
    public void close() throws IOException {
        memTable.flush(storage, generation);
    }

    private void flush() throws IOException {
        final File sstFile = memTable.flush(storage, generation);
        ssTables.put(generation, new SSTable(sstFile));
        memTable = new MemoryTable();
        generation++;
    }
}
