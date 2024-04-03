package io.hamster.storage.journal;

import com.google.common.collect.Sets;
import io.hamster.storage.StorageException;
import io.hamster.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

import static com.google.common.base.Preconditions.*;

public class SegmentedJournal<E> implements Journal<E> {

    /**
     * Returns a new Raft log builder.
     *
     * @return A new Raft log builder.
     */
    public static <E> Builder<E> builder() {
        return new Builder<>();
    }

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final String name;
    private final StorageLevel storageLevel;
    private final File directory;
    private final JournalCodec<E> codec;

    private final NavigableMap<Long, JournalSegment<E>> segments = new ConcurrentSkipListMap<>();

    private final int maxEntrySize;
    private final int maxSegmentSize;
    private final double indexDensity;
    private final boolean flushOnCommit;
    private JournalSegment<E> currentSegment;
    private final SegmentedJournalWriter<E> writer;
    private final Collection<SegmentedJournalReader> readers = Sets.newConcurrentHashSet();

    private volatile long commitIndex;
    private volatile boolean open = true;

    public SegmentedJournal(
            String name,
            StorageLevel storageLevel,
            File directory,
            JournalCodec<E> codec,
            double indexDensity,
            boolean flushOnCommit,
            int maxSegmentSize,
            int maxEntrySize) {
        this.name = name;
        this.storageLevel = checkNotNull(storageLevel, "storageLevel cannot be null");
        this.directory = directory;

        this.codec = codec;
        this.indexDensity = indexDensity;
        this.flushOnCommit = flushOnCommit;
        this.maxSegmentSize = maxSegmentSize;
        this.maxEntrySize = maxEntrySize;
        open();
        this.writer = new SegmentedJournalWriter<>(this);
    }

    /**
     * Opens the segments.
     */
    private void open() {
        // Load existing log segments from disk.
        for (JournalSegment<E> segment : loadSegments()) {
            segments.put(segment.descriptor().index(), segment);
        }
        // If a segment doesn't already exist, create an initial segment starting at index 1.
        if (!segments.isEmpty()) {
            currentSegment = segments.lastEntry().getValue();
        } else {
            JournalSegmentDescriptor descriptor = JournalSegmentDescriptor.builder()
                    .withId(1)
                    .withIndex(1)
                    .withMaxSegmentSize(maxSegmentSize)
                    .build();

            currentSegment = createSegment(descriptor);

            segments.put(descriptor.index(), currentSegment);
        }
    }

    private JournalSegment<E> createSegment(JournalSegmentDescriptor descriptor) {
        File segmentFile = JournalSegmentFile.createSegmentFile(name, directory, descriptor.id());

        RandomAccessFile randomFile = null;
        FileChannel channel = null;
        try {
            randomFile = new RandomAccessFile(segmentFile, "rw");
            randomFile.setLength(descriptor.maxSegmentSize());
            channel = randomFile.getChannel();

            ByteBuffer buffer = ByteBuffer.allocate(JournalSegmentDescriptor.BYTES);
            descriptor.copyTo(buffer);
            buffer.flip();
            channel.write(buffer);

        } catch (IOException e) {
            throw new StorageException(e);
        } finally {
            try {
                if (channel != null) {
                    channel.close();
                }

                if (randomFile != null) {
                    randomFile.close();
                }
            } catch (IOException e) {
                //ignore
            }
        }
        JournalSegment<E> segment = newSegment(new JournalSegmentFile(segmentFile), descriptor);
        log.debug("Created segment: {}", segment);
        return segment;
    }

    private JournalSegment<E> newSegment(JournalSegmentFile journalSegmentFile, JournalSegmentDescriptor descriptor) {
        return new JournalSegment<>(journalSegmentFile, descriptor, codec, indexDensity, maxEntrySize);
    }

    /**
     * Commits entries up to the given index.
     *
     * @param index The index up to which to commit entries.
     */
    void setCommitIndex(long index) {
        this.commitIndex = index;
    }

    /**
     * Returns the Raft log commit index.
     *
     * @return The Raft log commit index.
     */
    long getCommitIndex() {
        return commitIndex;
    }

    /**
     * Returns whether {@code flushOnCommit} is enabled for the log.
     *
     * @return Indicates whether {@code flushOnCommit} is enabled for the log.
     */
    boolean isFlushOnCommit() {
        return flushOnCommit;
    }

    @Override
    public SegmentedJournalWriter<E> writer() {
        return writer;
    }

    @Override
    public SegmentedJournalReader<E> openReader(long index) {
        return openReader(index, SegmentedJournalReader.Mode.ALL);
    }

    /**
     * Opens a new Raft log reader with the given reader mode.
     *
     * @param index The index from which to begin reading entries.
     * @param mode  The mode in which to read entries.
     * @return The Raft log reader.
     */
    public SegmentedJournalReader<E> openReader(long index, SegmentedJournalReader.Mode mode) {
        SegmentedJournalReader<E> reader = new SegmentedJournalReader<>(this, index, mode);
        readers.add(reader);
        return reader;
    }

    /**
     * Returns the segment for the given index.
     *
     * @param index The index for which to return the segment.
     * @throws IllegalStateException if the segment manager is not open
     */
    synchronized JournalSegment<E> getSegment(long index) {
        assertOpen();
        if (currentSegment != null && index > currentSegment.index()) {
            return currentSegment;
        }
        // If the index is in another segment, get the entry with the next lowest first index.
        Map.Entry<Long, JournalSegment<E>> segment = segments.floorEntry(index);
        if (segment != null) {
            return segment.getValue();
        }
        return getFirstSegment();
    }

    /**
     * Returns the segment following the segment with the given ID.
     *
     * @param index The segment index with which to look up the next segment.
     * @return The next segment for the given index.
     */
    JournalSegment<E> getNextSegment(long index) {
        Map.Entry<Long, JournalSegment<E>> nextSegment = segments.higherEntry(index);
        return nextSegment != null ? nextSegment.getValue() : null;
    }

    /**
     * Creates and returns the next segment.
     *
     * @return The next segment.
     * @throws IllegalStateException if the segment manager is not open
     */
    synchronized JournalSegment<E> getNextSegment() {
        assertOpen();
        JournalSegment lastSegment = getLastSegment();
        JournalSegmentDescriptor descriptor = JournalSegmentDescriptor.builder()
                .withId(lastSegment != null ? lastSegment.descriptor().id() + 1 : 1)
                .withIndex(currentSegment.lastIndex() + 1)
                .withMaxSegmentSize(maxSegmentSize)
                .build();

        currentSegment = createSegment(descriptor);

        segments.put(descriptor.index(), currentSegment);
        return currentSegment;
    }

    /**
     * Removes a segment.
     *
     * @param segment The segment to remove.
     */
    synchronized void removeSegment(JournalSegment segment) {
        segments.remove(segment.index());
        segment.close();
        segment.delete();
        resetCurrentSegment();
    }

    /**
     * Resets the current segment, creating a new segment if necessary.
     */
    private synchronized void resetCurrentSegment() {
        JournalSegment<E> lastSegment = getLastSegment();
        if (lastSegment != null) {
            currentSegment = lastSegment;
        } else {
            JournalSegmentDescriptor descriptor = JournalSegmentDescriptor.builder()
                    .withId(1)
                    .withIndex(1)
                    .withMaxSegmentSize(maxSegmentSize)
                    .build();

            currentSegment = createSegment(descriptor);
            segments.put(1L, currentSegment);
        }
    }

    /**
     * Returns the first segment in the log.
     *
     * @throws IllegalStateException if the segment manager is not open
     */
    JournalSegment<E> getFirstSegment() {
        assertOpen();
        Map.Entry<Long, JournalSegment<E>> segment = segments.firstEntry();
        return segment != null ? segment.getValue() : null;
    }


    /**
     * Resets journal readers to the given tail.
     *
     * @param index The index at which to reset readers.
     */
    void resetTail(long index) {
        for (SegmentedJournalReader reader : readers) {
            if (reader.getNextIndex() >= index) {
                reader.reset(index);
            }
        }
    }

    /**
     * Compacts the journal up to the given index.
     * <p>
     * The semantics of compaction are not specified by this interface.
     *
     * @param index The index up to which to compact the journal.
     */
    public void compact(long index) {
        Map.Entry<Long, JournalSegment<E>> segmentEntry = segments.floorEntry(index);
        if (segmentEntry != null) {
            SortedMap<Long, JournalSegment<E>> compactSegments = segments.headMap(segmentEntry.getValue().index());
            if (!compactSegments.isEmpty()) {
                log.debug("{} - Compacting {} segment(s)", name, compactSegments.size());
                for (JournalSegment<E> segment : compactSegments.values()) {
                    segment.close();
                    segment.delete();
                }
            }
            compactSegments.clear();
            resetHead(segmentEntry.getValue().index());
        }
    }

    /**
     * Resets journal readers to the given head.
     *
     * @param index The index at which to reset readers.
     */
    void resetHead(long index) {
        for (SegmentedJournalReader reader : readers) {
            if (reader.getNextIndex() < index) {
                reader.reset(index);
            }
        }
    }

    void closeReader(SegmentedJournalReader reader) {
        readers.remove(reader);
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    /**
     * Returns the last segment in the log.
     *
     * @throws IllegalStateException if the segment manager is not open
     */
    JournalSegment<E> getLastSegment() {
        assertOpen();
        return segments.lastEntry().getValue();
    }

    /**
     * Resets and returns the first segment in the journal.
     *
     * @param index the starting index of the journal
     * @return the first segment
     */
    JournalSegment<E> resetSegments(long index) {
        assertOpen();

        // If the index already equals the first segment index, skip the reset.
        JournalSegment<E> firstSegment = getFirstSegment();
        if (index == firstSegment.index()) {
            return firstSegment;
        }

        for (JournalSegment<E> segment : segments.values()) {
            segment.close();
            segment.delete();
        }

        segments.clear();

        JournalSegmentDescriptor descriptor = JournalSegmentDescriptor.builder()
                .withId(1)
                .withIndex(index)
                .withMaxSegmentSize(maxSegmentSize)
                .build();
        currentSegment = createSegment(descriptor);
        segments.put(index, currentSegment);
        return currentSegment;
    }

    /**
     * Asserts that the manager is open.
     *
     * @throws IllegalStateException if the segment manager is not open
     */
    private void assertOpen() {
        checkState(currentSegment != null, "journal not open");
    }


    /**
     * Loads all segments from disk.
     *
     * @return A collection of segments for the log.
     */
    protected Collection<JournalSegment<E>> loadSegments() {
        // Ensure log directories are created.
        directory.mkdirs();

        TreeMap<Long, JournalSegment<E>> segments = new TreeMap<>();
        for (File file : directory.listFiles(File::isFile)) {

            // If the file looks like a segment file, attempt to load the segment.
            if (JournalSegmentFile.isSegmentFile(name, file)) {
                JournalSegmentFile segmentFile = new JournalSegmentFile(file);
                ByteBuffer buffer = ByteBuffer.allocate(JournalSegmentDescriptor.BYTES);
                try (FileChannel channel = openChannel(file)) {
                    channel.read(buffer);
                    buffer.flip();
                } catch (IOException e) {
                    throw new StorageException(e);
                }
                JournalSegmentDescriptor descriptor = new JournalSegmentDescriptor(buffer);
                JournalSegment<E> segment = newSegment(new JournalSegmentFile(file), descriptor);
                // Add the segment to the segments list.
                log.debug("Found segment: {} ({})", segment.descriptor().id(), segmentFile.file().getName());
                segments.put(descriptor.index(), segment);
            }
        }
        return segments.values();
    }

    private FileChannel openChannel(File file) {
        try {
            return FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void close() {
        segments.values().forEach(segment -> {
            log.debug("Closing segment: {}", segment);
            segment.close();
        });
        currentSegment = null;
        open = false;
    }

    public static class Builder<E> implements io.hamster.utils.Builder<SegmentedJournal<E>> {

        private static final boolean DEFAULT_FLUSH_ON_COMMIT = false;
        private static final String DEFAULT_NAME = "hamster";
        private static final String DEFAULT_DIRECTORY = System.getProperty("user.dir");
        private static final int DEFAULT_MAX_SEGMENT_SIZE = 1024 * 1024 * 32;
        private static final int DEFAULT_MAX_ENTRY_SIZE = 1024 * 1024;
        private static final double DEFAULT_INDEX_DENSITY = .005;

        private String name = DEFAULT_NAME;
        private StorageLevel storageLevel = StorageLevel.DISK;
        private File directory = new File(DEFAULT_DIRECTORY);
        private JournalCodec<E> codec;
        private double indexDensity = DEFAULT_INDEX_DENSITY;
        private int maxEntrySize = DEFAULT_MAX_ENTRY_SIZE;
        private int maxSegmentSize = DEFAULT_MAX_SEGMENT_SIZE;
        private boolean flushOnCommit = DEFAULT_FLUSH_ON_COMMIT;

        protected Builder() {

        }

        /**
         * Sets the storage name.
         *
         * @param name The storage name.
         * @return The storage builder.
         */
        public Builder<E> withName(String name) {
            this.name = checkNotNull(name, "name cannot be null");
            return this;
        }

        /**
         * Sets the log storage level, returning the builder for method chaining.
         * <p>
         * The storage level indicates how individual entries should be persisted in the journal.
         *
         * @param storageLevel The log storage level.
         * @return The storage builder.
         */
        public Builder<E> withStorageLevel(StorageLevel storageLevel) {
            this.storageLevel = checkNotNull(storageLevel, "storageLevel cannot be null");
            return this;
        }

        /**
         * Sets the log directory, returning the builder for method chaining.
         * <p>
         * The log will write segment files into the provided directory.
         *
         * @param directory The log directory.
         * @return The storage builder.
         * @throws NullPointerException If the {@code directory} is {@code null}
         */
        public Builder<E> withDirectory(String directory) {
            return withDirectory(new File(checkNotNull(directory, "directory cannot be null")));
        }

        /**
         * Sets the log directory, returning the builder for method chaining.
         * <p>
         * The log will write segment files into the provided directory.
         *
         * @param directory The log directory.
         * @return The storage builder.
         * @throws NullPointerException If the {@code directory} is {@code null}
         */
        public Builder<E> withDirectory(File directory) {
            this.directory = checkNotNull(directory, "directory cannot be null");
            return this;
        }

        /**
         * Sets the journal codec, returning the builder for method chaining.
         *
         * @param codec The journal codec.
         * @return The journal builder.
         */
        public Builder<E> withCodec(JournalCodec<E> codec) {
            this.codec = checkNotNull(codec, "codec cannot be null");
            return this;
        }

        /**
         * Sets the maximum segment size in bytes, returning the builder for method chaining.
         * <p>
         * The maximum segment size dictates when logs should roll over to new segments. As entries are written to a segment
         * of the log, once the size of the segment surpasses the configured maximum segment size, the log will create a new
         * segment and append new entries to that segment.
         * <p>
         * By default, the maximum segment size is {@code 1024 * 1024 * 32}.
         *
         * @param maxSegmentSize The maximum segment size in bytes.
         * @return The storage builder.
         * @throws IllegalArgumentException If the {@code maxSegmentSize} is not positive
         */
        public Builder<E> withMaxSegmentSize(int maxSegmentSize) {
            checkArgument(maxSegmentSize > JournalSegmentDescriptor.BYTES, "maxSegmentSize must be greater than " + JournalSegmentDescriptor.BYTES);
            this.maxSegmentSize = maxSegmentSize;
            return this;
        }

        /**
         * Sets the maximum entry size in bytes, returning the builder for method chaining.
         *
         * @param maxEntrySize the maximum entry size in bytes
         * @return the storage builder
         * @throws IllegalArgumentException if the {@code maxEntrySize} is not positive
         */
        public Builder<E> withMaxEntrySize(int maxEntrySize) {
            checkArgument(maxEntrySize > 0, "maxEntrySize must be positive");
            this.maxEntrySize = maxEntrySize;
            return this;
        }

        /**
         * Sets the journal index density.
         * <p>
         * The index density is the frequency at which the position of entries written to the journal will be recorded in an
         * in-memory index for faster seeking.
         *
         * @param indexDensity the index density
         * @return the journal builder
         * @throws IllegalArgumentException if the density is not between 0 and 1
         */
        public Builder<E> withIndexDensity(double indexDensity) {
            checkArgument(indexDensity > 0 && indexDensity < 1, "index density must be between 0 and 1");
            this.indexDensity = indexDensity;
            return this;
        }

        /**
         * Enables flushing buffers to disk when entries are committed to a segment, returning the builder for method
         * chaining.
         * <p>
         * When flush-on-commit is enabled, log entry buffers will be automatically flushed to disk each time an entry is
         * committed in a given segment.
         *
         * @return The storage builder.
         */
        public Builder<E> withFlushOnCommit() {
            return withFlushOnCommit(true);
        }

        /**
         * Sets whether to flush buffers to disk when entries are committed to a segment, returning the builder for method
         * chaining.
         * <p>
         * When flush-on-commit is enabled, log entry buffers will be automatically flushed to disk each time an entry is
         * committed in a given segment.
         *
         * @param flushOnCommit Whether to flush buffers to disk when entries are committed to a segment.
         * @return The storage builder.
         */
        public Builder<E> withFlushOnCommit(boolean flushOnCommit) {
            this.flushOnCommit = flushOnCommit;
            return this;
        }


        @Override
        public SegmentedJournal<E> build() {
            return new SegmentedJournal<>(name,
                    storageLevel,
                    directory,
                    codec,
                    indexDensity,
                    flushOnCommit,
                    maxSegmentSize,
                    maxEntrySize
            );
        }
    }

}
