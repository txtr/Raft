package io.hamster.storage.journal;

import com.google.common.collect.Sets;
import io.hamster.storage.StorageException;
import io.hamster.storage.journal.index.JournalIndex;
import io.hamster.storage.journal.index.SparseJournalIndex;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;

public class JournalSegment<E> implements AutoCloseable {

    private final JournalSegmentFile file;
    private final JournalSegmentDescriptor descriptor;
    private final JournalCodec<E> codec;
    private final JournalIndex index;


    private final MappableJournalSegmentWriter<E> writer;
    private final Set<MappableJournalSegmentReader<E>> readers = Sets.newConcurrentHashSet();
    private final AtomicInteger references = new AtomicInteger();
    private final int maxEntrySize;
    private boolean open = true;

    public JournalSegment(
            JournalSegmentFile file,
            JournalSegmentDescriptor descriptor,
            JournalCodec<E> codec,
            double indexDensity,
            int maxEntrySize) {
        this.file = file;
        this.descriptor = descriptor;
        this.codec = codec;
        this.maxEntrySize = maxEntrySize;
        this.index = new SparseJournalIndex(indexDensity);
        this.writer = new MappableJournalSegmentWriter<>(openChannel(), this, codec, this.index, maxEntrySize);
    }

    @Override
    public void close() {
        writer.close();
        readers.forEach(reader -> reader.close());
        open = false;
    }

    /**
     * Returns the segment writer.
     *
     * @return The segment writer.
     */
    public MappableJournalSegmentWriter<E> writer() {
        checkOpen();
        return writer;
    }

    /**
     * Creates a new segment reader.
     *
     * @return A new segment reader.
     */
    MappableJournalSegmentReader<E> createReader() {
        checkOpen();
        MappableJournalSegmentReader reader = new MappableJournalSegmentReader(openChannel(), this,
                codec, this.index, maxEntrySize);
        MappedByteBuffer buffer = writer.buffer();
        if (buffer != null) {
            //reader.map(buffer);
        }
        readers.add(reader);
        return reader;
    }

    /**
     * Closes a segment reader.
     *
     * @param reader the closed segment reader
     */
    void closeReader(MappableJournalSegmentReader<E> reader) {
        readers.remove(reader);
    }

    /**
     * Acquires a reference to the log segment.
     */
    void acquire() {
        if (references.getAndIncrement() == 0 && open) {
            //map();
        }
    }

    /**
     * Releases a reference to the log segment.
     */
    void release() {
        if (references.decrementAndGet() == 0 && open) {
            //unmap();
        }
    }

    private FileChannel openChannel() {
        try {
            return FileChannel.open(this.file.file().toPath(), StandardOpenOption.CREATE,
                    StandardOpenOption.READ, StandardOpenOption.WRITE);
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }

    /**
     * Returns the segment descriptor.
     *
     * @return The segment descriptor.
     */
    public JournalSegmentDescriptor descriptor() {
        return descriptor;
    }

    /**
     * Returns the segment ID.
     *
     * @return The segment ID.
     */
    public long id() {
        return descriptor.id();
    }

    /**
     * Returns the segment version.
     *
     * @return The segment version.
     */
    public long version() {
        return descriptor.version();
    }

    /**
     * Returns the segment's starting index.
     *
     * @return The segment's starting index.
     */
    public long index() {
        return descriptor.index();
    }

    /**
     * Checks whether the segment is open.
     */
    private void checkOpen() {
        checkState(open, "Segment not open");
    }

    /**
     * Returns the last index in the segment.
     *
     * @return The last index in the segment.
     */
    public long lastIndex() {
        return writer.getLastIndex();
    }

    /**
     * Returns a boolean indicating whether the segment is open.
     *
     * @return indicates whether the segment is open
     */
    public boolean isOpen() {
        return open;
    }

    /**
     * Deletes the segment.
     */
    public void delete(){
        try {
            Files.deleteIfExists(file.file().toPath());
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("id", id())
                .add("version", version())
                .add("index", index())
                .toString();
    }


}
