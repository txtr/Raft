package io.hamster.storage.journal;

import io.hamster.storage.StorageException;
import io.hamster.storage.journal.index.JournalIndex;
import io.hamster.storage.journal.index.Position;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.NoSuchElementException;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

/**
 * Log segment reader.
 */
public class FileChannelJournalSegmentReader<E> implements JournalReader<E> {

    private final FileChannel channel;
    private final int maxEntrySize;
    private final JournalIndex index;
    private final JournalCodec<E> codec;
    private final ByteBuffer memory;
    private final long firstIndex;
    private Indexed<E> currentEntry;
    private Indexed<E> nextEntry;

    public FileChannelJournalSegmentReader(
            FileChannel channel,
            JournalSegment<E> segment,
            int maxEntrySize,
            JournalIndex index,
            JournalCodec<E> codec) {
        this.channel = channel;
        this.maxEntrySize = maxEntrySize;
        this.index = index;
        this.codec = codec;
        this.memory = ByteBuffer.allocate((maxEntrySize + Integer.BYTES + Integer.BYTES) * 2);
        this.firstIndex = segment.index();
        reset();
    }

    @Override
    public long getFirstIndex() {
        return this.firstIndex;
    }

    @Override
    public long getCurrentIndex() {
        return currentEntry != null ? currentEntry.index() : 0;
    }

    @Override
    public Indexed<E> getCurrentEntry() {
        return currentEntry;
    }

    @Override
    public long getNextIndex() {
        return currentEntry != null ? currentEntry.index() + 1 : firstIndex;
    }

    @Override
    public boolean hasNext() {
        if (nextEntry == null) {
            readNext();
        }
        return nextEntry != null;
    }

    @Override
    public Indexed<E> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        // Set the current entry to the next entry.
        currentEntry = this.nextEntry;

        // Reset the next entry to null.
        nextEntry = null;
        // Read the next entry in the segment.
        readNext();

        // Return the current entry.
        return currentEntry;
    }

    @Override
    public void reset() {
        try {
            channel.position(JournalSegmentDescriptor.BYTES);
        } catch (IOException e) {
            throw new StorageException(e);
        }
        currentEntry = null;
        nextEntry = null;
        memory.clear().limit(0);
        readNext();
    }

    @Override
    public void reset(long index) {
        reset();

        Position position = this.index.lookup(index - 1);
        if (position != null) {
            currentEntry = new Indexed<>(position.index() - 1, null, 0);
            try {
                channel.position(position.position());
                memory.clear().flip();
            } catch (IOException e) {
                throw new StorageException(e);
            }
            readNext();
        }
        while (getNextIndex() < index && hasNext()) {
            next();
        }
    }

    /**
     * Reads the next entry in the segment.
     */
    private void readNext() {
        // Compute the index of the next entry in the segment.
        long nextIndex = getNextIndex();
        try {
            // Read more bytes from the segment if necessary.
            if (memory.remaining() < maxEntrySize) {
                long position = channel.position() + memory.position();
                channel.position(position);
                memory.clear();
                channel.read(memory);
                //reset channel position
                channel.position(position);
                memory.flip();
            }

            memory.mark();
            int length = memory.getInt();

            // If the buffer length is zero then return.
            if (length <= 0 || length > maxEntrySize) {
                memory.reset().limit(memory.position());
                nextEntry = null;
                return;
            }

            // Read the checksum of the entry.
            long checksum = memory.getInt() & 0xFFFFFFFFL;

            // Compute the checksum for the entry bytes.
            final Checksum crc32 = new CRC32();
            crc32.update(memory.array(), memory.position(), length);

            // If the stored checksum equals the computed checksum, return the entry.
            if (checksum == crc32.getValue()) {
                int limit = memory.limit();
                memory.limit(memory.position() + length);
                E entry = codec.decode(memory);
                memory.limit(limit);
                nextEntry = new Indexed<>(nextIndex, entry, length);
            } else {
                memory.reset().limit(memory.position());
                nextEntry = null;
            }
        } catch (BufferUnderflowException e) {
            memory.reset().limit(memory.position());
            nextEntry = null;
        } catch (IOException e) {
            throw new StorageException(e);
        }

    }

    @Override
    public void close() {

    }
}
