package io.hamster.storage.journal;

import io.hamster.storage.StorageException;
import io.hamster.storage.journal.index.JournalIndex;

import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * Mappable log segment reader.
 */
class MappableJournalSegmentReader<E> implements JournalReader<E> {

    private final JournalSegment<E> segment;
    private final FileChannel channel;
    private final int maxEntrySize;
    private final JournalIndex index;
    private final JournalCodec<E> codec;
    private JournalReader<E> reader;

    MappableJournalSegmentReader(
            FileChannel channel,
            JournalSegment<E> segment,
            JournalCodec<E> codec,
            JournalIndex index,
            int maxEntrySize) {
        this.channel = channel;
        this.segment = segment;
        this.maxEntrySize = maxEntrySize;
        this.index = index;
        this.codec = codec;
        this.reader = new FileChannelJournalSegmentReader<>(channel, segment, maxEntrySize, index, codec);
    }

    @Override
    public long getFirstIndex() {
        return reader.getFirstIndex();
    }

    @Override
    public long getCurrentIndex() {
        return reader.getCurrentIndex();
    }

    @Override
    public Indexed<E> getCurrentEntry() {
        return reader.getCurrentEntry();
    }

    @Override
    public long getNextIndex() {
        return reader.getNextIndex();
    }

    @Override
    public boolean hasNext() {
        return reader.hasNext();
    }

    @Override
    public Indexed<E> next() {
        return reader.next();
    }

    @Override
    public void reset() {
        reader.reset();
    }

    @Override
    public void reset(long index) {
        reader.reset(index);
    }

    @Override
    public void close() {
        reader.close();
        try {
            channel.close();
        } catch (IOException e) {
            throw new StorageException(e);
        } finally {
            segment.closeReader(this);
        }
    }
}
