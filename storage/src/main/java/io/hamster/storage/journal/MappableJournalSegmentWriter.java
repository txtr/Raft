package io.hamster.storage.journal;

import io.hamster.storage.StorageException;
import io.hamster.storage.journal.index.JournalIndex;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class MappableJournalSegmentWriter<E> implements JournalWriter<E> {

    private final FileChannel channel;
    private final JournalSegment<E> journalSegment;
    private final JournalCodec<E> codec;

    private JournalWriter<E> writer;
    private int maxEntrySize;


    public MappableJournalSegmentWriter(FileChannel channel,
                                        JournalSegment<E> journalSegment,
                                        JournalCodec<E> codec,
                                        JournalIndex index,
                                        int maxEntrySize
    ) {
        this.channel = channel;
        this.journalSegment = journalSegment;
        this.codec = codec;
        this.writer = new FileChannelJournalSegmentWriter<>(channel, journalSegment, codec, maxEntrySize, index);
    }

    MappedByteBuffer buffer() {
        JournalWriter<E> writer = this.writer;
        if (writer instanceof MappedJournalSegmentWriter) {
            return ((MappedJournalSegmentWriter<E>) writer).buffer();
        }
        return null;
    }

    @Override
    public long getLastIndex() {
        return writer.getLastIndex();
    }

    @Override
    public Indexed<E> getLastEntry() {
        return writer.getLastEntry();
    }

    @Override
    public long getNextIndex() {
        return writer.getNextIndex();
    }

    @Override
    public <T extends E> Indexed<T> append(T entry) {
        return writer.append(entry);
    }

    @Override
    public void reset(long index) {
        writer.reset(index);
    }

    @Override
    public void truncate(long index) {
        writer.truncate(index);
    }

    @Override
    public void flush() {
        writer.flush();
    }

    @Override
    public void append(Indexed<E> entry) {
        writer.append(entry);
    }

    @Override
    public void close() {
        writer.close();
        try {
            channel.close();
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }
}
