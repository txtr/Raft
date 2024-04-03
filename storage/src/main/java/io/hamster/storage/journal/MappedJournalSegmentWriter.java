package io.hamster.storage.journal;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

public class MappedJournalSegmentWriter<E> implements JournalWriter<E> {

    private final MappedByteBuffer mappedBuffer;
    private final ByteBuffer buffer;

    MappedJournalSegmentWriter(MappedByteBuffer buffer) {
        this.mappedBuffer = buffer;
        this.buffer = buffer.slice();
    }

    /**
     * Returns the mapped buffer underlying the segment writer.
     *
     * @return the mapped buffer underlying the segment writer
     */
    MappedByteBuffer buffer() {
        return mappedBuffer;
    }

    @Override
    public long getLastIndex() {
        return 0;
    }

    @Override
    public Indexed<E> getLastEntry() {
        return null;
    }

    @Override
    public long getNextIndex() {
        return 0;
    }

    @Override
    public <T extends E> Indexed<T> append(T entry) {

        return null;
    }

    @Override
    public void append(Indexed<E> entry) {
        final long nextIndex = getNextIndex();



    }

    @Override
    public void reset(long index) {

    }

    @Override
    public void truncate(long index) {

    }

    @Override
    public void flush() {

    }

    @Override
    public void close() {

    }
}
