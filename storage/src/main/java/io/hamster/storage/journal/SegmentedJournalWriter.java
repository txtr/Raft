package io.hamster.storage.journal;

import java.nio.BufferOverflowException;

public class SegmentedJournalWriter<E> implements JournalWriter<E> {

    private final SegmentedJournal<E> journal;
    private JournalSegment<E> currentSegment;
    private MappableJournalSegmentWriter<E> currentWriter;

    public SegmentedJournalWriter(SegmentedJournal<E> journal) {
        this.journal = journal;
        this.currentSegment = journal.getLastSegment();
        currentSegment.acquire();
        this.currentWriter = currentSegment.writer();
    }

    @Override
    public long getLastIndex() {
        return currentWriter.getLastIndex();
    }

    @Override
    public Indexed<E> getLastEntry() {
        return currentWriter.getLastEntry();
    }

    @Override
    public long getNextIndex() {
        return currentWriter.getNextIndex();
    }

    @Override
    public <T extends E> Indexed<T> append(T entry) {
        try {
            return currentWriter.append(entry);
        } catch (BufferOverflowException e) {
            //First entry can not write , the entry size is too large
            if (currentSegment.index() == currentWriter.getNextIndex()) {
                throw e;
            }
            currentWriter.flush();
            currentSegment.release();
            currentSegment = journal.getNextSegment();
            currentSegment.acquire();
            currentWriter = currentSegment.writer();
            return currentWriter.append(entry);
        }
    }

    @Override
    public void append(Indexed<E> entry) {
        try {
            currentWriter.append(entry);
        } catch (BufferOverflowException e) {
            //First entry can not write , the entry size is too large
            if (currentSegment.index() == currentWriter.getNextIndex()) {
                throw e;
            }
            currentWriter.flush();
            currentSegment.release();
            currentSegment = journal.getNextSegment();
            currentSegment.acquire();
            currentWriter = currentSegment.writer();
            currentWriter.append(entry);
        }
    }

    @Override
    public void commit(long index) {
        if (index > journal.getCommitIndex()) {
            journal.setCommitIndex(index);
            if (journal.isFlushOnCommit()) {
                flush();
            }
        }
    }

    @Override
    public void reset(long index) {
        if (index > currentSegment.index()) {
            currentSegment.release();
            currentSegment = journal.resetSegments(index);
            currentSegment.acquire();
            currentWriter = currentSegment.writer();
        } else {
            truncate(index - 1);
        }
        journal.resetHead(index);
    }

    @Override
    public void truncate(long index) {
        if (index < journal.getCommitIndex()) {
            throw new IndexOutOfBoundsException("Cannot truncate committed index: " + index);
        }

        while (index < currentSegment.index() && currentSegment != journal.getFirstSegment()) {
            currentSegment.release();
            journal.removeSegment(currentSegment);
            currentSegment = journal.getLastSegment();
            currentSegment.acquire();
            currentWriter = currentSegment.writer();
        }

        // Truncate the current index.
        currentWriter.truncate(index);

        // Reset segment readers.
        journal.resetTail(index + 1);
    }

    @Override
    public void flush() {

    }

    @Override
    public void close() {
        currentWriter.close();
    }
}
