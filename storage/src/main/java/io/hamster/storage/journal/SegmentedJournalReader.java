package io.hamster.storage.journal;

import java.util.NoSuchElementException;

public class SegmentedJournalReader<E> implements JournalReader<E> {

    private final SegmentedJournal<E> journal;
    private JournalSegment<E> currentSegment;
    private MappableJournalSegmentReader<E> currentReader;
    private Indexed<E> previousEntry;
    private final Mode mode;

    public SegmentedJournalReader(SegmentedJournal<E> journal, long index, Mode mode) {
        this.journal = journal;
        this.mode = mode;
        initialize(index);
    }

    /**
     * Initializes the reader to the given index.
     */
    private void initialize(long index) {
        currentSegment = journal.getSegment(index);
        currentSegment.acquire();
        currentReader = currentSegment.createReader();
        long nextIndex = getNextIndex();
        while (index > nextIndex && hasNext()) {
            next();
            nextIndex = getNextIndex();
        }
    }


    @Override
    public long getFirstIndex() {
        return currentReader.getFirstIndex();
    }

    @Override
    public long getCurrentIndex() {
        long currentIndex = currentReader.getCurrentIndex();
        if (currentIndex != 0) {
            return currentIndex;
        }
        if (previousEntry != null) {
            return previousEntry.index();
        }
        return 0;
    }

    @Override
    public Indexed<E> getCurrentEntry() {
        Indexed<E> currentEntry = currentReader.getCurrentEntry();
        if (currentEntry != null) {
            return currentEntry;
        }
        return previousEntry;
    }

    @Override
    public long getNextIndex() {
        return currentReader.getNextIndex();
    }

    @Override
    public boolean hasNext() {
        if (Mode.ALL == mode) {
            return hasNextEntry();
        }
        long nextIndex = getNextIndex();
        long commitIndex = journal.getCommitIndex();
        return (nextIndex <= commitIndex) && hasNextEntry();
    }

    private boolean hasNextEntry() {
        if (currentReader.hasNext()) {
            return true;
        }

        JournalSegment<E> nextSegment = journal.getNextSegment(currentSegment.index());
        if (nextSegment != null && nextSegment.index() == getNextIndex()) {
            previousEntry = currentReader.getCurrentEntry();
            currentSegment.release();
            currentSegment = nextSegment;
            currentSegment.acquire();
            currentReader = currentSegment.createReader();

            return currentReader.hasNext();
        }
        return false;
    }


    @Override
    public Indexed<E> next() {
        if (!currentReader.hasNext()) {
            JournalSegment<E> nextSegment = journal.getNextSegment(currentSegment.index());
            if (nextSegment != null && nextSegment.index() == getNextIndex()) {
                previousEntry = currentReader.getCurrentEntry();
                currentSegment.release();
                currentSegment = nextSegment;
                currentSegment.acquire();
                currentReader = currentSegment.createReader();
                return currentReader.next();
            } else {
                throw new NoSuchElementException();
            }
        } else {
            previousEntry = currentReader.getCurrentEntry();
            return currentReader.next();
        }

    }

    @Override
    public void reset() {
        currentReader.close();
        currentSegment.release();
        currentSegment = journal.getFirstSegment();
        currentSegment.acquire();
        currentReader = currentSegment.createReader();
        previousEntry = null;
    }

    @Override
    public void reset(long index) {

        // If the current segment is not open, it has been replaced. Reset the segments.
        if (!currentSegment.isOpen()) {
            reset();
        }

        if (index < currentReader.getNextIndex()) {
            rewind(index);
        } else if (index > currentReader.getNextIndex()) {
            forward(index);
        } else {
            currentReader.reset(index);
        }
    }

    /**
     * Fast forwards the journal to the given index.
     */
    private void forward(long index) {
        while (index > currentReader.getNextIndex() && hasNext()) {
            next();
        }
    }

    /**
     * Rewinds the journal to the given index.
     */
    private void rewind(long index) {

        if (currentSegment.index() >= index) {
            //Why not use index , but index-1  , because get the previous entry
            JournalSegment<E> segment = journal.getSegment(index - 1);
            if (segment != null) {
                currentReader.close();
                currentSegment.release();
                currentSegment = segment;
                currentSegment.acquire();
                currentReader = currentSegment.createReader();
            }
        }
        currentReader.reset(index);
        previousEntry = currentReader.getCurrentEntry();
    }

    @Override
    public void close() {
        currentReader.close();
        journal.closeReader(this);
    }
}
