package io.hamster.storage.journal;

public interface Journal<E> extends AutoCloseable {

    /**
     * Returns the journal writer.
     *
     * @return The journal writer.
     */
    JournalWriter<E> writer();
    /**
     * Opens a new journal reader.
     *
     * @param index The index at which to start the reader.
     * @return A new journal reader.
     */
    JournalReader<E> openReader(long index);

    /**
     * Opens a new journal reader.
     *
     * @param index The index at which to start the reader.
     * @param mode  the reader mode
     * @return A new journal reader.
     */
    JournalReader<E> openReader(long index, JournalReader.Mode mode);

    /**
     * Returns a boolean indicating whether the journal is open.
     *
     * @return Indicates whether the journal is open.
     */
    boolean isOpen();

    @Override
    void close();
}
