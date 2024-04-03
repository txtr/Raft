package io.hamster.storage.journal;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Journal codec.
 */
public interface JournalCodec<E> {

    /**
     * Encodes the given entry to the given buffer.
     *
     * @param entry  the entry to encode
     * @param buffer the buffer to which to encode the entry
     */
    void encode(E entry, ByteBuffer buffer) throws IOException;

    /**
     * Decodes an entry from the given buffer.
     *
     * @param buffer the buffer from which to decode the entry
     * @return the decoded entry
     */
    E decode(ByteBuffer buffer) throws IOException;

}
