package io.hamster.protocols.raft.storage.log;

import io.hamster.storage.journal.DelegatingJournalReader;
import io.hamster.storage.journal.JournalReader;

/**
 * Raft log reader
 */
public class RaftLogReader extends DelegatingJournalReader<RaftLogEntry> {

    public RaftLogReader(JournalReader<RaftLogEntry> delegate) {
        super(delegate);
    }
}
