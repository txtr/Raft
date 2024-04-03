package io.hamster.protocols.raft.storage.log;

import io.hamster.storage.journal.DelegatingJournalWriter;
import io.hamster.storage.journal.JournalWriter;

/**
 * Raft log writer
 */
public class RaftLogWriter extends DelegatingJournalWriter<RaftLogEntry> {

    public RaftLogWriter(JournalWriter<RaftLogEntry> delegate) {
        super(delegate);
    }
}
