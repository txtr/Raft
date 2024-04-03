package io.hamster.protocols.raft.storage.log;

import com.google.protobuf.CodedOutputStream;
import io.hamster.storage.journal.JournalCodec;

import java.io.IOException;
import java.nio.ByteBuffer;

public class RaftLogCodec implements JournalCodec<RaftLogEntry> {
    @Override
    public void encode(RaftLogEntry entry, ByteBuffer buffer) throws IOException {
        CodedOutputStream stream = CodedOutputStream.newInstance(buffer);
        entry.writeTo(stream);
        stream.flush();
    }

    @Override
    public RaftLogEntry decode(ByteBuffer buffer) throws IOException {
        RaftLogEntry entry = RaftLogEntry.parseFrom(buffer);
        buffer.position(buffer.limit());
        return entry;
    }
}
