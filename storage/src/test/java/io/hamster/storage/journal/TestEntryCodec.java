package io.hamster.storage.journal;

import java.nio.ByteBuffer;

public class TestEntryCodec implements JournalCodec<TestEntry> {
    @Override
    public void encode(TestEntry entry, ByteBuffer buffer)  {
        byte[] bytes = entry.bytes();
        buffer.putInt(bytes.length);
        buffer.put(bytes);
    }

    @Override
    public TestEntry decode(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.getInt()];
        buffer.get(bytes);
        TestEntry testEntry = new TestEntry(bytes);
        return testEntry;
    }
}
