package io.hamster.storage.journal;

import org.junit.Ignore;

import java.nio.ByteBuffer;

public class Test {


    @org.junit.Test
    @Ignore
    public void t() throws Exception {

        //size = ((1024-64)/6) - 8 - 4;
        int size = 148;

        TestEntry testEntry = new TestEntry(148);
        TestEntryCodec codec = new TestEntryCodec();
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        codec.encode(testEntry, buffer);

        int len = buffer.position() + 8;

        Journal<TestEntry> journal = SegmentedJournal.<TestEntry>builder()
                .withIndexDensity(0.2)
                .withMaxEntrySize(1024)
                .withMaxSegmentSize(1024)
                .withCodec(new TestEntryCodec())
                .build();


        /*JournalWriter<TestEntry> writer1 = journal.writer();

        for(int i = 0; i < 18;i++) {
            Indexed<TestEntry> indexed = writer1.append(testEntry);
            System.out.println(indexed);
        }*/

        JournalReader<TestEntry> reader = journal.openReader(1);

        while (reader.hasNext()) {
            Indexed<TestEntry> next = reader.next();
            System.out.println(next);
        }

        reader.reset(7);
        Indexed<TestEntry> next = reader.next();
        /*System.out.println(writer1.getNextIndex());*/


    }

}
