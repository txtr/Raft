package io.hamster.protocols.raft.storage;


import io.hamster.protocols.raft.storage.log.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

import static org.junit.Assert.*;

/**
 * Raft storage test.
 */
public class RaftStorageTest {

    private static final Path PATH = Paths.get("target/test-logs/");

    @Test
    public void testDefaultConfiguration() {
        RaftStorage storage = RaftStorage.builder().build();
        assertEquals("hamster", storage.prefix());
        assertEquals(new File(System.getProperty("user.dir")), storage.directory());
        assertEquals(1024 * 1024 * 32, storage.maxLogSegmentSize());
        assertTrue(storage.dynamicCompaction());
        assertEquals(.2, storage.freeDiskBuffer(), .01);
        assertTrue(storage.isFlushOnCommit());
        assertFalse(storage.isRetainStaleSnapshots());
        assertTrue(storage.statistics().getFreeMemory() > 0);
    }

    @Test
    public void testCustomConfiguration() {
        RaftStorage storage = RaftStorage.builder()
                .withPrefix("foo")
                .withDirectory(new File(PATH.toFile(), "foo"))
                .withMaxSegmentSize(1024 * 1024)
                .withDynamicCompaction(false)
                .withFreeDiskBuffer(.5)
                .withFlushOnCommit(false)
                .withRetainStaleSnapshots()
                .build();
        assertEquals("foo", storage.prefix());
        assertEquals(new File(PATH.toFile(), "foo"), storage.directory());
        assertEquals(1024 * 1024, storage.maxLogSegmentSize());
        assertFalse(storage.dynamicCompaction());
        assertEquals(.5, storage.freeDiskBuffer(), .01);
        assertFalse(storage.isFlushOnCommit());
        assertTrue(storage.isRetainStaleSnapshots());
    }

    @Test
    public void testCustomConfiguration2() {
        RaftStorage storage = RaftStorage.builder()
                .withDirectory(PATH.toString() + "/baz")
                .withDynamicCompaction()
                .withFlushOnCommit()
                .build();
        assertEquals(new File(PATH.toFile(), "baz"), storage.directory());
        assertTrue(storage.dynamicCompaction());
        assertTrue(storage.isFlushOnCommit());
    }

    @Test
    public void testStorageLock() {
        RaftStorage storage1 = RaftStorage.builder()
                .withDirectory(PATH.toFile())
                .withPrefix("test")
                .build();

        assertTrue(storage1.lock("a"));

        RaftStorage storage2 = RaftStorage.builder()
                .withDirectory(PATH.toFile())
                .withPrefix("test")
                .build();
        assertFalse(storage2.lock("b"));

        RaftStorage storage3 = RaftStorage.builder()
                .withDirectory(PATH.toFile())
                .withPrefix("test")
                .build();

        assertTrue(storage3.lock("a"));
    }

    @Test
    public void testRaftLogReadWrite() {
        RaftStorage storage = RaftStorage.builder()
                .withPrefix("foo")
                .withDirectory(new File(PATH.toFile(), "foo"))
                .withMaxSegmentSize(1024 * 1024)
                .build();

        RaftLog log = storage.openLog();
        RaftLogWriter writer = log.writer();
        RaftLogReader reader = log.openReader(0);

        writer.append(RaftLogEntry.newBuilder()
                .setTerm(1)
                .setTimestamp(2)
                .setInitialize(InitializeEntry.newBuilder().build())
                .build());
        assertEquals(1, reader.next().entry().getTerm());

        writer.append(RaftLogEntry.newBuilder()
                .setTerm(2)
                .setTimestamp(3)
                .setInitialize(InitializeEntry.newBuilder().build())
                .build());
        assertEquals(2, reader.next().entry().getTerm());

        log.close();
    }


    @Before
    @After
    public void cleanupStorage() throws IOException {
        if (Files.exists(PATH)) {
            Files.walkFileTree(PATH, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                }
            });
        }
    }
}
