package io.hamster.storage.journal;

import io.hamster.storage.StorageLevel;

import java.io.IOException;

/**
 * Disk journal test.
 */
public class DiskJournalTest extends AbstractJournalTest {

    public DiskJournalTest(int maxSegmentSize) throws IOException {
        super(maxSegmentSize);
    }

    @Override
    protected StorageLevel storageLevel() {
        return StorageLevel.DISK;
    }

}
