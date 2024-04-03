package io.hamster.storage.journal;

import java.io.Serializable;
import java.util.Arrays;

import static com.google.common.base.MoreObjects.toStringHelper;

public class TestEntry implements Serializable {

    private final byte[] bytes;

    public TestEntry(int size) {
        this(new byte[size]);
    }

    public TestEntry(byte[] bytes) {
        this.bytes = bytes;
    }

    public byte[] bytes() {
        return bytes;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("length", bytes.length)
                .add("hash", Arrays.hashCode(bytes))
                .toString();
    }
}
