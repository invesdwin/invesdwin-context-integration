package de.invesdwin.context.integration.channel.sync.jucx;

import java.util.zip.CRC32;
import java.util.zip.Checksum;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.Immutable;

@Immutable
public final class JucxTags {

    public static final long TAG_MASK_ALL = 0xffffffffffffffffL;
    private static final int MIN_TAG = 1;
    private static final long MAX_TAG = 0x00ffffffffffffffL;

    @GuardedBy("this.class")
    private static long nextTag = MIN_TAG;

    private JucxTags() {}

    public static synchronized long nextTag() {
        final long tag = nextTag++;
        if (tag > MAX_TAG) {
            nextTag = MIN_TAG;
            return MIN_TAG;
        }
        return tag;
    }

    public static long checksum(final long tag) {
        final Checksum checksum = new CRC32();
        for (int i = 0; i < Long.BYTES; i++) {
            final byte currentByte = (byte) (tag >> (i * 8));
            checksum.update(currentByte);
        }
        return checksum.getValue();
    }

}
