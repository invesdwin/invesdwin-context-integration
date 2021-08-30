package de.invesdwin.context.integration.channel.jocket.sample;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;

import org.junit.Test;

/**
 * Tests zero-copy features of Jocket.
 * 
 * @author pcdv
 */
public class TestZeroCopyJocket extends AbstractJocketTest {

    /**
     * Write data and read it before new iteration.
     */
    @Test
    public void testWriteAndReadNTimes() throws Exception {
        init(4, 128);

        for (int i = 0; i < 100; i++) {
            final String data = "xxx" + i;
            ByteBuffer b = w.newPacket(16);
            b.put(data.getBytes());
            w.send(b);

            b = r.nextPacket();
            final byte[] arr = new byte[b.remaining()];
            b.get(arr);
            assertEquals(data, new String(arr));
            r.release(b);
        }
    }

    @Test
    public void testWriteBatchAndRead() throws Exception {
        init(128, 1024);

        final int ITER = 2;

        for (int i = 0; i < ITER; i++) {
            final String data = "xxx" + i;
            final ByteBuffer b = w.newPacket(16);
            b.put(data.getBytes());
            w.send(b);
        }

        for (int i = 0; i < ITER; i++) {
            final String data = "xxx" + i;
            final ByteBuffer b = r.nextPacket();
            final byte[] arr = new byte[b.remaining()];
            b.get(arr);
            assertEquals(data, new String(arr));
            r.release(b);
        }
    }

}
