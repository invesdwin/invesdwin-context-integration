package de.invesdwin.context.integration.channel.jocket.sample;

import static org.junit.Assert.assertEquals;

import java.io.EOFException;
import java.nio.ByteBuffer;

import org.junit.Ignore;

import jocket.impl.Const;
import jocket.impl.JocketReader;
import jocket.impl.JocketWriter;

@Ignore
public abstract class AbstractJocketTest {

    private final int readBufSize = 8192;

    protected JocketWriter w;
    protected JocketReader r;

    protected void init(final int npackets, final int capacity) {
        // allows to run tests several times without resetting the socket
        if (w == null) {
            final ByteBuffer buf = ByteBuffer.allocate(Const.PACKET_INFO + npackets * Const.LEN_PACKET_INFO + capacity);
            w = new JocketWriter(buf, npackets);
            r = new JocketReader(buf, npackets);
            w.setAlign(0);
        }
    }

    /**
     * Writes strings, flushing after each of them.
     *
     * @return number of bytes successfully written
     */
    protected int write(final String... strs) {
        return write(true, strs);
    }

    /**
     * Writes strings without flushing.
     *
     * @return number of bytes successfully written
     */
    protected int write0(final String... strs) {
        return write(false, strs);
    }

    protected int write(final boolean flush, final String... strs) {
        int total = 0;
        for (final String s : strs) {
            total += w.write(s.getBytes(), 0, s.length());
            if (flush) {
                w.flush();
            }
        }
        return total;
    }

    protected void flush() {
        w.flush();
    }

    protected void write(final int... bytes) {
        for (final int len : bytes) {
            w.write(new byte[len], 0, len);
        }
    }

    protected void read(final int... bytes) {
        for (final int len : bytes) {
            assertEquals(len, r.read(new byte[len], 0, len));
        }
    }

    public void read(final String... str) throws EOFException {
        for (final String s : str) {
            assertEquals(s, read());
        }
    }

    protected String read() throws EOFException {
        final byte[] buf = new byte[readBufSize];
        return read(buf, 0, buf.length);
    }

    protected void read(final int bytes, final String expect) throws EOFException {
        final byte[] buf = new byte[bytes];
        assertEquals(expect, read(buf, 0, bytes));
    }

    protected String read(final byte[] buf, final int off, final int len) throws EOFException {
        return new String(buf, off, r.read(buf, off, len));
    }
}
