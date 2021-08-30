package de.invesdwin.context.integration.channel.jocket.sample;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.DataInputStream;
import java.io.OutputStream;

import org.junit.Test;

import jocket.impl.ClosedException;

public class TestJocketSocket extends AbstractJocketSocketTest {

    @Test
    public void testWriteRead() throws Exception {
        final OutputStream out = c.getOutputStream();
        out.write("hello".getBytes());
        out.flush();
        final byte[] buf = new byte[100];
        final int len = s.getInputStream().read(buf);
        assertEquals(5, len);
        assertEquals("hello", new String(buf, 0, 5));
    }

    @Test
    public void testCloseOutput() throws Exception {
        c.getOutputStream().close();
        assertEquals(-1, s.getInputStream().read());
    }

    @Test
    public void testCloseInput() throws Exception {
        c.getInputStream().close();
        try {
            s.getOutputStream().write(22);
            fail("");
        } catch (final ClosedException e) {
        }
    }

    /**
     * Used to reproduce a bug. Warning: this test works only if the jocket buffer capacity is big enough.
     */
    @Test
    public void testUnderflow() throws Exception {
        final byte[] buf = new byte[300000];
        for (int i = 0; i < 100; i++) {
            c.getOutputStream().write(buf);
            c.getWriter().flush();
            new DataInputStream(s.getInputStream()).readFully(buf);
        }
    }
}
