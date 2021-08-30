package de.invesdwin.context.integration.channel.jocket.sample;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;

import jocket.net.JocketSocket;
import jocket.net.ServerJocket;

@Ignore
public abstract class AbstractJocketSocketTest {
    protected JocketSocket c;
    protected volatile JocketSocket s;
    protected ServerJocket srv;

    @Before
    public void setUp() throws Exception {
        srv = new ServerJocket(0);
        new Thread() {
            @Override
            public void run() {
                try {
                    s = srv.accept();
                } catch (final IOException e) {
                    e.printStackTrace();
                }
            }
        }.start();
        c = new JocketSocket(srv.getLocalPort());
        while (s == null) {
            Thread.sleep(1);
        }
    }

    @After
    public void tearDown() throws Exception {
        srv.close();
    }
}
