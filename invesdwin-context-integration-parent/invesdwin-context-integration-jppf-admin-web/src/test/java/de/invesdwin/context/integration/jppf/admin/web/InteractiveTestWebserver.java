package de.invesdwin.context.integration.jppf.admin.web;

import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.Test;

import de.invesdwin.context.test.ATest;
import de.invesdwin.context.webserver.test.WebserverTest;

@WebserverTest
@NotThreadSafe
public class InteractiveTestWebserver extends ATest {

    @Test
    public void test() throws Exception {
        TimeUnit.DAYS.sleep(Long.MAX_VALUE);
    }

}
