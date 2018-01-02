package de.invesdwin.context.integration.jppf.server;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.Test;

import de.invesdwin.context.integration.jppf.client.ConfiguredJPPFClient;
import de.invesdwin.context.integration.jppf.node.ConfiguredJPPFNode;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;

@NotThreadSafe
public class ConfiguredJPPFDriverTest extends ATest {

    @Test
    public void test() {
        Assertions.checkNotNull(ConfiguredJPPFDriver.getInstance());
        Assertions.checkNotNull(ConfiguredJPPFNode.getInstance());
        Assertions.checkNotNull(ConfiguredJPPFClient.getInstance());
    }

}
