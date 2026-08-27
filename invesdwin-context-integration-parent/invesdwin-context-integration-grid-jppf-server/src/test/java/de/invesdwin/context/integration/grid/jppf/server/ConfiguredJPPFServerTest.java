package de.invesdwin.context.integration.grid.jppf.server;

import javax.annotation.concurrent.NotThreadSafe;

import org.jppf.server.node.local.JPPFLocalNode;
import org.jppf.server.node.remote.JPPFRemoteNode;
import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.grid.jppf.client.ConfiguredJPPFClient;
import de.invesdwin.context.integration.grid.jppf.node.ConfiguredJPPFNode;
import de.invesdwin.context.integration.grid.jppf.node.test.JPPFNodeTest;
import de.invesdwin.context.integration.grid.jppf.server.test.JPPFServerTest;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.time.date.FTimeUnit;
import jakarta.inject.Inject;

@JPPFServerTest
@JPPFNodeTest
@NotThreadSafe
public class ConfiguredJPPFServerTest extends ATest {

    @Inject
    private ConfiguredJPPFServer server;
    @Inject
    private ConfiguredJPPFNode node;

    @Test
    public void test() {
        Assertions.checkNotNull(server.getDriver());
        Assertions.checkNotNull(node.getNode());
        if (JPPFServerProperties.LOCAL_NODE_ENABLED) {
            Assertions.assertThat(node.getNode()).isInstanceOf(JPPFLocalNode.class);
        } else {
            Assertions.assertThat(node.getNode()).isInstanceOf(JPPFRemoteNode.class);
        }
        FTimeUnit.SECONDS.sleepNoInterrupt(5);
        Assertions.checkNotNull(ConfiguredJPPFClient.getInstance());
    }

}
