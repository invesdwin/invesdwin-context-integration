package de.invesdwin.context.integration.jppf.server;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;

import org.jppf.server.node.local.JPPFLocalNode;
import org.jppf.server.node.remote.JPPFRemoteNode;
import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.jppf.client.ConfiguredJPPFClient;
import de.invesdwin.context.integration.jppf.node.ConfiguredJPPFNode;
import de.invesdwin.context.integration.jppf.node.test.JPPFNodeTest;
import de.invesdwin.context.integration.jppf.server.test.JPPFServerTest;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;

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
        Assertions.checkNotNull(ConfiguredJPPFClient.getInstance());
    }

}
