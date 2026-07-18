package de.invesdwin.context.integration.channel.sync.axon;

import javax.annotation.concurrent.NotThreadSafe;

import org.axonframework.test.server.AxonServerContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import de.invesdwin.context.integration.channel.sync.axon.channel.AAxonSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.axon.channel.ServerAxonSynchronousChannel;
import de.invesdwin.util.collections.loadingcache.ALoadingCache;

@Testcontainers
@NotThreadSafe
public class ServerAxonChannelTest extends AAxonChannelTest {
    @Container
    private static final AxonServerContainer AXON_CONTAINER = newAxonServerContainer();

    private final ALoadingCache<Boolean, ServerAxonSynchronousChannel> server_channel = new ALoadingCache<Boolean, ServerAxonSynchronousChannel>() {

        @Override
        protected ServerAxonSynchronousChannel loadValue(final Boolean key) {
            return new ServerAxonSynchronousChannel(newAxonUrl());
        }
    };

    @SuppressWarnings("resource")
    private static AxonServerContainer newAxonServerContainer() {
        return new AxonServerContainer().withAxonServerName("axonserver-test").withAxonServerHostname("localhost");
    }

    private String newAxonUrl() {
        return AXON_CONTAINER.getHost() + ":" + AXON_CONTAINER.getGrpcPort();
    }

    @Override
    protected AAxonSynchronousChannel getAxonSynchronousChannel(final boolean server, final String topic,
            final boolean lowLatency) {
        //        return new ServerAxonSynchronousChannel(newAxonUrl());
        return server_channel.get(server);
    }

}
