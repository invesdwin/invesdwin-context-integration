package de.invesdwin.context.integration.channel.sync.axon;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.axon.channel.AAxonSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.axon.channel.InMemoryAxonSynchronousChannel;
import de.invesdwin.util.collections.loadingcache.ALoadingCache;

@NotThreadSafe
public class InMemoryAxonChannelTest extends AAxonChannelTest {
    private final ALoadingCache<String, InMemoryAxonSynchronousChannel> topic_channel = new ALoadingCache<String, InMemoryAxonSynchronousChannel>() {
        @Override
        protected InMemoryAxonSynchronousChannel loadValue(final String key) {
            return new InMemoryAxonSynchronousChannel(false);
        }
    };
    private final ALoadingCache<String, InMemoryAxonSynchronousChannel> topic_channelLowLatency = new ALoadingCache<String, InMemoryAxonSynchronousChannel>() {
        @Override
        protected InMemoryAxonSynchronousChannel loadValue(final String key) {
            return new InMemoryAxonSynchronousChannel(true);
        }
    };

    @Override
    protected AAxonSynchronousChannel getAxonSynchronousChannel(final boolean server, final String topic,
            final boolean lowLatency) {
        if (lowLatency) {
            return topic_channelLowLatency.get(topic);
        } else {
            return topic_channel.get(topic);
        }
    }
}
