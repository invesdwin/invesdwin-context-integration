package de.invesdwin.context.integration.channel.stream;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.ThroughputChannelTest;

@NotThreadSafe
public class StreamThroughputChannelTest extends ThroughputChannelTest {

    public StreamThroughputChannelTest(final AChannelTest parent) {
        super(parent);
    }

}
