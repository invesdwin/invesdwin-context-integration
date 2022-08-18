package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.dh;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.HandshakeChannelFactory;

@NotThreadSafe
public class DhHandshakeProviderTest extends AChannelTest {

    @Test
    public void testDhHandshakePerformance() throws InterruptedException {
        final boolean tmpfs = true;
        final FileChannelType pipes = FileChannelType.MAPPED;
        final File requestFile = newFile("testDhHandshakePerformance_request.pipe", tmpfs, pipes);
        final File responseFile = newFile("testDhHandshakePerformance_response.pipe", tmpfs, pipes);
        runPerformanceTest(pipes, requestFile, responseFile, null, null,
                new HandshakeChannelFactory(new DhHandshakeProvider(MAX_WAIT_DURATION)));
    }

    @Test
    public void testDhHandshakePerformanceSigned() throws InterruptedException {
        final boolean tmpfs = true;
        final FileChannelType pipes = FileChannelType.MAPPED;
        final File requestFile = newFile("testDhHandshakePerformanceSigned_request.pipe", tmpfs, pipes);
        final File responseFile = newFile("testDhHandshakePerformanceSigned_response.pipe", tmpfs, pipes);
        runPerformanceTest(pipes, requestFile, responseFile, null, null,
                new HandshakeChannelFactory(new DhHandshakeProvider(MAX_WAIT_DURATION).asSigned()));
    }

    @Override
    protected int getMaxMessageSize() {
        return 1141;
    }

}
