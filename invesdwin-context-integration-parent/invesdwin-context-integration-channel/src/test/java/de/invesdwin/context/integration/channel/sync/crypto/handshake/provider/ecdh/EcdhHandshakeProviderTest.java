package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.ecdh;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.HandshakeChannelFactory;

@NotThreadSafe
public class EcdhHandshakeProviderTest extends AChannelTest {

    @Test
    public void testEcdhHandshakePerformance() throws InterruptedException {
        final boolean tmpfs = true;
        final FileChannelType pipes = FileChannelType.MAPPED;
        final File requestFile = newFile("testEcdhHandshakePerformance_request.pipe", tmpfs, pipes);
        final File responseFile = newFile("testEcdhHandshakePerformance_response.pipe", tmpfs, pipes);
        runPerformanceTest(pipes, requestFile, responseFile, null, null,
                new HandshakeChannelFactory(new EcdhHandshakeProvider(MAX_WAIT_DURATION)));
    }

    @Test
    public void testEcdhHandshakePerformanceSigned() throws InterruptedException {
        final boolean tmpfs = true;
        final FileChannelType pipes = FileChannelType.MAPPED;
        final File requestFile = newFile("testEcdhHandshakePerformanceSigned_request.pipe", tmpfs, pipes);
        final File responseFile = newFile("testEcdhHandshakePerformanceSigned_response.pipe", tmpfs, pipes);
        runPerformanceTest(pipes, requestFile, responseFile, null, null,
                new HandshakeChannelFactory(new EcdhHandshakeProvider(MAX_WAIT_DURATION).asSigned()));
    }

    @Override
    protected int getMaxMessageSize() {
        return 230;
    }

}
