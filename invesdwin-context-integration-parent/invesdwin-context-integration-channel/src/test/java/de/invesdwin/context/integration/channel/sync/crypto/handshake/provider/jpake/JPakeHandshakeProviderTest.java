package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.jpake;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.HandshakeChannelFactory;

@NotThreadSafe
public class JPakeHandshakeProviderTest extends AChannelTest {

    @Test
    public void testJPakeHandshakePerformance() throws InterruptedException {
        final boolean tmpfs = true;
        final FileChannelType pipes = FileChannelType.MAPPED;
        final String sessionIdentifier = "testJPakeHandshakePerformance";
        final File requestFile = newFile(sessionIdentifier + "_request.pipe", tmpfs, pipes);
        final File responseFile = newFile(sessionIdentifier + "_response.pipe", tmpfs, pipes);
        runPerformanceTest(pipes, requestFile, responseFile, null, null,
                new HandshakeChannelFactory(new JPakeHandshakeProvider(MAX_WAIT_DURATION, sessionIdentifier)));
    }

    @Override
    protected int getMaxMessageSize() {
        return 1714;
    }

}
