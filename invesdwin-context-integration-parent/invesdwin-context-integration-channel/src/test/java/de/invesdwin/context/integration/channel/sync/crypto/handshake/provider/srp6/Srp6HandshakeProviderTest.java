package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.srp6;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.HandshakeChannelFactory;
import de.invesdwin.context.security.crypto.CryptoProperties;

@NotThreadSafe
public class Srp6HandshakeProviderTest extends AChannelTest {

    @Test
    public void testJPakeHandshakePerformance() throws InterruptedException {
        final boolean tmpfs = true;
        final FileChannelType pipes = FileChannelType.MAPPED;
        final String sessionIdentifier = "testJPakeHandshakePerformance";
        final File requestFile = newFile(sessionIdentifier + "_request.pipe", tmpfs, pipes);
        final File responseFile = newFile(sessionIdentifier + "_response.pipe", tmpfs, pipes);
        final HandshakeChannelFactory serverHandshakeChannel = new HandshakeChannelFactory(
                new Srp6ServerHandshakeProvider(MAX_WAIT_DURATION, sessionIdentifier));
        final HandshakeChannelFactory clientHandshakeChannel = new HandshakeChannelFactory(
                new Srp6ClientHandshakeProvider(MAX_WAIT_DURATION, sessionIdentifier, "userId",
                        CryptoProperties.DEFAULT_PEPPER_STR));
        runPerformanceTest(pipes, requestFile, responseFile, null, null, serverHandshakeChannel,
                clientHandshakeChannel);
    }

    @Override
    protected int getMaxMessageSize() {
        return 104;
    }

}
