package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.srp6;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.ALatencyChannelTest;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.HandshakeChannelFactory;
import de.invesdwin.context.security.crypto.CryptoProperties;
import de.invesdwin.util.lang.UUIDs;

@NotThreadSafe
public class Srp6HandshakeProviderTest extends ALatencyChannelTest {

    @Test
    public void testSrp6HandshakePerformance() throws InterruptedException {
        final boolean tmpfs = true;
        final FileChannelType pipes = FileChannelType.MAPPED;
        final String sessionIdentifier = "testSrp6HandshakePerformance";
        final File requestFile = newFile(sessionIdentifier + "_request.pipe", tmpfs, pipes);
        final File responseFile = newFile(sessionIdentifier + "_response.pipe", tmpfs, pipes);
        final String userId = UUIDs.newPseudoRandomUUID();
        final String password = CryptoProperties.DEFAULT_PEPPER_STR;
        final HandshakeChannelFactory serverHandshakeChannel = new HandshakeChannelFactory(
                new PreSharedSrp6ServerHandshakeProvider(MAX_WAIT_DURATION, sessionIdentifier, userId, password));
        final HandshakeChannelFactory clientHandshakeChannel = new HandshakeChannelFactory(
                new Srp6ClientHandshakeProvider(MAX_WAIT_DURATION, sessionIdentifier, userId, password));
        runLatencyTest(pipes, requestFile, responseFile, null, null, serverHandshakeChannel,
                clientHandshakeChannel);
    }

    @Override
    protected int getMaxMessageSize() {
        return 594;
    }

}
