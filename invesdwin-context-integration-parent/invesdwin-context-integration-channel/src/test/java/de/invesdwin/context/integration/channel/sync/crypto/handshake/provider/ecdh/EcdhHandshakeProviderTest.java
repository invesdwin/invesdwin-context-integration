package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.ecdh;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.HandshakeChannelFactory;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.DerivedSignedKeyAgreementHandshakeProvider;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.SignedKeyAgreementHandshakeProvider;

@NotThreadSafe
public class EcdhHandshakeProviderTest extends AChannelTest {

    @Test
    public void testEcdhHandshakePerformance() throws InterruptedException {
        final boolean tmpfs = true;
        final FileChannelType pipes = FileChannelType.MAPPED;
        final String sessionIdentifier = "testEcdhHandshakePerformance";
        final File requestFile = newFile(sessionIdentifier + "_request.pipe", tmpfs, pipes);
        final File responseFile = newFile(sessionIdentifier + "_response.pipe", tmpfs, pipes);
        new LatencyChannelTest(this).runLatencyTest(pipes, requestFile, responseFile, null, null,
                new HandshakeChannelFactory(new EcdhHandshakeProvider(MAX_WAIT_DURATION, sessionIdentifier)));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testEcdhHandshakePerformanceSigned() throws InterruptedException {
        final boolean tmpfs = true;
        final FileChannelType pipes = FileChannelType.MAPPED;
        final String sessionIdentifier = "testEcdhHandshakePerformanceSigned";
        final File requestFile = newFile(sessionIdentifier + "_request.pipe", tmpfs, pipes);
        final File responseFile = newFile(sessionIdentifier + "_response.pipe", tmpfs, pipes);
        new LatencyChannelTest(this).runLatencyTest(pipes, requestFile, responseFile, null, null,
                new HandshakeChannelFactory(new SignedKeyAgreementHandshakeProvider(
                        new EcdhHandshakeProvider(MAX_WAIT_DURATION, sessionIdentifier))));
    }

    @Test
    public void testEcdhHandshakePerformanceDerivedSigned() throws InterruptedException {
        final boolean tmpfs = true;
        final FileChannelType pipes = FileChannelType.MAPPED;
        final String sessionIdentifier = "testEcdhHandshakePerformanceDerivedSigned";
        final File requestFile = newFile(sessionIdentifier + "_request.pipe", tmpfs, pipes);
        final File responseFile = newFile(sessionIdentifier + "_response.pipe", tmpfs, pipes);
        new LatencyChannelTest(this).runLatencyTest(pipes, requestFile, responseFile, null, null,
                new HandshakeChannelFactory(new DerivedSignedKeyAgreementHandshakeProvider(
                        new EcdhHandshakeProvider(MAX_WAIT_DURATION, sessionIdentifier))));
    }

    @Override
    public int getMaxMessageSize() {
        return 230;
    }

}
