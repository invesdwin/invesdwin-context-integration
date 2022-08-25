package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.dh;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.HandshakeChannelFactory;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.DerivedSignedKeyAgreementHandshakeProvider;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.SignedKeyAgreementHandshakeProvider;

@NotThreadSafe
public class DhHandshakeProviderTest extends AChannelTest {

    @Test
    public void testDhHandshakePerformance() throws InterruptedException {
        final boolean tmpfs = true;
        final FileChannelType pipes = FileChannelType.MAPPED;
        final String sessionIdentifier = "testDhHandshakePerformance";
        final File requestFile = newFile(sessionIdentifier + "_request.pipe", tmpfs, pipes);
        final File responseFile = newFile(sessionIdentifier + "_response.pipe", tmpfs, pipes);
        runPerformanceTest(pipes, requestFile, responseFile, null, null,
                new HandshakeChannelFactory(new DhHandshakeProvider(MAX_WAIT_DURATION, sessionIdentifier)));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testDhHandshakePerformanceSigned() throws InterruptedException {
        final boolean tmpfs = true;
        final FileChannelType pipes = FileChannelType.MAPPED;
        final String sessionIdentifier = "testDhHandshakePerformanceSigned";
        final File requestFile = newFile(sessionIdentifier + "_request.pipe", tmpfs, pipes);
        final File responseFile = newFile(sessionIdentifier + "_response.pipe", tmpfs, pipes);
        runPerformanceTest(pipes, requestFile, responseFile, null, null,
                new HandshakeChannelFactory(new SignedKeyAgreementHandshakeProvider(
                        new DhHandshakeProvider(MAX_WAIT_DURATION, sessionIdentifier))));
    }

    @Test
    public void testDhHandshakePerformanceDerivedSigned() throws InterruptedException {
        final boolean tmpfs = true;
        final FileChannelType pipes = FileChannelType.MAPPED;
        final String sessionIdentifier = "testDhHandshakePerformanceDerivedSigned";
        final File requestFile = newFile(sessionIdentifier + "_request.pipe", tmpfs, pipes);
        final File responseFile = newFile(sessionIdentifier + "_response.pipe", tmpfs, pipes);
        runPerformanceTest(pipes, requestFile, responseFile, null, null,
                new HandshakeChannelFactory(new DerivedSignedKeyAgreementHandshakeProvider(
                        new DhHandshakeProvider(MAX_WAIT_DURATION, sessionIdentifier))));
    }

    @Override
    protected int getMaxMessageSize() {
        return 1141;
    }

}
