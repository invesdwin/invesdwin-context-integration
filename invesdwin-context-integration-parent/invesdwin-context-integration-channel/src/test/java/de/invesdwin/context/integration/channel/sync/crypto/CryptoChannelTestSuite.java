package de.invesdwin.context.integration.channel.sync.crypto;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

import de.invesdwin.context.integration.channel.sync.crypto.encryption.EncryptionChannelTest;
import de.invesdwin.context.integration.channel.sync.crypto.encryption.stream.StreamEncryptionChannelTest;
import de.invesdwin.context.integration.channel.sync.crypto.encryption.verification.VerifiedEncryptionChannelTest;
import de.invesdwin.context.integration.channel.sync.crypto.encryption.verification.stream.StreamVerifiedEncryptionChannelTest;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.dh.DhHandshakeProviderTest;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.ecdh.EcdhHandshakeProviderTest;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.jpake.JPakeHandshakeProviderTest;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.DtlsHandshakeProviderTest;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.TlsHandshakeProviderTest;
import de.invesdwin.context.integration.channel.sync.crypto.verification.VerificationChannelTest;

// CHECKSTYLE:OFF
@Suite
@SelectClasses({ EncryptionChannelTest.class, StreamEncryptionChannelTest.class, VerificationChannelTest.class,
        VerifiedEncryptionChannelTest.class, StreamVerifiedEncryptionChannelTest.class, DhHandshakeProviderTest.class,
        EcdhHandshakeProviderTest.class, TlsHandshakeProviderTest.class, DtlsHandshakeProviderTest.class,
        JPakeHandshakeProviderTest.class })
@Immutable
public class CryptoChannelTestSuite {
    //CHECKSTYLE:ON

}
