package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.jpake;

import java.io.IOException;
import java.math.BigInteger;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.Immutable;

import org.bouncycastle.crypto.CryptoException;
import org.bouncycastle.crypto.Digest;
import org.bouncycastle.crypto.agreement.jpake.JPAKEParticipant;
import org.bouncycastle.crypto.agreement.jpake.JPAKEPrimeOrderGroup;
import org.bouncycastle.crypto.agreement.jpake.JPAKEPrimeOrderGroups;
import org.bouncycastle.crypto.agreement.jpake.JPAKERound1Payload;
import org.bouncycastle.crypto.agreement.jpake.JPAKERound2Payload;
import org.bouncycastle.crypto.agreement.jpake.JPAKERound3Payload;
import org.bouncycastle.crypto.digests.SHA256Digest;

import de.invesdwin.context.integration.channel.sync.ISynchronousChannelFactory;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.IgnoreOpenCloseSynchronousReader;
import de.invesdwin.context.integration.channel.sync.IgnoreOpenCloseSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.HandshakeChannel;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.AKeyExchangeHandshakeProvider;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.jpake.payload.JPakeRound1PayloadSerde;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.jpake.payload.JPakeRound2PayloadSerde;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.jpake.payload.JPakeRound3PayloadSerde;
import de.invesdwin.context.security.crypto.CryptoProperties;
import de.invesdwin.context.security.crypto.key.DerivedKeyProvider;
import de.invesdwin.context.security.crypto.random.CryptoRandomGenerators;
import de.invesdwin.util.concurrent.loop.ASpinWait;
import de.invesdwin.util.lang.Closeables;
import de.invesdwin.util.lang.UUIDs;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.duration.Duration;

/**
 * Based on org.bouncycastle.crypto.examples.JPAKEExample
 */
@Immutable
public class JPakeHandshakeProvider extends AKeyExchangeHandshakeProvider {

    private static final JPAKEPrimeOrderGroup DEFAULT_PRIME_ORDER_GROUP = JPAKEPrimeOrderGroups.NIST_3072;

    /**
     * SessionIdentifier should be common on both sides.
     */
    public JPakeHandshakeProvider(final Duration handshakeTimeout, final String sessionIdentifier) {
        super(handshakeTimeout, sessionIdentifier);
    }

    @Override
    protected void performHandshake(final HandshakeChannel channel,
            final IgnoreOpenCloseSynchronousWriter<IByteBufferProvider> underlyingWriter,
            final ISynchronousWriter<IByteBufferProvider> handshakeWriter,
            final IgnoreOpenCloseSynchronousReader<IByteBuffer> underlyingReader,
            final ISynchronousReader<IByteBuffer> handshakeReader) throws IOException {
        final ASpinWait handshakeReaderSpinWait = newSpinWait(handshakeReader);

        handshakeWriter.open();
        final IByteBuffer buffer = ByteBuffers.EXPANDABLE_POOL.borrowObject();
        try {
            handshakeReader.open();
            try {
                final JPAKEParticipant ourParticipant = new JPAKEParticipant(newParticipantIdentifier(),
                        getPresharedPassword().toCharArray(), getPrimeOrderGroup(), newDigest(),
                        CryptoRandomGenerators.getThreadLocalCryptoRandom());

                round1(handshakeWriter, handshakeReaderSpinWait, handshakeReader, buffer, ourParticipant);
                round2(handshakeWriter, handshakeReaderSpinWait, handshakeReader, buffer, ourParticipant);

                final BigInteger keyingMaterial = ourParticipant.calculateKeyingMaterial();
                round3(handshakeWriter, handshakeReaderSpinWait, handshakeReader, buffer, ourParticipant,
                        keyingMaterial);

                final byte[] sharedSecret = keyingMaterial.toByteArray();
                final DerivedKeyProvider derivedKeyProvider = DerivedKeyProvider
                        .fromRandom(getSessionIdentifier().getBytes(), sharedSecret, getDerivationFactory());
                finishHandshake(channel, underlyingWriter, underlyingReader, derivedKeyProvider);
            } catch (final CryptoException e) {
                throw new IOException(e);
            } finally {
                Closeables.closeQuietly(handshakeReader);
            }
        } finally {
            Closeables.closeQuietly(handshakeWriter);
            ByteBuffers.EXPANDABLE_POOL.returnObject(buffer);
        }
    }

    protected void round1(final ISynchronousWriter<IByteBufferProvider> handshakeWriter,
            final ASpinWait handshakeReaderSpinWait, final ISynchronousReader<IByteBuffer> handshakeReader,
            final IByteBuffer buffer, final JPAKEParticipant ourParticipant) throws IOException, CryptoException {
        final JPAKERound1Payload ourRound1Payload = ourParticipant.createRound1PayloadToSend();

        final int ourRound1PayloadLength = JPakeRound1PayloadSerde.INSTANCE.toBuffer(buffer, ourRound1Payload);
        handshakeWriter.write(buffer.slice(0, ourRound1PayloadLength));

        waitForMessage(handshakeReaderSpinWait);
        final IByteBuffer otherRound1PayloadMessage = handshakeReader.readMessage();
        final JPAKERound1Payload otherRound1Payload = JPakeRound1PayloadSerde.INSTANCE
                .fromBuffer(otherRound1PayloadMessage, otherRound1PayloadMessage.capacity());
        handshakeReader.readFinished();

        ourParticipant.validateRound1PayloadReceived(otherRound1Payload);
    }

    protected void round2(final ISynchronousWriter<IByteBufferProvider> handshakeWriter,
            final ASpinWait handshakeReaderSpinWait, final ISynchronousReader<IByteBuffer> handshakeReader,
            final IByteBuffer buffer, final JPAKEParticipant ourParticipant) throws IOException, CryptoException {
        final JPAKERound2Payload ourRound2Payload = ourParticipant.createRound2PayloadToSend();

        final int ourRound2PayloadLength = JPakeRound2PayloadSerde.INSTANCE.toBuffer(buffer, ourRound2Payload);
        handshakeWriter.write(buffer.slice(0, ourRound2PayloadLength));

        waitForMessage(handshakeReaderSpinWait);
        final IByteBuffer otherRound2PayloadMessage = handshakeReader.readMessage();
        final JPAKERound2Payload otherRound2Payload = JPakeRound2PayloadSerde.INSTANCE
                .fromBuffer(otherRound2PayloadMessage, otherRound2PayloadMessage.capacity());
        handshakeReader.readFinished();

        ourParticipant.validateRound2PayloadReceived(otherRound2Payload);
    }

    protected void round3(final ISynchronousWriter<IByteBufferProvider> handshakeWriter,
            final ASpinWait handshakeReaderSpinWait, final ISynchronousReader<IByteBuffer> handshakeReader,
            final IByteBuffer buffer, final JPAKEParticipant ourParticipant, final BigInteger keyingMaterial)
            throws IOException, CryptoException {
        final JPAKERound3Payload ourRound3Payload = ourParticipant.createRound3PayloadToSend(keyingMaterial);

        final int ourRound3PayloadLength = JPakeRound3PayloadSerde.INSTANCE.toBuffer(buffer, ourRound3Payload);
        handshakeWriter.write(buffer.slice(0, ourRound3PayloadLength));

        waitForMessage(handshakeReaderSpinWait);
        final IByteBuffer otherRound3PayloadMessage = handshakeReader.readMessage();
        final JPAKERound3Payload otherRound2Payload = JPakeRound3PayloadSerde.INSTANCE
                .fromBuffer(otherRound3PayloadMessage, otherRound3PayloadMessage.capacity());
        handshakeReader.readFinished();

        ourParticipant.validateRound3PayloadReceived(otherRound2Payload, keyingMaterial);
    }

    protected void waitForMessage(final ASpinWait handshakeReaderSpinWait) {
        try {
            if (!handshakeReaderSpinWait.awaitFulfill(System.nanoTime(), getHandshakeTimeout())) {
                throw new TimeoutException("Read handshake message timeout exceeded: " + getHandshakeTimeout());
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * We use SHA-256 per default. Though we need a BouncyCastle instance.
     */
    protected Digest newDigest() {
        return new SHA256Digest();
    }

    protected JPAKEPrimeOrderGroup getPrimeOrderGroup() {
        return DEFAULT_PRIME_ORDER_GROUP;
    }

    protected String getPresharedPassword() {
        return CryptoProperties.DEFAULT_PEPPER_STR + getSessionIdentifier();
    }

    /**
     * ParticipantIdentifier should be different on each side. We just use a random value thus.
     */
    protected String newParticipantIdentifier() {
        return UUIDs.newPseudoRandomUUID();
    }

    /**
     * Actually there is no need to encrypt during the handshake since no secrets are sent over the wire and the client
     * gets authenticated via the pre shared pepper. Thus one could replace this with DisabledChannelFactory to send the
     * handshake steps in plaintext.
     */
    @Override
    public ISynchronousChannelFactory<IByteBuffer, IByteBufferProvider> newAuthenticatedHandshakeChannelFactory() {
        return super.newAuthenticatedHandshakeChannelFactory();
    }

}
