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

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.IgnoreOpenCloseSynchronousReader;
import de.invesdwin.context.integration.channel.sync.IgnoreOpenCloseSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.HandshakeChannel;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.AKeyExchangeHandshakeProvider;
import de.invesdwin.context.security.crypto.CryptoProperties;
import de.invesdwin.context.security.crypto.key.DerivedKeyProvider;
import de.invesdwin.context.security.crypto.random.CryptoRandomGenerators;
import de.invesdwin.util.concurrent.loop.ASpinWait;
import de.invesdwin.util.lang.Closeables;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.duration.Duration;

/**
 * Based on org.bouncycastle.crypto.examples.JPAKEExample
 */
@Immutable
public class JPakeHandshakeProvider extends AKeyExchangeHandshakeProvider {

    private final String participantIdentifier;

    /**
     * SessionIdentifier should be common, ParticipantIdentifier should be different on each side.
     */
    public JPakeHandshakeProvider(final Duration handshakeTimeout, final String sessionIdentifier,
            final String participantIdentifier) {
        super(handshakeTimeout, sessionIdentifier);
        this.participantIdentifier = participantIdentifier;
    }

    @Override
    protected void performHandshake(final HandshakeChannel channel,
            final IgnoreOpenCloseSynchronousWriter<IByteBufferProvider> ignoreOpenCloseWriter,
            final ISynchronousWriter<IByteBufferProvider> handshakeWriter,
            final IgnoreOpenCloseSynchronousReader<IByteBuffer> ignoreOpenCloseReader,
            final ISynchronousReader<IByteBuffer> handshakeReader) throws IOException {
        handshakeWriter.open();
        final IByteBuffer buffer = ByteBuffers.EXPANDABLE_POOL.borrowObject();
        try {
            handshakeReader.open();
            try {
                final JPAKEParticipant ourParticipant = new JPAKEParticipant(getParticipantIdentifier(),
                        getPresharedPassword().toCharArray(), getPrimeOrderGroup(), newDigest(),
                        CryptoRandomGenerators.getThreadLocalCryptoRandom());

                round1(handshakeWriter, handshakeReader, buffer, ourParticipant);
                round2(handshakeWriter, handshakeReader, buffer, ourParticipant);

                final BigInteger keyingMaterial = ourParticipant.calculateKeyingMaterial();
                round3(handshakeWriter, handshakeReader, buffer, ourParticipant, keyingMaterial);

                final byte[] sharedSecret = keyingMaterial.toByteArray();
                final DerivedKeyProvider derivedKeyProvider = new DerivedKeyProvider(sharedSecret,
                        getDerivationFactory());
                finishHandshake(channel, ignoreOpenCloseWriter, ignoreOpenCloseReader, derivedKeyProvider);
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
            final ISynchronousReader<IByteBuffer> handshakeReader, final IByteBuffer buffer,
            final JPAKEParticipant ourParticipant) throws IOException, CryptoException {
        final JPAKERound1Payload ourRound1Payload = ourParticipant.createRound1PayloadToSend();

        final int ourRound1PayloadLength = JPakeRound1PayloadSerde.INSTANCE.toBuffer(buffer, ourRound1Payload);
        handshakeWriter.write(buffer.slice(0, ourRound1PayloadLength));

        waitForMessage(handshakeReader);
        final IByteBuffer otherRound1PayloadMessage = handshakeReader.readMessage();
        final JPAKERound1Payload otherRound1Payload = JPakeRound1PayloadSerde.INSTANCE
                .fromBuffer(otherRound1PayloadMessage, otherRound1PayloadMessage.capacity());
        handshakeReader.readFinished();

        ourParticipant.validateRound1PayloadReceived(otherRound1Payload);
    }

    protected void round2(final ISynchronousWriter<IByteBufferProvider> handshakeWriter,
            final ISynchronousReader<IByteBuffer> handshakeReader, final IByteBuffer buffer,
            final JPAKEParticipant ourParticipant) throws IOException, CryptoException {
        final JPAKERound2Payload ourRound2Payload = ourParticipant.createRound2PayloadToSend();

        final int ourRound2PayloadLength = JPakeRound2PayloadSerde.INSTANCE.toBuffer(buffer, ourRound2Payload);
        handshakeWriter.write(buffer.slice(0, ourRound2PayloadLength));

        waitForMessage(handshakeReader);
        final IByteBuffer otherRound2PayloadMessage = handshakeReader.readMessage();
        final JPAKERound2Payload otherRound2Payload = JPakeRound2PayloadSerde.INSTANCE
                .fromBuffer(otherRound2PayloadMessage, otherRound2PayloadMessage.capacity());
        handshakeReader.readFinished();

        ourParticipant.validateRound2PayloadReceived(otherRound2Payload);
    }

    protected void round3(final ISynchronousWriter<IByteBufferProvider> handshakeWriter,
            final ISynchronousReader<IByteBuffer> handshakeReader, final IByteBuffer buffer,
            final JPAKEParticipant ourParticipant, final BigInteger keyingMaterial)
            throws IOException, CryptoException {
        final JPAKERound3Payload ourRound3Payload = ourParticipant.createRound3PayloadToSend(keyingMaterial);

        final int ourRound3PayloadLength = JPakeRound3PayloadSerde.INSTANCE.toBuffer(buffer, ourRound3Payload);
        handshakeWriter.write(buffer.slice(0, ourRound3PayloadLength));

        waitForMessage(handshakeReader);
        final IByteBuffer otherRound3PayloadMessage = handshakeReader.readMessage();
        final JPAKERound3Payload otherRound2Payload = JPakeRound3PayloadSerde.INSTANCE
                .fromBuffer(otherRound3PayloadMessage, otherRound3PayloadMessage.capacity());
        handshakeReader.readFinished();

        ourParticipant.validateRound3PayloadReceived(otherRound2Payload, keyingMaterial);
    }

    protected void waitForMessage(final ISynchronousReader<IByteBuffer> handshakeReader) {
        final ASpinWait spinWait = newSpinWait(handshakeReader);
        try {
            if (!spinWait.awaitFulfill(System.nanoTime(), getHandshakeTimeout())) {
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
        return JPAKEPrimeOrderGroups.NIST_3072;
    }

    protected String getPresharedPassword() {
        return CryptoProperties.DEFAULT_PEPPER_STR + getSessionIdentifier();
    }

    protected String getParticipantIdentifier() {
        return participantIdentifier;
    }

}
