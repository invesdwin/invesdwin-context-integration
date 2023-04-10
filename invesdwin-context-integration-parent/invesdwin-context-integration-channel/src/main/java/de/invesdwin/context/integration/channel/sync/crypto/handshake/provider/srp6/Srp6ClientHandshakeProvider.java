package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.srp6;

import java.io.IOException;
import java.math.BigInteger;

import javax.annotation.concurrent.Immutable;

import com.nimbusds.srp6.SRP6ClientCredentials;
import com.nimbusds.srp6.SRP6ClientSession;
import com.nimbusds.srp6.SRP6Exception;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.IgnoreOpenCloseSynchronousReader;
import de.invesdwin.context.integration.channel.sync.IgnoreOpenCloseSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.HandshakeChannel;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.srp6.payload.Srp6ClientStep2Result;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.srp6.payload.Srp6ClientStep2ResultSerde;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.srp6.payload.Srp6ServerStep1LookupInput;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.srp6.payload.Srp6ServerStep1LookupInputSerde;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.srp6.payload.Srp6ServerStep1Result;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.srp6.payload.Srp6ServerStep1ResultSerde;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.srp6.payload.Srp6ServerStep2Result;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.srp6.payload.Srp6ServerStep2ResultSerde;
import de.invesdwin.context.integration.channel.sync.spinwait.SynchronousReaderSpinWait;
import de.invesdwin.context.integration.channel.sync.spinwait.SynchronousReaderSpinWaitPool;
import de.invesdwin.context.integration.channel.sync.spinwait.SynchronousWriterSpinWait;
import de.invesdwin.context.integration.channel.sync.spinwait.SynchronousWriterSpinWaitPool;
import de.invesdwin.context.security.crypto.key.DerivedKeyProvider;
import de.invesdwin.util.lang.Closeables;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBuffer;
import de.invesdwin.util.time.duration.Duration;

@Immutable
public class Srp6ClientHandshakeProvider extends ASrp6HandshakeProvider {

    private final String userIdHash;
    private final String passwordHash;

    public Srp6ClientHandshakeProvider(final Duration handshakeTimeout, final String sessionIdentifier,
            final String userId, final String password) {
        super(handshakeTimeout, sessionIdentifier);
        this.userIdHash = hashSecret(userId);
        this.passwordHash = hashSecret(password);
    }

    @Override
    protected void performHandshake(final HandshakeChannel channel,
            final IgnoreOpenCloseSynchronousWriter<IByteBufferProvider> underlyingWriter,
            final ISynchronousWriter<IByteBufferProvider> handshakeWriter,
            final IgnoreOpenCloseSynchronousReader<IByteBufferProvider> underlyingReader,
            final ISynchronousReader<IByteBufferProvider> handshakeReader) throws IOException {
        handshakeWriter.open();
        final SynchronousReaderSpinWait<IByteBufferProvider> handshakeReaderSpinWait = SynchronousReaderSpinWaitPool
                .borrowObject(handshakeReader);
        final SynchronousWriterSpinWait<IByteBufferProvider> handshakeWriterSpinWait = SynchronousWriterSpinWaitPool
                .borrowObject(handshakeWriter);
        try (ICloseableByteBuffer buffer = ByteBuffers.EXPANDABLE_POOL.borrowObject()) {
            handshakeReader.open();
            try {
                //Create new client SRP-6a auth session.
                final SRP6ClientSession client = new SRP6ClientSession();
                client.setXRoutine(getXRoutine());

                clientStep1(handshakeWriterSpinWait, buffer, client);
                clientStep2(handshakeWriterSpinWait, handshakeReaderSpinWait, buffer, client);
                final BigInteger sessionKey = clientStep3(handshakeReaderSpinWait, buffer, client);

                final byte[] sharedSecret = sessionKey.toByteArray();
                final DerivedKeyProvider derivedKeyProvider = newDerivedKeyProvider(sharedSecret);
                finishHandshake(channel, underlyingWriter, underlyingReader, derivedKeyProvider);
            } catch (final SRP6Exception e) {
                throw new IOException(e);
            } finally {
                Closeables.closeQuietly(handshakeReader);
            }
        } finally {
            Closeables.closeQuietly(handshakeWriter);
            SynchronousWriterSpinWaitPool.returnObject(handshakeWriterSpinWait);
            SynchronousReaderSpinWaitPool.returnObject(handshakeReaderSpinWait);
        }
    }

    private void clientStep1(final SynchronousWriterSpinWait<IByteBufferProvider> handshakeWriterSpinWait,
            final IByteBuffer buffer, final SRP6ClientSession client) throws IOException {
        //Store input user identity 'I' and password 'P'.
        client.step1(userIdHash, passwordHash);

        //Send user identity 'I' to server.
        final Srp6ServerStep1LookupInput serverStep1LookupInput = new Srp6ServerStep1LookupInput(userIdHash);
        final int serverStep1LookupInputLength = Srp6ServerStep1LookupInputSerde.INSTANCE.toBuffer(buffer,
                serverStep1LookupInput);
        handshakeWriterSpinWait.waitForWrite(buffer.sliceTo(serverStep1LookupInputLength), getHandshakeTimeout());
    }

    private void clientStep2(final SynchronousWriterSpinWait<IByteBufferProvider> handshakeWriterSpinWait,
            final SynchronousReaderSpinWait<IByteBufferProvider> handshakeReaderSpinWait, final IByteBuffer buffer,
            final SRP6ClientSession client) throws IOException, SRP6Exception {
        //On receiving the server public value 'B' (and crypto parameters):
        final IByteBuffer serverStep1ResultMessage = handshakeReaderSpinWait.waitForRead(getHandshakeTimeout())
                .asBuffer();
        final Srp6ServerStep1Result serverStep1Result = Srp6ServerStep1ResultSerde.INSTANCE
                .fromBuffer(serverStep1ResultMessage);
        handshakeReaderSpinWait.getReader().readFinished();

        //Set the SRP-6a crypto parameters.
        //Compute the client public value 'A' and evidence message 'M1'.
        //Send the client public value 'A' and evidence message 'M1' to the server.
        final SRP6ClientCredentials srp6ClientCredentials = client.step2(getSrp6CryptoParams(),
                serverStep1Result.getPasswordSaltS(), serverStep1Result.getServerPublicValueB());
        final Srp6ClientStep2Result clientStep2Result = new Srp6ClientStep2Result(srp6ClientCredentials.A,
                srp6ClientCredentials.M1);
        final int clientStep2ResultLength = Srp6ClientStep2ResultSerde.INSTANCE.toBuffer(buffer, clientStep2Result);
        handshakeWriterSpinWait.waitForWrite(buffer.sliceTo(clientStep2ResultLength), getHandshakeTimeout());
    }

    private BigInteger clientStep3(final SynchronousReaderSpinWait<IByteBufferProvider> handshakeReaderSpinWait,
            final IByteBuffer buffer, final SRP6ClientSession client) throws IOException, SRP6Exception {
        //On receiving the server evidence message 'M2':
        //Validate the server evidence message 'M2' after which user and server are mutually authenticated.
        final IByteBuffer serverStep2ResultMessage = handshakeReaderSpinWait.waitForRead(getHandshakeTimeout())
                .asBuffer();
        final Srp6ServerStep2Result serverStep2Result = Srp6ServerStep2ResultSerde.INSTANCE
                .fromBuffer(serverStep2ResultMessage);
        handshakeReaderSpinWait.getReader().readFinished();
        client.step3(serverStep2Result.getServerEvidenceMessageM2());

        //On completing mutual authentication:
        //The established session key 'S' can be used to encrypt further communication between client and server.
        final BigInteger sessionKey = client.getSessionKey();
        return sessionKey;
    }

    protected DerivedKeyProvider newDerivedKeyProvider(final byte[] sharedSecret) {
        return DerivedKeyProvider.fromRandom(getSessionIdentifierWithUserId(userIdHash).getBytes(), sharedSecret,
                getDerivationFactory());
    }

}
