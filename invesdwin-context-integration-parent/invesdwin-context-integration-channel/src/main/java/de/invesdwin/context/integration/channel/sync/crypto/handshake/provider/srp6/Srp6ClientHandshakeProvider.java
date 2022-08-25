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
import de.invesdwin.context.security.crypto.key.DerivedKeyProvider;
import de.invesdwin.util.concurrent.loop.ASpinWait;
import de.invesdwin.util.lang.Closeables;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.duration.Duration;

@Immutable
public class Srp6ClientHandshakeProvider extends ASrp6HandshakeProvider {

    private final String userIdHash;
    private final String passwordHash;

    public Srp6ClientHandshakeProvider(final Duration handshakeTimeout, final String userId, final String password,
            final String sessionIdentifier) {
        super(handshakeTimeout, sessionIdentifier);
        this.userIdHash = hashSecret(sessionIdentifier);
        this.passwordHash = hashSecret(password);
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
                //Create new client SRP-6a auth session.
                final SRP6ClientSession client = new SRP6ClientSession();

                clientStep1(handshakeWriter, buffer, client);
                clientStep2(handshakeWriter, handshakeReader, handshakeReaderSpinWait, buffer, client);
                final BigInteger sessionKey = clientStep3(handshakeReader, handshakeReaderSpinWait, buffer, client);

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
            ByteBuffers.EXPANDABLE_POOL.returnObject(buffer);
        }
    }

    private BigInteger clientStep3(final ISynchronousReader<IByteBuffer> handshakeReader,
            final ASpinWait handshakeReaderSpinWait, final IByteBuffer buffer, final SRP6ClientSession client)
            throws IOException, SRP6Exception {
        //On receiving the server evidence message 'M2':
        //Validate the server evidence message 'M2' after which user and server are mutually authenticated.
        waitForMessage(handshakeReaderSpinWait);
        final IByteBuffer serverStep2ResultMessage = handshakeReader.readMessage();
        final Srp6ServerStep2Result serverStep2Result = Srp6ServerStep2ResultSerde.INSTANCE
                .fromBuffer(serverStep2ResultMessage, buffer.capacity());
        handshakeReader.readFinished();
        client.step3(serverStep2Result.getServerEvidenceMessageM2());

        //On completing mutual authentication:
        //The established session key 'S' can be used to encrypt further communication between client and server.
        final BigInteger sessionKey = client.getSessionKey();
        return sessionKey;
    }

    private void clientStep2(final ISynchronousWriter<IByteBufferProvider> handshakeWriter,
            final ISynchronousReader<IByteBuffer> handshakeReader, final ASpinWait handshakeReaderSpinWait,
            final IByteBuffer buffer, final SRP6ClientSession client) throws IOException, SRP6Exception {
        //On receiving the server public value 'B' (and crypto parameters):
        waitForMessage(handshakeReaderSpinWait);
        final IByteBuffer serverStep1ResultMessage = handshakeReader.readMessage();
        final Srp6ServerStep1Result serverStep1Result = Srp6ServerStep1ResultSerde.INSTANCE
                .fromBuffer(serverStep1ResultMessage, serverStep1ResultMessage.capacity());
        handshakeReader.readFinished();

        //Set the SRP-6a crypto parameters.
        //Compute the client public value 'A' and evidence message 'M1'.
        //Send the client public value 'A' and evidence message 'M1' to the server.
        final SRP6ClientCredentials srp6ClientCredentials = client.step2(getSrp6CryptoParams(),
                serverStep1Result.getPasswordSaltS(), serverStep1Result.getServerPublicValueB());
        final Srp6ClientStep2Result clientStep2Result = new Srp6ClientStep2Result(srp6ClientCredentials.A,
                srp6ClientCredentials.M1);
        final int clientStep2ResultLength = Srp6ClientStep2ResultSerde.INSTANCE.toBuffer(buffer, clientStep2Result);
        handshakeWriter.write(buffer.sliceTo(clientStep2ResultLength));
    }

    private void clientStep1(final ISynchronousWriter<IByteBufferProvider> handshakeWriter, final IByteBuffer buffer,
            final SRP6ClientSession client) throws IOException {
        //Store input user identity 'I' and password 'P'.
        client.step1(userIdHash, passwordHash);

        //Send user identity 'I' to server.
        final Srp6ServerStep1LookupInput serverStep1LookupInput = new Srp6ServerStep1LookupInput(userIdHash);
        final int serverStep1LookupInputLength = Srp6ServerStep1LookupInputSerde.INSTANCE.toBuffer(buffer,
                serverStep1LookupInput);
        handshakeWriter.write(buffer.sliceTo(serverStep1LookupInputLength));
    }

    protected DerivedKeyProvider newDerivedKeyProvider(final byte[] sharedSecret) {
        return DerivedKeyProvider.fromRandom(getSessionIdentifierWithUserId(userIdHash).getBytes(), sharedSecret,
                getDerivationFactory());
    }

}
