package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.srp6;

import java.io.IOException;
import java.math.BigInteger;

import javax.annotation.concurrent.Immutable;

import com.nimbusds.srp6.SRP6Exception;
import com.nimbusds.srp6.SRP6ServerSession;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.IgnoreOpenCloseSynchronousReader;
import de.invesdwin.context.integration.channel.sync.IgnoreOpenCloseSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.HandshakeChannel;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.srp6.payload.Srp6ClientStep2Result;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.srp6.payload.Srp6ClientStep2ResultSerde;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.srp6.payload.Srp6ServerStep1LookupInput;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.srp6.payload.Srp6ServerStep1LookupInputSerde;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.srp6.payload.Srp6ServerStep1LookupOutput;
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
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;

@Immutable
public abstract class ASrp6ServerHandshakeProvider extends ASrp6HandshakeProvider {

    /**
     * SessionIdentifier can be null, in that case only the userId is used as the salt for the DerivedKeyProvider.
     */
    public ASrp6ServerHandshakeProvider(final Duration handshakeTimeout, final String sessionIdentifier) {
        super(handshakeTimeout, sessionIdentifier);
    }

    @Override
    protected void performHandshake(final HandshakeChannel channel,
            final IgnoreOpenCloseSynchronousWriter<IByteBufferProvider> underlyingWriter,
            final ISynchronousWriter<IByteBufferProvider> handshakeWriter,
            final IgnoreOpenCloseSynchronousReader<IByteBufferProvider> underlyingReader,
            final ISynchronousReader<IByteBufferProvider> handshakeReader) throws IOException {
        final ASpinWait handshakeReaderSpinWait = newSpinWait(handshakeReader);

        handshakeWriter.open();
        final IByteBuffer buffer = ByteBuffers.EXPANDABLE_POOL.borrowObject();
        try {
            handshakeReader.open();
            try {
                //Create new server SRP-6a auth session.
                final SRP6ServerSession server = new SRP6ServerSession(getSrp6CryptoParams(),
                        getHandshakeTimeout().intValue(FTimeUnit.SECONDS));

                final String userId = serverStep1(handshakeWriter, handshakeReader, handshakeReaderSpinWait, buffer,
                        server);
                serverStep2(handshakeWriter, handshakeReader, handshakeReaderSpinWait, buffer, server);
                final BigInteger sessionKey = serverStep3(server);

                final byte[] sharedSecret = sessionKey.toByteArray();
                final DerivedKeyProvider derivedKeyProvider = newDerivedKeyProvider(userId, sharedSecret);
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

    private String serverStep1(final ISynchronousWriter<IByteBufferProvider> handshakeWriter,
            final ISynchronousReader<IByteBufferProvider> handshakeReader, final ASpinWait handshakeReaderSpinWait,
            final IByteBuffer buffer, final SRP6ServerSession server) throws IOException {
        waitForMessage(handshakeReaderSpinWait);
        final IByteBuffer serverStep1LookupInputMessage = handshakeReader.readMessage().asBuffer();
        final Srp6ServerStep1LookupInput serverStep1LookupInput = Srp6ServerStep1LookupInputSerde.INSTANCE
                .fromBuffer(serverStep1LookupInputMessage);
        handshakeReader.readFinished();

        //Look up stored salt 's' and verifier 'v' for the authenticating user identity 'I'.
        final Srp6ServerStep1LookupOutput serverStep1LookupOutput = getServerStep1LookupResult(serverStep1LookupInput);
        //Compute the server public value 'B'.
        final String userIdHash = serverStep1LookupInput.getUserIdHash();
        final BigInteger serverPublicValueB = server.step1(userIdHash, serverStep1LookupOutput.getPasswordSaltS(),
                serverStep1LookupOutput.getPasswordVerifierV());
        //Respond with the server public value 'B' and password salt 's'.
        //If the SRP-6a crypto parameters 'N', 'g' and 'H' were not agreed in advance between server and client append them to the response.
        final Srp6ServerStep1Result serverStep1Result = new Srp6ServerStep1Result(
                serverStep1LookupOutput.getPasswordSaltS(), serverPublicValueB);
        final int step1ResultLength = Srp6ServerStep1ResultSerde.INSTANCE.toBuffer(buffer, serverStep1Result);
        handshakeWriter.write(buffer.sliceTo(step1ResultLength));
        //CHECKSTYLE:OFF
        while (!handshakeWriter.writeFinished()) {
            //CHECKSTYLE:ON
            //repeat
        }

        return userIdHash;
    }

    private void serverStep2(final ISynchronousWriter<IByteBufferProvider> handshakeWriter,
            final ISynchronousReader<IByteBufferProvider> handshakeReader, final ASpinWait handshakeReaderSpinWait,
            final IByteBuffer buffer, final SRP6ServerSession server) throws IOException, SRP6Exception {
        //On receiving the client public value 'A' and evidence message 'M1':
        waitForMessage(handshakeReaderSpinWait);
        final IByteBuffer clientStep2ResultMessage = handshakeReader.readMessage().asBuffer();
        final Srp6ClientStep2Result clientStep2Result = Srp6ClientStep2ResultSerde.INSTANCE
                .fromBuffer(clientStep2ResultMessage);
        handshakeReader.readFinished();
        //Complete user authentication.
        //Compute server evidence message 'M2'.
        final BigInteger serverEvidenceMessageM2 = server.step2(clientStep2Result.getClientPublicValueA(),
                clientStep2Result.getClientEvidenceMessageM1());
        //Respond with the server evidence message 'M2'.
        final Srp6ServerStep2Result serverStep2Result = new Srp6ServerStep2Result(serverEvidenceMessageM2);
        final int serverStep2ResultLength = Srp6ServerStep2ResultSerde.INSTANCE.toBuffer(buffer, serverStep2Result);
        handshakeWriter.write(buffer.sliceTo(serverStep2ResultLength));
        //CHECKSTYLE:OFF
        while (!handshakeWriter.writeFinished()) {
            //CHECKSTYLE:ON
            //repeat
        }
    }

    private BigInteger serverStep3(final SRP6ServerSession server) {
        //On completing mutual authentication:
        //The established session key 'S' can be used to encrypt further communication between client and server.
        final BigInteger sessionKey = server.getSessionKey();
        return sessionKey;
    }

    /**
     * Override this method with something that actually looks up user credentials that are stored from a previous
     * registration process.
     * 
     * Use symmetric AES encryption with a key only visible at the webserver to encrypt the verifier v value within the
     * database. This protects against off-site database backups being used in an offline dictionary attack against v.
     * (https://github.com/simbo1905/thinbus-srp-npm#recommendations)
     */
    protected abstract Srp6ServerStep1LookupOutput getServerStep1LookupResult(Srp6ServerStep1LookupInput input)
            throws IOException;

    protected DerivedKeyProvider newDerivedKeyProvider(final String userId, final byte[] sharedSecret) {
        return DerivedKeyProvider.fromRandom(getSessionIdentifierWithUserId(userId).getBytes(), sharedSecret,
                getDerivationFactory());
    }

}
