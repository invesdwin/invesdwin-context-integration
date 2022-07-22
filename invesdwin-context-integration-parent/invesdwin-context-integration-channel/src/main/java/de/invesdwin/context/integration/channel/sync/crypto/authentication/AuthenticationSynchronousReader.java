package de.invesdwin.context.integration.channel.sync.crypto.authentication;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.security.crypto.authentication.IAuthenticationFactory;
import de.invesdwin.context.security.crypto.authentication.mac.pool.IMac;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;

/**
 * Verifies each message separately. Stateless regarding the connection.
 */
@NotThreadSafe
public class AuthenticationSynchronousReader implements ISynchronousReader<IByteBuffer> {

    private final ISynchronousReader<IByteBuffer> delegate;
    private final IAuthenticationFactory authenticationFactory;
    private IMac mac;

    public AuthenticationSynchronousReader(final ISynchronousReader<IByteBuffer> delegate,
            final IAuthenticationFactory authenticationFactory) {
        this.delegate = delegate;
        this.authenticationFactory = authenticationFactory;
    }

    @Override
    public void open() throws IOException {
        delegate.open();
        this.mac = authenticationFactory.getAlgorithm().newMac();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
        mac = null;
    }

    @Override
    public boolean hasNext() throws IOException {
        return delegate.hasNext();
    }

    @Override
    public IByteBuffer readMessage() throws IOException {
        final IByteBuffer signedBuffer = delegate.readMessage();
        //no copy needed here
        return authenticationFactory.verifyAndSlice(signedBuffer, mac);
    }

    @Override
    public void readFinished() {
        delegate.readFinished();
    }
}
