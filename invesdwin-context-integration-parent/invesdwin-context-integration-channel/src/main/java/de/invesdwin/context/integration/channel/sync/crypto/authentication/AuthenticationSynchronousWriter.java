package de.invesdwin.context.integration.channel.sync.crypto.authentication;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.security.crypto.authentication.IAuthenticationFactory;
import de.invesdwin.context.security.crypto.authentication.mac.IMac;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;

/**
 * Signs each message separately. Stateless regarding the connection.
 */
@NotThreadSafe
public class AuthenticationSynchronousWriter implements ISynchronousWriter<IByteBufferWriter>, IByteBufferWriter {

    private final ISynchronousWriter<IByteBufferWriter> delegate;
    private final IAuthenticationFactory authenticationFactory;
    private IByteBuffer buffer;
    private IByteBuffer unsignedBuffer;
    private IMac mac;

    public AuthenticationSynchronousWriter(final ISynchronousWriter<IByteBufferWriter> delegate,
            final IAuthenticationFactory authenticationFactory) {
        this.delegate = delegate;
        this.authenticationFactory = authenticationFactory;
    }

    public ISynchronousWriter<IByteBufferWriter> getDelegate() {
        return delegate;
    }

    @Override
    public void open() throws IOException {
        delegate.open();
        mac = authenticationFactory.getAlgorithm().newMac();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
        buffer = null;
        if (mac != null) {
            mac.close();
            mac = null;
        }
    }

    @Override
    public void write(final IByteBufferWriter message) throws IOException {
        this.unsignedBuffer = message.asBuffer();
        try {
            delegate.write(this);
        } finally {
            this.unsignedBuffer = null;
        }
    }

    @Override
    public int writeBuffer(final IByteBuffer buffer) {
        //Sadly we need to copy here. E.g. StreamAuthenticatedEncryptionSynchronousWriter spares a copy by doing this together
        return authenticationFactory.copyAndSign(unsignedBuffer, buffer, mac);
    }

    @Override
    public IByteBuffer asBuffer() {
        if (buffer == null) {
            buffer = ByteBuffers.allocateExpandable();
        }
        final int length = writeBuffer(buffer);
        return buffer.slice(0, length);
    }

}
