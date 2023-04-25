package de.invesdwin.context.integration.channel.rpc.session;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.rpc.service.command.IServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.concurrent.lock.ILock;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBufferProvider;

@NotThreadSafe
public class SynchronousEndpointClientSessionResponse implements ICloseableByteBufferProvider {

    private final SynchronousEndpointClientSession session;
    private final SynchronousEndpointClientSessionPool pool;
    private final ISynchronousReader<IServiceSynchronousCommand<IByteBufferProvider>> reader;
    private final ILock lock;
    private IByteBufferProvider message;

    public SynchronousEndpointClientSessionResponse(final SynchronousEndpointClientSessionPool pool,
            final SynchronousEndpointClientSession session, final ILock lock,
            final ISynchronousReader<IServiceSynchronousCommand<IByteBufferProvider>> reader) {
        this.pool = pool;
        this.session = session;
        this.lock = lock;
        this.reader = reader;
    }

    public void setMessage(final IByteBufferProvider message) {
        this.message = message;
    }

    public IByteBufferProvider getMessage() {
        return message;
    }

    @Override
    public void close() {
        if (message == null) {
            return;
        }
        message = null;
        try {
            reader.readFinished();
            pool.returnObject(session);
        } catch (final IOException e) {
            pool.invalidateObject(session);
        }
        lock.unlock();
    }

    @Override
    public int getBuffer(final IByteBuffer dst) throws IOException {
        return message.getBuffer(dst);
    }

    @Override
    public IByteBuffer asBuffer() throws IOException {
        return message.asBuffer();
    }

}
