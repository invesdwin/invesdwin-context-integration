package de.invesdwin.context.integration.channel.rpc.base.client.session.single;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.rpc.base.client.session.ISynchronousEndpointClientSession;
import de.invesdwin.context.integration.channel.rpc.base.server.service.command.IServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.concurrent.lock.ILock;
import de.invesdwin.util.concurrent.pool.IObjectPool;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBufferProvider;

@NotThreadSafe
public class SingleplexingSynchronousEndpointClientSessionResponse implements ICloseableByteBufferProvider {

    private final ISynchronousEndpointClientSession session;
    private final IObjectPool<ISynchronousEndpointClientSession> pool;
    private final ISynchronousReader<IServiceSynchronousCommand<IByteBufferProvider>> reader;
    private final ILock lock;
    private IByteBufferProvider message;

    public SingleplexingSynchronousEndpointClientSessionResponse(
            final IObjectPool<ISynchronousEndpointClientSession> pool, final ISynchronousEndpointClientSession session,
            final ILock lock, final ISynchronousReader<IServiceSynchronousCommand<IByteBufferProvider>> reader) {
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
