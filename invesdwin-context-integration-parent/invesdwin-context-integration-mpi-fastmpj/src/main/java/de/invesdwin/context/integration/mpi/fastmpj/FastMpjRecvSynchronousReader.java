package de.invesdwin.context.integration.mpi.fastmpj;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.concurrent.reference.integer.IIntReference;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import mpi.MPI;
import mpi.Request;
import mpi.Status;

@NotThreadSafe
public class FastMpjRecvSynchronousReader implements ISynchronousReader<IByteBufferProvider> {

    private final IIntReference source;
    private final IIntReference tag;
    private final int maxMessageSize;
    private IByteBuffer buffer;
    private Request request;
    private Status status;

    public FastMpjRecvSynchronousReader(final IIntReference source, final IIntReference tag, final int maxMessageSize) {
        this.source = source;
        this.tag = tag;
        this.maxMessageSize = maxMessageSize;
    }

    @Override
    public void open() throws IOException {
        buffer = ByteBuffers.allocateDirect(maxMessageSize);
    }

    @Override
    public void close() throws IOException {
        buffer = null;
    }

    @Override
    public boolean hasNext() throws IOException {
        if (request != null) {
            return hasMessage();
        }
        request = MPI.COMM_WORLD.Irecv(buffer.asNioByteBuffer(), 0, buffer.capacity(), MPI.BYTE, source.get(),
                tag.get());
        return hasMessage();
    }

    private boolean hasMessage() throws IOException {
        if (status != null) {
            return true;
        }
        status = request.Test();
        return status != null;
    }

    @Override
    public IByteBufferProvider readMessage() throws IOException {
        final int length = status.Get_count(MPI.BYTE);
        return buffer.sliceTo(length);
    }

    @Override
    public void readFinished() throws IOException {
        request.Free();
        request = null;
        status = null;
    }

}
